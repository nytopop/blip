// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Strategies for handling network partitions.
use super::{
    cut::{self, MultiNodeCut, Subscription},
    proto::{
        membership_client::MembershipClient, Endpoint, EndpointError, Join, JoinReq, JoinResp,
        NodeId, NodeMetadata, PreJoinReq, PreJoinResp,
    },
    Cluster, State,
};
use futures::{
    future::TryFutureExt,
    stream::{FuturesUnordered, StreamExt},
};
use log::{info, warn};
use std::{borrow::Cow, cmp, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::time::{delay_for, timeout, Elapsed};
use tonic::transport;

mod private {
    pub trait Sealed {}
}

/// A strategy to handle network partitions where the local member is ejected from an
/// active cluster.
///
/// This trait is sealed against outside implementations.
#[crate::async_trait]
pub trait Strategy: private::Sealed + Default + Send + Sync + 'static {
    // If this method exits, the mesh goes down with it (!).
    #[doc(hidden)]
    async fn handle_parts(node: Arc<Cluster<Self>>, mut cuts: Subscription) -> cut::Result;
}

/// Rejoin the existing cluster through random healthy members.
#[derive(Default)]
pub struct Rejoin {}

impl private::Sealed for Rejoin {}

#[crate::async_trait]
impl Strategy for Rejoin {
    async fn handle_parts(node: Arc<Cluster<Self>>, mut cuts: Subscription) -> cut::Result {
        node.initialize().await;

        loop {
            let cut = cuts.recv().await?;

            if !cut.is_degraded() {
                continue;
            }

            if cut.members().is_empty() {
                node.initialize().await;
                continue;
            }

            node.join_via_backoff(|| Cow::Owned(cut.random_member().into()))
                .await;
        }
    }
}

#[derive(Debug, Error)]
enum JoinError {
    #[error("timed out: {}", .0)]
    TimedOut(#[from] Elapsed),

    #[error("endpoint resolution failed: {}", .0)]
    Resolution(#[from] EndpointError),

    #[error("phase 1 failed: {}", .0)]
    Phase1(GrpcError),

    #[error("phase 2 failed: {}", .0)]
    Phase2(GrpcError),

    #[error("phase 2 failed: no observers")]
    NoObservers,
}

#[derive(Debug, Error)]
enum GrpcError {
    #[error("connection failed: {}", .0)]
    Connect(#[from] transport::Error),

    #[error("call failed: {}", .0)]
    Call(#[from] tonic::Status),
}

impl<St: Strategy> Cluster<St> {
    /// Initialize as if we just started up by attempting to join a seed node, or becoming
    /// a single node bootstrap cluster.
    async fn initialize(&self) {
        if let Some(seed) = self.cfg.seed.as_ref() {
            self.join_via_backoff(|| Cow::Borrowed(seed)).await;
        } else {
            let mut state = self.state.write().await;

            state.clear_consensus();
            state.clear_membership();

            self.bootstrap(&mut state);
        }
    }

    /// Boostrap a new cluster. This will not reset any membership state, and should only
    /// be called with a blank [State].
    fn bootstrap(&self, state: &mut State) {
        // NOTE(invariant): must only be called with completely cleared state
        assert!(state.nodes.is_empty());
        assert!(state.uuids.is_empty());
        assert!(state.metadata.is_empty());
        assert!(state.last_cut.is_none());

        let node = Endpoint::from(self.addr).tls(self.cfg.server_tls);
        let uuid = state.uuid.clone();
        let meta = self.cfg.meta.clone();

        let members: Arc<[_]> = vec![self
            .resolve_member_meta(self.cfg.meta.clone(), &node)
            .unwrap()]
        .into();

        state.join_node(node, Join { uuid, meta });

        let cut = MultiNodeCut {
            skipped: 0,
            local_addr: self.addr,
            conf_id: state.rehash_config(),
            degraded: false,
            members: members.clone(),
            joined: members,
            kicked: vec![].into(),
        };

        state.last_cut = Some(cut.clone());
        self.propagate_cut(cut);

        info!("bootstrapped: conf_id={}", state.conf_id);
    }

    /// Join a cluster using the provided function to generate a seed on each attempt.
    ///
    /// Uses exponential backoff if join failures are encountered.
    async fn join_via_backoff<'a, F: FnMut() -> Cow<'a, Endpoint>>(&self, mut seed: F) {
        const RETRY_MAX: Duration = Duration::from_secs(4);
        const JOIN_MAX: Duration = Duration::from_secs(15);

        let mut retry_backoff = Duration::from_millis(200);
        let mut join_backoff = Duration::from_secs(5);

        while let Err(e) = self.join_via(&seed(), join_backoff).await {
            warn!("join failed: {}", e);

            delay_for(retry_backoff).await;
            retry_backoff = cmp::min(retry_backoff * 2, RETRY_MAX);
            join_backoff = cmp::min(join_backoff + (join_backoff / 2), JOIN_MAX);
        }
    }

    /// Attempt to join a cluster via the provided seed node.
    async fn join_via(&self, seed: &Endpoint, max_wait: Duration) -> Result<(), JoinError> {
        let mut state = self.state.write().await;

        state.uuid = NodeId::generate();

        info!("requesting join: timeout={:?}", max_wait);
        let JoinResp { nodes, uuids, .. } =
            timeout(max_wait, self.request_join(&state, seed)).await??;

        state.clear_consensus();
        state.clear_membership();

        let mut joined = Vec::with_capacity(nodes.len());
        for NodeMetadata { node, meta } in nodes {
            joined.push(self.resolve_member_meta(meta.clone(), &node).unwrap());
            assert!(state.nodes.insert(node.clone()));
            assert!(state.metadata.insert(node, meta).is_none());
        }
        for uuid in uuids {
            assert!(state.uuids.insert(uuid));
        }

        joined.sort_by_key(|m| m.addr());

        let mut members: Vec<_> = (state.nodes.iter())
            .map(|node| self.resolve_member(&state, node).unwrap())
            .collect();

        members.sort_by_key(|m| m.addr());

        let cut = MultiNodeCut {
            skipped: 0,
            local_addr: self.addr,
            degraded: !state.nodes.contains(&self.local_node()),
            conf_id: state.rehash_config(),
            members: members.into(),
            joined: joined.into(),
            kicked: vec![].into(),
        };

        state.last_cut = Some(cut.clone());
        self.propagate_cut(cut);

        info!("joined: conf_id={}", state.conf_id);
        Ok(())
    }

    /// Request to join the provided seed node. Returns `Ok(_)` if both phases of the join
    /// protocol completed successfully.
    async fn request_join(&self, state: &State, seed: &Endpoint) -> Result<JoinResp, JoinError> {
        let p1j_req = PreJoinReq {
            sender: self.local_node(),
            uuid: state.uuid.clone(),
        };

        let r1 = self.join_phase1(p1j_req, seed).await?;

        let conf_id = r1.conf_id;

        let p2j_req = |ring| JoinReq {
            sender: self.local_node(),
            ring: ring as u64,
            uuid: state.uuid.clone(),
            conf_id,
            meta: self.cfg.meta.clone(),
        };

        let mut joins = (r1.contact.into_iter())
            .enumerate()
            .map(move |(ring, observer)| (p2j_req(ring), observer))
            .map(|(req, observer)| self.join_phase2(req, observer))
            .collect::<FuturesUnordered<_>>();

        let mut e = None;
        while let Some(resp) = joins.next().await {
            match resp {
                Ok(resp) => return Ok(resp),
                Err(err) => e = Some(err),
            }
        }

        Err(e.unwrap_or(JoinError::NoObservers))
    }

    /// Initiate phase 1 of the join protocol via the provided endpoint.
    async fn join_phase1(&self, req: PreJoinReq, via: &Endpoint) -> Result<PreJoinResp, JoinError> {
        let seed = self.resolve_endpoint(via)?;

        let mut c = MembershipClient::connect(seed)
            .map_err(|e| JoinError::Phase1(e.into()))
            .await?;

        (c.pre_join(req).map_ok(|r| r.into_inner()))
            .map_err(|e| JoinError::Phase1(e.into()))
            .await
    }

    /// Initiate phase 2 of the join protocol with the provided `observer`.
    async fn join_phase2(&self, req: JoinReq, observer: Endpoint) -> Result<JoinResp, JoinError> {
        let observer = self.resolve_endpoint(&observer)?;

        let mut c = MembershipClient::connect(observer)
            .map_err(|e| JoinError::Phase2(e.into()))
            .await?;

        (c.join(req).map_ok(|r| r.into_inner()))
            .map_err(|e| JoinError::Phase2(e.into()))
            .await
    }
}
