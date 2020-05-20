// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Strategies for handling network partitions.
use super::{
    cut::{MultiNodeCut, Subscription},
    proto::{
        membership_client::MembershipClient, Endpoint, Join, JoinReq, JoinResp, NodeId,
        NodeMetadata, PreJoinReq,
    },
    Cluster, State,
};
use failure::{format_err, Fallible};
use futures::{
    future::ready,
    stream::{FuturesUnordered, StreamExt},
};
use std::{sync::Arc, time::Duration};
use tokio::time::{delay_for, timeout};

const JOIN_DELAY: Duration = Duration::from_millis(1000);

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
    async fn handle_parts(node: Arc<Cluster<Self>>, mut cuts: Subscription) -> Fallible<()>;
}

/// Rejoin the existing cluster through random healthy members.
#[derive(Default)]
pub struct Rejoin {}

impl private::Sealed for Rejoin {}

#[crate::async_trait]
impl Strategy for Rejoin {
    async fn handle_parts(node: Arc<Cluster<Self>>, mut cuts: Subscription) -> Fallible<()> {
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

            while !node.join_via(&cut.random_member().into()).await {
                delay_for(JOIN_DELAY).await;
            }
        }
    }
}

impl<St: Strategy> Cluster<St> {
    /// Initialize as if we just started up by attempting to join a seed node, or becoming
    /// a single node bootstrap cluster.
    async fn initialize(&self) {
        if let Some(seed) = self.cfg.seed.as_ref() {
            while !self.join_via(seed).await {
                delay_for(JOIN_DELAY).await;
            }
        } else {
            let mut state = self.state.write().await;

            state.clear_consensus();
            state.clear_membership();
            state.last_cut = None;

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
            conf_id: state.refresh_config(),
            degraded: false,
            members: members.clone(),
            joined: members,
            kicked: vec![].into(),
        };

        state.last_cut = Some(cut.clone());
        self.propagate_cut(cut);
    }

    /// Join a cluster through the provided seed node.
    ///
    /// Returns true if the seed's configuration was accepted. Conversely, returns false
    /// if the join failed for any reason.
    async fn join_via(&self, seed: &Endpoint) -> bool {
        const JOIN_TIMEOUT: Duration = Duration::from_secs(5);

        let mut state = self.state.write().await;

        state.uuid = NodeId::generate();

        let request = timeout(JOIN_TIMEOUT, self.request_join(&state, seed));

        let JoinResp { nodes, uuids, .. } = match request.await {
            Ok(Ok(join)) => join,
            _ => return false,
        };

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

        let cut = MultiNodeCut {
            skipped: 0,
            local_addr: self.addr,
            degraded: !state.nodes.contains(&self.local_node()),
            conf_id: state.refresh_config(),
            members: (state.nodes.iter())
                .map(|node| self.resolve_member(&state, node).unwrap())
                .collect(),
            joined: joined.into(),
            kicked: vec![].into(),
        };

        state.last_cut = Some(cut.clone());
        self.propagate_cut(cut);

        true
    }

    /// Request to join the provided seed node. Returns `Ok(_)` if both phases of the join
    /// protocol completed successfully, and `seed` .
    async fn request_join(&self, state: &State, seed: &Endpoint) -> Fallible<JoinResp> {
        let seed = self.resolve_endpoint(seed)?;
        let mut c = MembershipClient::connect(seed).await?;

        let p1j = PreJoinReq {
            sender: self.local_node(),
            uuid: state.uuid.clone(),
        };

        let r1 = c.pre_join(p1j).await?.into_inner();

        let p2j = JoinReq {
            sender: self.local_node(),
            ring: std::u64::MAX,
            uuid: state.uuid.clone(),
            conf_id: r1.conf_id,
            meta: self.cfg.meta.clone(),
        };

        let send_join_p2 = |(i, raw)| {
            let observer = self.resolve_endpoint(&raw);

            let mut p2j = p2j.clone();
            p2j.ring = i as u64;

            async move {
                let mut c = MembershipClient::connect(observer?).await?;
                c.join(p2j).await.map_err(|e| format_err!("{:?}", e))
            }
        };

        let mut joins = (r1.contact.into_iter())
            .enumerate()
            .map(send_join_p2)
            .collect::<FuturesUnordered<_>>()
            .filter_map(|r| ready(r.map(|jr| jr.into_inner()).ok()));

        match joins.next().await {
            Some(join) => Ok(join),
            None => Err(format_err!("join p2 failed")),
        }
    }
}
