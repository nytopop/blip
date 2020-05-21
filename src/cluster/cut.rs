// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Multi-node cuts and friends.
use super::{proto, Metadata, State};
use futures::stream::{unfold, Stream};
use rand::{thread_rng, Rng};
use std::{
    collections::HashMap,
    convert::TryInto,
    net::SocketAddr,
    sync::{Arc, Weak},
};
use tokio::sync::{
    broadcast::{Receiver, RecvError},
    RwLock,
};
use tonic::transport::{self, ClientTlsConfig};

/// An error returned by [recv](Subscription::recv) if the subscription source was dropped.
///
/// If this error is received, the local node can be assumed to have stopped executing.
#[derive(Copy, Clone, Debug, failure_derive::Fail)]
#[fail(display = "closed")]
pub struct Closed;

/// A subscription to accepted view-change proposals.
pub struct Subscription {
    state: Weak<RwLock<State>>,
    rx: Receiver<MultiNodeCut>,
}

impl Subscription {
    pub(crate) fn new(state: Weak<RwLock<State>>, rx: Receiver<MultiNodeCut>) -> Self {
        Self { state, rx }
    }

    /// Resolves when the next view-change proposal is accepted, or the subscription ends.
    pub async fn recv(&mut self) -> Result<MultiNodeCut, Closed> {
        let n = match self.rx.recv().await {
            Ok(view_change) => {
                return Ok(view_change);
            }

            Err(RecvError::Closed) => {
                return Err(Closed);
            }

            Err(RecvError::Lagged(n)) => n,
        };

        let state = self.state.upgrade().ok_or(Closed)?;
        let state = state.read().await;

        let mut cut = state.last_cut.clone().ok_or(Closed)?;
        cut.skipped = n;

        Ok(cut)
    }

    /// Consume this subscription, converting it into a [Stream] of view-change proposals.
    pub fn into_stream(self) -> impl Stream<Item = MultiNodeCut> {
        unfold(self, |mut s| async { Some((s.recv().await.ok()?, s)) })
    }

    /// Borrow this subscription as a [Stream] of view-change proposals.
    pub fn as_stream(&mut self) -> impl Stream<Item = MultiNodeCut> + '_ {
        unfold(self, |s| async { Some((s.recv().await.ok()?, s)) })
    }
}

/// An accepted view-change proposal. Cloning this is cheap, as membership information is
/// stored as refcounted slices.
#[derive(Clone, Debug)]
pub struct MultiNodeCut {
    pub(crate) skipped: u64,
    pub(crate) local_addr: SocketAddr,
    pub(crate) conf_id: u64,
    pub(crate) degraded: bool,
    pub(crate) members: Arc<[Member]>,
    pub(crate) joined: Arc<[Member]>,
    pub(crate) kicked: Arc<[Member]>,
}

impl MultiNodeCut {
    /// Returns the number of cuts that were skipped between this and the last received
    /// cut.
    ///
    /// If this isn't 0, [joined](MultiNodeCut::joined) and [kicked](MultiNodeCut::kicked)
    /// most likely do not represent the complete set of membership changes since the last
    /// cut.
    ///
    /// On the other hand, [members](MultiNodeCut::members) will always be complete.
    pub fn skipped(&self) -> u64 {
        self.skipped
    }

    /// Returns the local node's listening address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the accepted configuration id.
    pub fn conf_id(&self) -> u64 {
        self.conf_id
    }

    /// Returns true if the local node is not a member of the configuration.
    pub fn is_degraded(&self) -> bool {
        self.degraded
    }

    /// Returns a random healthy member.
    ///
    /// # Panics
    /// Panics if the configuration is empty.
    pub(crate) fn random_member(&self) -> &Member {
        &self.members[thread_rng().gen_range(0, self.members.len())]
    }

    /// Returns all members in the configuration.
    pub fn members(&self) -> &Arc<[Member]> {
        &self.members
    }

    /// Returns any members that joined.
    pub fn joined(&self) -> &Arc<[Member]> {
        &self.joined
    }

    /// Returns any members that were kicked.
    pub fn kicked(&self) -> &Arc<[Member]> {
        &self.kicked
    }
}

/// A cluster member.
///
/// This is meant to be used via its [From] impl for [Endpoint](transport::Endpoint), which
/// will have tls settings configured for convenience.
#[derive(Clone, Debug)]
pub struct Member {
    pub(crate) addr: SocketAddr,
    pub(crate) tls: Option<Arc<ClientTlsConfig>>,
    pub(crate) meta: Metadata,
}

impl From<&Member> for proto::Endpoint {
    fn from(Member { addr, tls, .. }: &Member) -> Self {
        Self::from(*addr).tls(tls.is_some())
    }
}

impl From<&Member> for transport::Endpoint {
    fn from(Member { addr, tls, .. }: &Member) -> Self {
        match tls.as_deref().cloned() {
            Some(tls) => format!("https://{}", addr)
                .try_into()
                .map(|e: Self| e.tls_config(tls)),
            None => format!("http://{}", addr).try_into(),
        }
        .unwrap()
    }
}

impl Member {
    /// Returns the member's socket address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns a reference to the tls configuration that will be used for outgoing conns
    /// to this member, or `None` if it isn't expecting tls.
    pub fn tls_config(&self) -> Option<&ClientTlsConfig> {
        self.tls.as_deref()
    }

    /// Returns a reference to the member's metadata.
    pub fn metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.meta.keys
    }
}
