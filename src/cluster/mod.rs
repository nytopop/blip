// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! A distributed membership protocol based closely on [rapid].
//!
//! # References
//! * [Stable and Consistent Membership at Scale with Rapid][rapid]
//! * [Fast Paxos][fpx]
//!
//! [rapid]: https://arxiv.org/abs/1803.03620
//! [fpx]: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
pub mod cut;
pub mod partition;
mod proto;

use super::collections::{EventFilter, EventId, FreqSet, Tumbler};
use cut::{Member, MultiNodeCut, Subscription};
use proto::{
    broadcast_req::{Broadcasted::*, *},
    membership_client::*,
    membership_server::*,
    *,
};

use fnv::FnvHasher;
use futures::{
    future::{join, FutureExt, TryFutureExt},
    stream::{FuturesUnordered, StreamExt},
};
use log::{error, info, warn};
use rand::{thread_rng, Rng};
use std::{
    collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap, HashSet},
    convert::TryInto,
    future::Future,
    hash::{Hash, Hasher},
    mem,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    select,
    sync::{broadcast, oneshot, RwLock},
    task,
    time::{delay_for, timeout},
};
use tonic::{
    transport::{self, ClientTlsConfig},
    Code, Request, Response, Status,
};

#[doc(hidden)]
pub struct Config<St> {
    #[allow(dead_code)]
    pub(crate) strategy: St,
    pub(crate) lh: (usize, usize),
    pub(crate) k: usize,
    pub(crate) seed: Option<Endpoint>,
    pub(crate) meta: Metadata,
    pub(crate) server_tls: bool,
    pub(crate) client_tls: Option<Arc<ClientTlsConfig>>,
    pub(crate) fd_timeout: Duration,
    pub(crate) fd_strikes: usize,
}

type Grpc<T> = Result<T, Status>;
type GrpcResponse<T> = Grpc<Response<T>>;

#[doc(hidden)]
pub struct Cluster<St> {
    cfg: Config<St>,
    addr: SocketAddr,
    state: Arc<RwLock<State>>,
    cuts: broadcast::Sender<MultiNodeCut>,
}

#[crate::async_trait]
impl<St: partition::Strategy> Membership for Arc<Cluster<St>> {
    /// Handle a pre-join request (phase 1 of the join protocol).
    ///
    /// The only thing we need to do is inform the joiner about which nodes to contact in
    /// order to initiate phase 2 of the join protocol.
    async fn pre_join(&self, req: Request<PreJoinReq>) -> GrpcResponse<PreJoinResp> {
        let PreJoinReq { sender, uuid } = req.into_inner();
        sender.validate()?;

        let state = self.state.read().await;

        state.verify_unused_host(&sender)?;
        state.verify_unused_uuid(&uuid)?;

        let resp = PreJoinResp {
            sender: self.local_node(),
            conf_id: state.conf_id,
            contact: state.nodes.predecessors(&sender).cloned().collect(),
        };

        Ok(Response::new(resp))
    }

    /// Handle a join request (phase 2 of the join protocol).
    ///
    /// The sender has (presumably) already performed a pre-join request, and received a
    /// list of observers to contact (of which we are ~one (presumably)).
    ///
    /// Our role here is to notify the rest of the cluster that sender wants to join.
    async fn join(&self, req: Request<JoinReq>) -> GrpcResponse<JoinResp> {
        #[rustfmt::skip]
        let JoinReq { sender, ring, uuid, conf_id, meta } = req.into_inner();
        sender.validate()?;
        let mut state = self.state.write().await;

        state.verify_config(conf_id)?;
        state.verify_unused_host(&sender)?;
        state.verify_unused_uuid(&uuid)?;
        state.verify_ring(&self.local_node(), &sender, ring)?;

        self.enqueue_edge(&mut state, Edge {
            node: sender.clone(),
            ring,
            join: Some(Join { uuid, meta }),
        });

        match state.join_requests.get(&sender) {
            Some(pending) if pending.task_is_waiting() => {
                return Err(Status::already_exists("another join request is pending"));
            }
            _ => {}
        }

        let (pending, join) = join_task();
        state.join_requests.insert(sender, pending);
        drop(state);

        Ok(Response::new(join.await.unwrap()))
    }

    /// Merge a batch of alerts into the local cut detection state.
    ///
    /// If the local set of alerts (after merging this batch) satisfies the aggregation
    /// rule in [section 4.2](https://arxiv.org/pdf/1803.03620.pdf#subsection.4.2) of the
    /// rapid paper, we initiate a consensus round for a view-change proposal.
    async fn batched_alert(&self, req: Request<BatchedAlertReq>) -> GrpcResponse<Ack> {
        #[rustfmt::skip]
        let BatchedAlertReq { sender, conf_id, edges } = req.into_inner();
        let mut state = self.state.write().await;

        state.verify_sender(&sender)?;
        state.verify_config(conf_id)?;

        if state.fpx_announced {
            return Err(Status::aborted("already announced fast paxos round"));
        }

        (edges.iter())
            .map(|e| state.verify_edge(&sender, e))
            .collect::<Grpc<()>>()?;

        for Edge { node, ring, join } in edges {
            state.merge_cd_alert(node, join, Vote {
                node: sender.clone(),
                ring,
            });
        }

        state.merge_implicit_cd_alerts(self.cfg.lh);

        if let Some(proposal) = state.generate_cd_proposal(self.cfg.lh) {
            state.register_fpx_round(&proposal);

            info!("starting fast round: conf_id={}", conf_id);
            task::spawn(Arc::clone(self).do_broadcast(FastPhase2b(FastPhase2bReq {
                sender: self.local_node(),
                conf_id,
                nodes: proposal,
            })));

            task::spawn(Arc::clone(self).begin_px_round(PaxosRound {
                sender: self.local_node(),
                conf_id,
                members: state.nodes.len(),
            }));
        }

        Ok(Response::new(Ack {}))
    }

    /// Handle a fast-phase2b request.
    ///
    /// This rpc covers the most common view-change scenario where most members in the
    /// cluster agree on the same proposal. In this case, a single round of fast paxos
    /// is sufficient to maintain strong consistency.
    ///
    /// We sum all votes received thus far, and apply the view-change proposal iff there
    /// is a quorum (>= 3/4) of votes for it.
    async fn fast_phase2b(&self, req: Request<FastPhase2bReq>) -> GrpcResponse<Ack> {
        #[rustfmt::skip]
        let FastPhase2bReq { sender, conf_id, nodes } = req.into_inner();
        let mut state = self.state.write().await;

        state.verify_sender(&sender)?;
        state.verify_config(conf_id)?;

        if !state.fpx_voters.insert(sender) {
            return Err(Status::already_exists("sender has already voted"));
        }

        let quorum = state.fast_quorum();

        let ballot = match state.fpx_ballots.entry(nodes) {
            Entry::Occupied(mut o) => {
                *o.get_mut() += 1;
                o
            }
            entry => entry.insert(1),
        };

        if *ballot.get() >= quorum {
            let (proposal, votes) = ballot.remove_entry();
            self.apply_view_change(&mut state, proposal);
            info!(
                "(fast) applied view-change with {} vote(s): conf_id={}",
                votes, state.conf_id
            );
        }

        Ok(Response::new(Ack {}))
    }

    // TODO(doc)
    async fn phase1a(&self, req: Request<Phase1aReq>) -> GrpcResponse<Ack> {
        #[rustfmt::skip]
        let Phase1aReq { sender, conf_id, rank } = req.into_inner();
        let mut state = self.state.write().await;

        state.verify_sender(&sender)?;
        state.verify_config(conf_id)?;

        if rank <= state.px_rnd {
            return Err(Status::aborted("rank is too low"));
        }

        let sender = self
            .resolve_endpoint(&sender)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let mut sender = MembershipClient::connect(sender)
            .map_err(|e| Status::unavailable(e.to_string()))
            .await?;

        state.px_rnd = rank.clone();

        let p1b = Phase1bReq {
            sender: self.local_node(),
            conf_id,
            rnd: rank,
            vrnd: state.px_vrnd.clone(),
            vval: state.px_vval.clone(),
        };

        let send = async move { sender.phase1b(p1b).await };
        task::spawn(send.map_err(|e| warn!("phase1b failed: {}", e)));

        Ok(Response::new(Ack {}))
    }

    // TODO(doc)
    async fn phase1b(&self, req: Request<Phase1bReq>) -> GrpcResponse<Ack> {
        let req = req.into_inner();
        let mut state = self.state.write().await;

        state.verify_sender(&req.sender)?;
        state.verify_config(req.conf_id)?;

        if req.rnd != state.px_crnd {
            return Err(Status::aborted("rnd is not our crnd"));
        }

        state.px_p1bs.push(req);

        let quorum = state.slow_quorum();
        let votes = state.px_p1bs.len();

        if votes > quorum && state.px_cval.is_empty() {
            if let Some(proposal) = state.choose_px_proposal() {
                state.px_cval = proposal.clone();

                task::spawn(Arc::clone(self).do_broadcast(Phase2a(Phase2aReq {
                    sender: self.local_node(),
                    conf_id: state.conf_id,
                    rnd: state.px_crnd.clone(),
                    vval: proposal,
                })));
            }
        }

        Ok(Response::new(Ack {}))
    }

    // TODO(doc)
    async fn phase2a(&self, req: Request<Phase2aReq>) -> GrpcResponse<Ack> {
        #[rustfmt::skip]
        let Phase2aReq { sender, conf_id, rnd, vval } = req.into_inner();
        let mut state = self.state.write().await;

        state.verify_sender(&sender)?;
        state.verify_config(conf_id)?;

        if rnd < state.px_rnd {
            return Err(Status::aborted("rnd is too low"));
        }
        if rnd == state.px_vrnd {
            return Err(Status::aborted("rnd is equal to vrnd"));
        }

        state.px_rnd = rnd.clone();
        state.px_vrnd = rnd.clone();
        state.px_vval = vval.clone();

        task::spawn(Arc::clone(self).do_broadcast(Phase2b(Phase2bReq {
            sender: self.local_node(),
            conf_id,
            rnd,
            nodes: vval,
        })));

        Ok(Response::new(Ack {}))
    }

    // TODO(doc)
    async fn phase2b(&self, req: Request<Phase2bReq>) -> GrpcResponse<Ack> {
        #[rustfmt::skip]
        let Phase2bReq { sender, conf_id, rnd, nodes } = req.into_inner();
        let mut state = self.state.write().await;

        state.verify_sender(&sender)?;
        state.verify_config(conf_id)?;

        let quorum = state.slow_quorum();

        let mut ballot = match state.px_rsps.entry(rnd) {
            Entry::Occupied(o) => o,
            e => e.insert((HashSet::new(), nodes)),
        };

        let (voters, _) = ballot.get_mut();
        voters.insert(sender);

        if voters.len() > quorum {
            let (voters, proposal) = ballot.remove();
            self.apply_view_change(&mut state, proposal);
            info!(
                "(slow) applied view-change with {} vote(s): conf_id={}",
                voters.len(),
                state.conf_id
            );
        }

        Ok(Response::new(Ack {}))
    }

    /// Handle a broadcast request.
    ///
    /// Each broadcast contains a persistent pseudo-unique id (within some sliding window
    /// of time) which will force it to be processed at most once on any given node.
    ///
    /// If the request id has not yet been seen on this node, the inner request will be
    /// handled locally, and the broadcast redistributed to all of this node's subjects.
    ///
    /// This infection-style method of broadcast dissemination scales very well to large
    /// clusters - the total network load is distributed amongst all receiving members -
    /// but necessarily introduces a constant multiple of overhead (mostly in the form of
    /// redundantly delivered messages) compared to unicasting messages to each receiver
    /// from a single source.
    ///
    /// In addition to the load distribution property, it provides a very high confidence
    /// of delivery at all healthy members.
    async fn broadcast(&self, req: Request<BroadcastReq>) -> GrpcResponse<Ack> {
        #[rustfmt::skip]
        let BroadcastReq { unix, uniq, broadcasted } = req.into_inner();

        let broadcasted = broadcasted //
            .ok_or_else(|| Status::invalid_argument("missing oneof"))?;
        let event_id = EventId::new(unix, uniq);

        let mut subjects: Vec<_> = {
            let mut state = self.state.write().await;

            if !state.bcast_filter.insert(event_id) {
                return Err(Status::already_exists("delivery is redundant"));
            }

            (state.nodes)
                .successors(&self.local_node())
                .cloned()
                .collect()
        };

        subjects.sort();
        subjects.dedup();

        for subject in subjects {
            let req = Request::new(BroadcastReq {
                unix,
                uniq,
                broadcasted: Some(broadcasted.clone()),
            });

            let subject = self
                .resolve_endpoint(&subject)
                .expect("all stored endpoints are valid");

            let send = async move {
                let mut client = MembershipClient::connect(subject)
                    .map_err(|e| Status::unavailable(e.to_string()))
                    .await?;

                client.broadcast(req).await
            };

            task::spawn(send.map_err(|e| match (e.code(), e.message()) {
                (Code::AlreadyExists, "delivery is redundant") => {}
                _ => warn!("infection failed: {}", e),
            }));
        }

        match broadcasted {
            BatchedAlert(ba) => self.batched_alert(Request::new(ba)).await,
            FastPhase2b(fp2b) => self.fast_phase2b(Request::new(fp2b)).await,
            Phase1a(p1a) => self.phase1a(Request::new(p1a)).await,
            Phase1b(p1b) => self.phase1b(Request::new(p1b)).await,
            Phase2a(p2a) => self.phase2a(Request::new(p2a)).await,
            Phase2b(p2b) => self.phase2b(Request::new(p2b)).await,
        }
    }

    /// Handle a fault-detection probe message.
    async fn probe(&self, _: Request<Ack>) -> GrpcResponse<Ack> {
        let State { ref nodes, .. } = *self.state.read().await;

        if !nodes.contains(&self.local_node()) {
            return Err(Status::unavailable("degraded"));
        }

        Ok(Response::new(Ack {}))
    }
}

struct Subject {
    node: Endpoint,
    rings: Vec<u64>,
    faults: usize,
}

impl<St: partition::Strategy> Cluster<St> {
    pub(crate) fn new(cfg: Config<St>, addr: SocketAddr) -> Self {
        let state = Arc::new(RwLock::new(State {
            uuid: NodeId::generate(),
            conf_id: 0,
            nodes: Tumbler::new(cfg.k),
            uuids: BTreeSet::new(),
            metadata: HashMap::new(),
            last_cut: None,

            bcast_filter: EventFilter::new(Duration::from_secs(3600)),

            join_requests: HashMap::new(),

            cd_batch: AlertBatch::default(),
            cd_joiners: HashMap::new(),
            cd_reports: BTreeMap::new(),

            fpx_announced: false,
            fpx_voters: HashSet::new(),
            fpx_ballots: HashMap::new(),

            px_rnd: Rank::zero(),
            px_vrnd: Rank::zero(),
            px_crnd: Rank::zero(),
            px_vval: Vec::new(),
            px_cval: Vec::new(),
            px_rsps: HashMap::new(),
            px_p1bs: Vec::new(),
        }));

        let (cuts, _) = broadcast::channel(8);

        Cluster {
            cfg,
            addr,
            state,
            cuts,
        }
    }

    #[inline]
    pub(crate) fn into_service(self: Arc<Self>) -> MembershipServer<Arc<Self>> {
        MembershipServer::new(self)
    }

    pub(crate) fn subscribe(&self) -> Subscription {
        let state = Arc::downgrade(&self.state);
        let rx = self.cuts.subscribe();
        Subscription::new(state, rx)
    }

    #[inline]
    fn local_node(&self) -> Endpoint {
        Endpoint::from(self.addr).tls(self.cfg.server_tls)
    }

    /// Run the fault detector until the cluster is brought down.
    pub(crate) async fn detect_faults(self: Arc<Self>, mut cuts: Subscription) -> cut::Result {
        loop {
            select! {
                _ = self.spin_fd_probes() => {}
                cut = cuts.recv() => { cut?; }
            }
        }
    }

    /// Spin the fault detector until the next view-change proposal is accepted.
    async fn spin_fd_probes(self: &Arc<Self>) {
        let (conf_id, mut subjects) = async {
            let state = self.state.read().await;
            let mut subjects: Vec<Subject> = Vec::with_capacity(self.cfg.k);

            // we might have been assigned the same subject on multiple rings, so we dedupe
            // by subject and track which rings we're authoritative over.
            for (ring, subject) in (state.nodes.successors(&self.local_node()))
                .cloned()
                .enumerate()
            {
                match subjects.binary_search_by_key(&&subject, |s| &s.node) {
                    Err(i) => subjects.insert(i, Subject {
                        node: subject,
                        rings: vec![ring as u64],
                        faults: 0,
                    }),
                    Ok(at) => subjects[at].rings.push(ring as u64),
                }
            }

            (state.conf_id, subjects)
        }
        .await;

        loop {
            // start sending off probes, each of which times out after fd_timeout.
            let probes = (subjects.iter_mut())
                .map(|s| self.probe_subject(s))
                .collect::<FuturesUnordered<_>>()
                .for_each(|_| async {});

            // wait for all probes to finish, and for fd_timeout to elapse. this caps the
            // rate at which subjects are probed to k per fd_timeout.
            let mut state = join(delay_for(self.cfg.fd_timeout), probes)
                .then(|_| self.state.write())
                .await;

            // if a there's been a view-change, we need to reload subjects as they might
            // have become invalidated. most of the time we'll instead be pre-empted and
            // cancelled before locking (during the probe).
            if state.conf_id != conf_id {
                break;
            }

            // subjects are marked as faulted if they failed fd_strikes successive probes.
            let faulted = (subjects.iter_mut())
                .filter(|s| s.faults >= self.cfg.fd_strikes)
                .flat_map(|s| {
                    let e = &s.node;
                    s.faults = 0;
                    s.rings.iter().map(move |ring| Edge::down(e.clone(), *ring))
                });

            self.enqueue_edges(&mut *state, faulted);
        }
    }

    /// Probe a subject, modifying its successive `faults` count appropriately upon success
    /// or failure.
    async fn probe_subject(&self, subject: &mut Subject) {
        let send_probe = timeout(self.cfg.fd_timeout, async {
            let e = self.resolve_endpoint(&subject.node).ok()?;
            let mut c = MembershipClient::connect(e).await.ok()?;
            c.probe(Ack {}).await.ok()
        });

        match send_probe.await.ok().flatten() {
            Some(_) => subject.faults = 0,
            None => subject.faults += 1,
        }
    }

    /// Resolve an `Endpoint` to a `transport::Endpoint`, applying the configured client TLS
    /// settings if specified by the endpoint.
    fn resolve_endpoint(&self, e: &Endpoint) -> Result<transport::Endpoint, EndpointError> {
        if !e.tls {
            return e.try_into();
        }

        let tls = (self.cfg.client_tls)
            .as_deref()
            .cloned()
            .unwrap_or_else(ClientTlsConfig::new);

        e.try_into().map(|e: transport::Endpoint| e.tls_config(tls))
    }

    /// Apply a view-change proposal to `state`. This will propagate the view-change to any
    /// subscribed tasks, and unblock any joining nodes in the proposal (if we're the ones
    /// handling their join request).
    fn apply_view_change(&self, state: &mut State, proposal: Vec<Endpoint>) {
        let mut joined = Vec::with_capacity(proposal.len());
        let mut kicked = Vec::with_capacity(proposal.len());

        for node in proposal {
            if let Some(Join { uuid, meta }) = state.cd_joiners.remove(&node) {
                joined.push(self.resolve_member_meta(meta.clone(), &node).unwrap());
                state.join_node(node, Join { uuid, meta });
            } else {
                let meta = state.kick_node(&node);
                kicked.push(self.resolve_member_meta(meta, &node).unwrap());
            }
        }

        state.clear_consensus();

        let local_node = self.local_node();

        joined.sort_by_key(|m| (m.addr().ip(), m.addr().port()));
        kicked.sort_by_key(|m| (m.addr().ip(), m.addr().port()));

        let mut members: Vec<_> = (state.nodes.iter())
            .map(|node| self.resolve_member(state, node).unwrap())
            .collect();

        members.sort_by_key(|m| (m.addr().ip(), m.addr().port()));

        let cut = MultiNodeCut {
            skipped: 0,
            local_addr: self.addr,
            degraded: !state.nodes.contains(&local_node),
            conf_id: state.refresh_config(),
            members: members.into(),
            joined: joined.into(),
            kicked: kicked.into(),
        };

        state.respond_to_joiners(&cut, local_node);
        state.last_cut = Some(cut.clone());
        self.propagate_cut(cut);
    }

    /// Resolve a [Member] by looking up its metadata in the cluster state.
    fn resolve_member(&self, state: &State, peer: &Endpoint) -> ResolvedMember {
        let addr: SocketAddr = peer.try_into()?;

        let meta = (state.metadata)
            .get(peer)
            .ok_or(MemberResolutionError::MissingMetadata)?
            .clone();

        let tls = self.get_client_tls(peer.tls);

        Ok(Member { addr, tls, meta })
    }

    /// Resolve a [Member] without performing a metadata lookup.
    ///
    /// This is useful if the endpoint has not been added to the cluster state.
    fn resolve_member_meta(&self, meta: Metadata, peer: &Endpoint) -> ResolvedMember {
        let addr: SocketAddr = peer.try_into()?;
        let tls = self.get_client_tls(peer.tls);

        Ok(Member { addr, tls, meta })
    }

    /// Get a client tls config, if `enabled`.
    fn get_client_tls(&self, enabled: bool) -> Option<Arc<ClientTlsConfig>> {
        guard! { enabled };

        let tls = (self.cfg.client_tls)
            .clone()
            .unwrap_or_else(|| Arc::new(ClientTlsConfig::new()));

        Some(tls)
    }

    /// Propagate a view-change proposal to all subscribed tasks. This includes the fault
    /// detector, partition detector, and any subscribed mesh services (from the overlay).
    #[inline]
    fn propagate_cut(&self, cut: MultiNodeCut) {
        // NOTE: this might fail, but if it does we'll exit soon anyway.
        let _ = self.cuts.send(cut);
    }

    /// Enqueue an edge to be included in the next batched broadcast.
    #[inline]
    fn enqueue_edge(self: &Arc<Self>, state: &mut State, edge: Edge) {
        self.enqueue_edges(state, std::iter::once(edge));
    }

    /// Enqueue some edge(s) to be included in the next batched broadcast.
    fn enqueue_edges<I: IntoIterator<Item = Edge>>(self: &Arc<Self>, state: &mut State, iter: I) {
        #[rustfmt::skip]
        let AlertBatch { started, conf_id, edges } = &mut state.cd_batch;

        // if there are any edges from a prior configuration, delete them.
        if mem::replace(conf_id, state.conf_id) != state.conf_id {
            edges.clear();
        }

        // add any new edges for send_batch to propagate.
        let n = edges.len();
        edges.extend(iter);

        // if we added edges and there's no send_batch task waiting, start one.
        if edges.len() != n && !mem::replace(started, true) {
            task::spawn(Arc::clone(self).send_batch());
        }
    }

    /// Send a batched cut detection broadcast. This coalesces any edges enqueued within the
    /// same ~short period of time into the same batch.
    async fn send_batch(self: Arc<Self>) {
        // wait a bit to allow more edges to be included in this batch.
        delay_for(Duration::from_millis(100)).await;

        let sender = self.local_node();

        // replace the state's cd_batch with an empty one.
        let AlertBatch { conf_id, edges, .. } = {
            let mut state = self.state.write().await;
            mem::take(&mut state.cd_batch)
        };

        // if configuration changed without adding new edges, there's nothing to send.
        if edges.is_empty() {
            return;
        }

        self.do_broadcast(BatchedAlert(BatchedAlertReq {
            sender,
            conf_id,
            edges,
        }))
        .await;
    }

    async fn do_broadcast(self: Arc<Self>, msg: Broadcasted) {
        let id = EventId::generate();

        let req = Request::new(BroadcastReq {
            unix: id.timestamp(),
            uniq: id.unique(),
            broadcasted: Some(msg),
        });

        if let Err(e) = self.broadcast(req).await {
            warn!("broadcast failed: {}", e);
        }
    }

    async fn begin_px_round(self: Arc<Self>, px: PaxosRound) {
        fn fnv_hash<T: Hash>(val: T) -> u64 {
            let mut h = FnvHasher::default();
            val.hash(&mut h);
            h.finish()
        }

        #[rustfmt::skip]
        let PaxosRound { sender, conf_id, .. } = px.init_delay().await;

        let rank = {
            let mut state = self.state.write().await;

            // if a fast or classical round has already changed conf_id, bail.
            if state.conf_id != conf_id {
                return;
            }
            // if a classical paxos round has already begun, bail.
            if state.px_crnd.round > 2 {
                return;
            }

            state.px_crnd.round = 2;
            state.px_crnd.node_idx = fnv_hash(&sender);
            state.px_crnd.clone()
        };

        info!("starting slow round: conf_id={}", conf_id);

        self.do_broadcast(Phase1a(Phase1aReq {
            sender,
            conf_id,
            rank,
        }))
        .await;
    }
}

struct PaxosRound {
    sender: Endpoint,
    conf_id: u64,
    members: usize,
}

impl PaxosRound {
    async fn init_delay(self) -> Self {
        let exp = ((self.members + 1) as f64).log(2.0) * 4000.0;
        let ms = thread_rng().gen_range(1000, exp as u64);
        delay_for(Duration::from_millis(ms)).await;
        self
    }
}

#[derive(Copy, Clone, Debug, Error)]
enum MemberResolutionError {
    #[error("invalid socketaddr: {}", .0)]
    InvalidSocketAddr(#[from] SocketAddrError),

    #[error("missing metadata")]
    MissingMetadata,
}

type ResolvedMember = Result<Member, MemberResolutionError>;

struct JoinTask {
    rx: oneshot::Receiver<JoinResp>,
    #[allow(dead_code)]
    tx: Weak<oneshot::Sender<JoinResp>>,
}

impl Future for JoinTask {
    type Output = Result<JoinResp, oneshot::error::RecvError>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(ctx)
    }
}

struct PendingJoin {
    tx: Arc<oneshot::Sender<JoinResp>>,
}

impl PendingJoin {
    /// Returns false if the originating task has dropped their [JoinTask].
    #[inline]
    fn task_is_waiting(&self) -> bool {
        Arc::weak_count(&self.tx) > 0
    }

    /// Notify the waiting task that a response is available.
    fn complete(self, join: JoinResp) {
        if !self.task_is_waiting() {
            return;
        }

        let _ = Arc::try_unwrap(self.tx)
            .expect("no other references to exist")
            .send(join);
    }
}

fn join_task() -> (PendingJoin, JoinTask) {
    let (tx, rx) = oneshot::channel();

    let tx = Arc::new(tx);
    let pending = PendingJoin { tx };

    let tx = Arc::downgrade(&pending.tx);
    let join = JoinTask { rx, tx };

    (pending, join)
}

#[derive(Default)]
struct AlertBatch {
    started: bool,
    conf_id: u64,
    edges: Vec<Edge>,
}

#[derive(PartialEq, Eq, Hash)]
struct Vote {
    node: Endpoint,
    ring: u64,
}

pub(crate) struct State {
    // membership state
    uuid: NodeId,
    conf_id: u64,
    nodes: Tumbler<Endpoint>,
    uuids: BTreeSet<NodeId>,
    metadata: HashMap<Endpoint, Metadata>,
    last_cut: Option<MultiNodeCut>,

    // broadcast protocol state
    bcast_filter: EventFilter,

    // join protocol state
    join_requests: HashMap<Endpoint, PendingJoin>,

    // cut detection state
    cd_batch: AlertBatch,
    cd_joiners: HashMap<Endpoint, Join>,
    cd_reports: BTreeMap<Endpoint, HashSet<Vote>>,

    // fast paxos state
    fpx_announced: bool,
    fpx_voters: HashSet<Endpoint>,
    fpx_ballots: HashMap<Vec<Endpoint>, usize>,

    // classical paxos state
    px_rnd: Rank,
    px_vrnd: Rank,
    px_crnd: Rank,
    px_vval: Vec<Endpoint>,
    px_cval: Vec<Endpoint>,
    px_rsps: HashMap<Rank, (HashSet<Endpoint>, Vec<Endpoint>)>,
    px_p1bs: Vec<Phase1bReq>,
}

#[inline]
fn err_when<F: FnOnce() -> Status>(cond: bool, err: F) -> Grpc<()> {
    if cond {
        Err(err())
    } else {
        Ok(())
    }
}

impl State {
    /// Verify that `host` is not already a member in the active configuration.
    fn verify_unused_host(&self, host: &Endpoint) -> Grpc<()> {
        err_when(self.nodes.contains(host), || {
            Status::already_exists("host already exists")
        })
    }

    /// Verify that `uuid` has not yet been used by any node in the active configuration.
    fn verify_unused_uuid(&self, uuid: &NodeId) -> Grpc<()> {
        err_when(self.uuids.contains(uuid), || {
            Status::already_exists("uuid already exists")
        })
    }

    /// Verify that `src` is the `ring`th observer of `dst`.
    fn verify_ring(&self, src: &Endpoint, dst: &Endpoint, ring: u64) -> Grpc<()> {
        err_when(
            self.nodes
                .predecessors(dst)
                .nth(ring.try_into().unwrap())
                .filter(|e| *e == src)
                .is_none(),
            || Status::invalid_argument("invalid ring number"),
        )
    }

    /// Verify that `sender` is a member of the active configuration.
    fn verify_sender(&self, sender: &Endpoint) -> Grpc<()> {
        err_when(!self.nodes.contains(sender), || {
            Status::unauthenticated(format!("sender {:?} isn't a cluster member", sender))
        })
    }

    /// Verify that `conf_id` refers to the active configuration.
    fn verify_config(&self, conf_id: u64) -> Grpc<()> {
        err_when(self.conf_id != conf_id, || {
            Status::aborted("mismatched configuration id")
        })
    }

    /// Verify that `edge` doesn't violate any protocol invariants.
    fn verify_edge(&self, sender: &Endpoint, edge: &Edge) -> Grpc<()> {
        if edge.join.is_some() {
            edge.node.validate()?;
        }

        if edge.join.is_none() != self.nodes.contains(&edge.node) {
            // either a node is trying to join and is already a member, or a node is
            // not a member and is trying to leave.
            Err(Status::aborted("edge cannot apply to configuration"))
        } else {
            self.verify_ring(sender, &edge.node, edge.ring)
        }
    }

    /// Merge a cut detection alert into the local state.
    fn merge_cd_alert(&mut self, node: Endpoint, join: Option<Join>, src: Vote) {
        if let Some(join) = join {
            self.cd_joiners.insert(node.clone(), join);
        }
        self.cd_reports.entry(node).or_default().insert(src);
    }

    /// Merge implicit cut detection alerts into the local state.
    fn merge_implicit_cd_alerts(&mut self, (l, h): (usize, usize)) {
        let mut implied = vec![];
        for (subject, r) in self.cd_reports.iter() {
            if r.len() <= l || r.len() >= h {
                continue;
            }

            for (ring, observer) in self.nodes.predecessors(subject).enumerate() {
                let n = (self.cd_reports)
                    .get(observer)
                    .map(|r| r.len())
                    .unwrap_or_default();

                if n <= l || n >= h {
                    continue;
                }

                implied.push((subject.clone(), Vote {
                    node: observer.clone(),
                    ring: ring as u64,
                }));
            }
        }

        for (subject, vote) in implied {
            self.cd_reports.get_mut(&subject).map(|r| r.insert(vote));
        }
    }

    /// Attempt to generate a view-change proposal for this cut detection round.
    ///
    /// If this returns `Some(_)`, any pending edge reports will have been removed.
    fn generate_cd_proposal(&mut self, (l, h): (usize, usize)) -> Option<Vec<Endpoint>> {
        // if any reported edges are in unstable report mode, bail.
        guard! { self.cd_report_counts().all(|n| n >= h || n < l) }
        // if there isn't at least one edge in stable report mode, bail.
        guard! { self.cd_report_counts().any(|n| n >= h) }
        // if we've already announced a proposal round, bail.
        guard! { !mem::replace(&mut self.fpx_announced, true) }

        Some(self.drain_cd_reports_gte(h).collect())
    }

    /// Returns an iterator over the number of unique reports received for each edge in
    /// this cut detection round.
    #[inline]
    fn cd_report_counts(&self) -> impl Iterator<Item = usize> + '_ {
        self.cd_reports.values().map(HashSet::len)
    }

    /// Returns an iterator over all edges in this cut detection round that have received
    /// at least `h` unique reports.
    ///
    /// Drains all reports, even if the iterator is dropped before completing!
    fn drain_cd_reports_gte(&mut self, h: usize) -> impl Iterator<Item = Endpoint> {
        mem::take(&mut self.cd_reports)
            .into_iter()
            .filter_map(move |(e, r)| guard!(r.len() >= h, e))
    }

    /// Register the initiation of a fast paxos round.
    fn register_fpx_round(&mut self, proposal: &[Endpoint]) {
        if self.px_rnd.round >= 2 {
            return;
        }

        self.px_rnd = Rank::fast_round();
        self.px_vrnd = Rank::fast_round();
        self.px_vval = proposal.into();
    }

    /// Returns the size of quorum needed for a fast paxos proposal to be accepted.
    #[inline]
    fn fast_quorum(&self) -> usize {
        let sz = self.nodes.len() as f64;
        (sz * 0.75).ceil() as usize
    }

    /// Clear all consensus state in preparation for a new round.
    fn clear_consensus(&mut self) {
        // clear cut detection state
        self.cd_joiners.clear();
        self.cd_reports.clear();

        // clear fast paxos state
        self.fpx_announced = false;
        self.fpx_voters.clear();
        self.fpx_ballots.clear();

        // clear classical paxos state
        self.px_rnd = Rank::zero();
        self.px_vrnd = Rank::zero();
        self.px_crnd = Rank::zero();
        self.px_vval.clear();
        self.px_cval.clear();
        self.px_rsps.clear();
        self.px_p1bs.clear();
    }

    /// Clear all membership state in preparation for a join.
    fn clear_membership(&mut self) {
        self.nodes.clear();
        self.uuids.clear();
        self.metadata.clear();
        self.last_cut = None;
    }

    /// Add `node` to the active configuration.
    fn join_node(&mut self, node: Endpoint, Join { uuid, meta }: Join) {
        assert!(self.nodes.insert(node.clone()));
        assert!(self.uuids.insert(uuid));
        assert!(self.metadata.insert(node, meta).is_none());
    }

    /// Remove `node` from the active configuration.
    fn kick_node(&mut self, node: &Endpoint) -> Metadata {
        assert!(self.nodes.remove(node));
        self.metadata.remove(node).unwrap()
    }

    /// Re-hash the active configuration, returning (and setting) its id.
    fn refresh_config(&mut self) -> u64 {
        let mut h = FnvHasher::default();

        self.nodes.iter().for_each(|e| e.hash(&mut h));
        self.uuids.iter().for_each(|i| i.hash(&mut h));

        self.conf_id = h.finish();
        self.conf_id
    }

    /// Respond to any inflight join calls.
    fn respond_to_joiners(&mut self, cut: &MultiNodeCut, sender: Endpoint) {
        let resp = self.join_response(sender);

        (cut.joined())
            .iter()
            .filter_map(|m| self.join_requests.remove(&m.into()))
            .for_each(|p| p.complete(resp.clone()));
    }

    /// Returns a join response with the current configuration.
    fn join_response(&self, sender: Endpoint) -> JoinResp {
        JoinResp {
            sender,
            conf_id: self.conf_id,
            nodes: (self.metadata.iter())
                .map(|(node, meta)| NodeMetadata {
                    node: node.clone(),
                    meta: meta.clone(),
                })
                .collect(),
            uuids: self.uuids.iter().cloned().collect(),
        }
    }

    /// Returns the size of quorum needed for classical paxos to proceed.
    #[inline]
    fn slow_quorum(&self) -> usize {
        self.nodes.len() / 2
    }

    // TODO(doc)
    fn choose_px_proposal(&self) -> Option<Vec<Endpoint>> {
        // NOTE(invariant): there must be at least one phase1b request available
        assert!(!self.px_p1bs.is_empty());

        let max_vrnd = self.px_p1bs.iter().map(|p| &p.vrnd).max().unwrap();

        // Let k be the largest value of vr(a) for all a in Q.
        //     V be the set of all vv(a) for all a in Q s.t vr(a) == k
        let vvals: FreqSet<&[Endpoint]> = self
            .px_p1bs
            .iter()
            .filter(|p| &p.vrnd == max_vrnd)
            .filter(|p| !p.vval.is_empty())
            .map(|p| p.vval.as_slice())
            .collect();

        // If V has a single element, then choose v.
        if vvals.len() == 1 {
            return vvals.into_iter().next().map(|(vval, _)| vval.into());
        }

        // if i-quorum Q of acceptors respond, and there is a k-quorum R such that vrnd = k and vval = v,
        // for all a in intersection(R, Q) -> then choose "v". When choosing E = N/4 and F = N/2, then
        // R intersection Q is N/4 -- meaning if there are more than N/4 identical votes.
        if vvals.total() > 1 {
            if let Some(vval) = vvals
                .iter()
                .find(|(_, &votes)| votes > self.nodes.len() / 4)
                .map(|(&vval, _)| vval.into())
            {
                return Some(vval);
            }
        }

        // At this point, no value has been selected yet and it is safe for the coordinator to pick any
        // proposed value. If none of the 'vvals' contain valid values (are all empty lists), then this
        // method returns an empty list. This can happen because a quorum of acceptors that did not vote
        // in prior rounds may have responded to the coordinator first. This is safe to do here for two
        // reasons:
        //
        // 1) The coordinator will only proceed with phase 2 if it has a valid vote.
        //
        // 2) It is likely that the coordinator (itself being an acceptor) is the only one with a valid
        // vval, and has not heard a Phase1bMessage from itself yet. Once that arrives, phase1b will be
        // triggered again.
        self.px_p1bs
            .iter()
            .filter(|p| !p.vval.is_empty())
            .map(|p| p.vval.clone())
            .next()
    }
}
