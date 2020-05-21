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

use failure::{Compat, Fallible};
use fnv::FnvHasher;
use futures::{
    future::TryFutureExt,
    stream::{FuturesUnordered, StreamExt},
};
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
use tokio::{
    join, select,
    sync::{broadcast, oneshot, RwLock},
    task,
    time::{delay_for, delay_until, timeout, Instant},
};
use tonic::{
    transport::{self, ClientTlsConfig},
    Request, Response, Status,
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
        let state = self.state.read().await;

        if !state.nodes.contains(&sender) {
            state.verify_unused_uuid(&uuid)?;
        }

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
        let mut state = self.state.write().await;

        state.verify_config(conf_id)?;

        let local_node = self.local_node();

        if state.nodes.contains(&sender) {
            return Ok(Response::new(state.join_response(local_node)));
        }

        state.verify_unused_uuid(&uuid)?;
        state.verify_ring(&local_node, &sender, ring)?;

        self.propagate_edge(&mut state, Edge {
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
            return Err(Status::aborted("fast paxos round has already begun"));
        }

        let invalid_node = |e: &Edge| e.join.is_none() != state.nodes.contains(&e.node);
        let invalid_ring = |e: &Edge| state.verify_ring(&sender, &e.node, e.ring).is_err();

        if edges.iter().any(|e| invalid_node(e) || invalid_ring(e)) {
            return Err(Status::invalid_argument("invalid edge(s) in batched alert"));
        }

        for Edge { node, ring, join } in edges {
            state.merge_cd_alert(node, join, Vote {
                node: sender.clone(),
                ring,
            });
        }

        state.merge_implicit_cd_alerts(self.cfg.lh);

        if let Some(proposal) = state.generate_cd_proposal(self.cfg.lh) {
            state.register_fpx_round(&proposal);

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
            let (proposal, _) = ballot.remove_entry();
            self.apply_view_change(&mut state, proposal);
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

        task::spawn(async move {
            let _ = sender.phase1b(p1b).await;
        });

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
            let (_, proposal) = ballot.remove();
            self.apply_view_change(&mut state, proposal);
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

            task::spawn(async move {
                let mut client = MembershipClient::connect(subject)
                    .map_err(|e| Status::unavailable(e.to_string()))
                    .await?;

                client.broadcast(req).await
            });
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
    async fn probe(&self, _: Request<ProbeReq>) -> GrpcResponse<ProbeResp> {
        #[rustfmt::skip]
        let State { conf_id, ref nodes, .. } = *self.state.read().await;

        let status = if nodes.contains(&self.local_node()) {
            NodeStatus::Healthy
        } else {
            NodeStatus::Degraded
        } as i32;

        Ok(Response::new(ProbeResp { status, conf_id }))
    }
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

        let (cuts, _) = broadcast::channel(32);

        Cluster {
            cfg,
            addr,
            state,
            cuts,
        }
    }

    pub(crate) fn into_service(self: Arc<Self>) -> MembershipServer<Arc<Self>> {
        MembershipServer::new(self)
    }

    pub(crate) fn subscribe(&self) -> Subscription {
        let state = Arc::downgrade(&self.state);
        let rx = self.cuts.subscribe();
        Subscription::new(state, rx)
    }

    pub(crate) async fn detect_faults(self: Arc<Self>, mut cuts: Subscription) -> Fallible<()> {
        loop {
            select! {
                () = self.run_fd_round() => {}
                cut = cuts.recv() => { cut?; }
            }
        }
    }

    fn local_node(&self) -> Endpoint {
        Endpoint::from(self.addr).tls(self.cfg.server_tls)
    }

    async fn run_fd_round(self: &Arc<Self>) {
        // TODO(cfg): allow both of these to be configured
        const TIMEOUT: Duration = Duration::from_secs(1);
        const STRIKES: usize = 3;

        let mut subjects: Vec<_> = (self.state.read().await.nodes)
            .successors(&self.local_node())
            .cloned()
            .enumerate()
            .collect();

        for _ in 0..STRIKES {
            // start sending off probes, each of which times out at the ~same time.
            let probes = (subjects.iter().cloned())
                .map(|(ring, e)| timeout(TIMEOUT, self.probe_endpoint(ring, e)))
                .collect::<FuturesUnordered<_>>()
                .filter_map(|r| async move { r.ok() })
                .filter_map(|r| async move { r.ok() })
                .collect::<Vec<usize>>();

            // wait for all probes to finish, and for TIMEOUT to elapse. this caps the rate
            // at which failing subjects are probed to k per TIMEOUT, and healthy ones to k
            // per TIMEOUT*STRIKES.
            for ring in join![delay_for(TIMEOUT), probes].1 {
                let i = subjects.iter().position(|(r, _)| *r == ring).unwrap();
                subjects.remove(i);
            }
        }

        self.propagate_edges(
            &mut *self.state.write().await,
            subjects
                .into_iter()
                .map(|(ring, e)| Edge::down(e, ring as u64)),
        );
    }

    async fn probe_endpoint(&self, ring: usize, e: Endpoint) -> Fallible<usize> {
        let e = self.resolve_endpoint(&e)?;
        let mut c = MembershipClient::connect(e).await?;
        c.probe(ProbeReq {}).await?;

        Ok(ring)
    }

    fn resolve_endpoint(&self, e: &Endpoint) -> Result<transport::Endpoint, Compat<EndpointError>> {
        if !e.tls {
            return e.try_into();
        }

        let tls = (self.cfg.client_tls)
            .as_ref()
            .map(|tls| (&**tls).clone())
            .unwrap_or_else(ClientTlsConfig::new);

        e.try_into().map(|e: transport::Endpoint| e.tls_config(tls))
    }

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

        let cut = MultiNodeCut {
            skipped: 0,
            local_addr: self.addr,
            degraded: !state.nodes.contains(&local_node),
            conf_id: state.refresh_config(),
            members: (state.nodes.iter())
                .map(|node| self.resolve_member(state, node).unwrap())
                .collect(),
            joined: joined.into(),
            kicked: kicked.into(),
        };

        state.respond_to_joiners(&cut, local_node);
        state.last_cut = Some(cut.clone());
        self.propagate_cut(cut);
    }

    fn resolve_member(&self, state: &State, peer: &Endpoint) -> ResolvedMember {
        let addr: SocketAddr = peer
            .try_into()
            .map_err(MemberResolutionError::InvalidSocketAddr)?;

        let meta = (state.metadata)
            .get(peer)
            .ok_or(MemberResolutionError::MissingMetadata)?
            .clone();

        let tls = self.get_client_tls(peer.tls);

        Ok(Member { addr, tls, meta })
    }

    fn resolve_member_meta(&self, meta: Metadata, peer: &Endpoint) -> ResolvedMember {
        let addr: SocketAddr = peer
            .try_into()
            .map_err(MemberResolutionError::InvalidSocketAddr)?;

        let tls = self.get_client_tls(peer.tls);

        Ok(Member { addr, tls, meta })
    }

    fn get_client_tls(&self, enabled: bool) -> Option<Arc<ClientTlsConfig>> {
        guard! { enabled };

        let tls = (self.cfg.client_tls)
            .clone()
            .unwrap_or_else(|| Arc::new(ClientTlsConfig::new()));

        Some(tls)
    }

    fn propagate_cut(&self, cut: MultiNodeCut) {
        // NOTE: this might fail, but if it does we'll exit soon anyway.
        let _ = self.cuts.send(cut);
    }

    fn propagate_edge(self: &Arc<Self>, state: &mut State, edge: Edge) {
        self.propagate_edges(state, std::iter::once(edge));
    }

    fn propagate_edges<I: IntoIterator<Item = Edge>>(self: &Arc<Self>, state: &mut State, iter: I) {
        let now = Instant::now();

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
            task::spawn(Arc::clone(self).send_batch(now));
        }
    }

    async fn send_batch(self: Arc<Self>, started: Instant) {
        // wait a bit to allow more edges to be included in this batch.
        delay_until(started + Duration::from_millis(100)).await;

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

        let _ = self.broadcast(req).await;
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

#[derive(Debug, failure_derive::Fail)]
enum MemberResolutionError {
    #[fail(display = "invalid socketaddr: {:?}", _0)]
    InvalidSocketAddr(SocketAddrError),
    #[fail(display = "missing metadata")]
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

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(ctx)
    }
}

struct PendingJoin {
    tx: Arc<oneshot::Sender<JoinResp>>,
}

impl PendingJoin {
    /// Returns false if the originating task has dropped their [JoinTask].
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

impl State {
    /// Verify that `uuid` has not yet been used by any node in the active configuration.
    fn verify_unused_uuid(&self, uuid: &NodeId) -> Grpc<()> {
        if self.uuids.contains(uuid) {
            Err(Status::already_exists("uuid already exists"))
        } else {
            Ok(())
        }
    }

    /// Verify that `src` is the `ring`th observer of `dst`.
    fn verify_ring(&self, src: &Endpoint, dst: &Endpoint, ring: u64) -> Grpc<()> {
        if (self.nodes.predecessors(dst))
            .nth(ring.try_into().unwrap())
            .filter(|e| *e == src)
            .is_none()
        {
            Err(Status::invalid_argument("invalid ring number"))
        } else {
            Ok(())
        }
    }

    /// Verify that `sender` is a member of the active configuration.
    fn verify_sender(&self, sender: &Endpoint) -> Grpc<()> {
        if !self.nodes.contains(sender) {
            Err(Status::unauthenticated(format!(
                "sender {:?} isn't a cluster member",
                sender
            )))
        } else {
            Ok(())
        }
    }

    /// Verify that `conf_id` refers to the active configuration.
    fn verify_config(&self, conf_id: u64) -> Grpc<()> {
        if self.conf_id != conf_id {
            Err(Status::aborted("mismatched configuration id"))
        } else {
            Ok(())
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
