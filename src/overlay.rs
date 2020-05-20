// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Batteries-included grpc service mesh.
#[cfg(feature = "simulation")]
use crate::simulation::Network;

use super::cluster::{
    cut::Subscription,
    partition::{self, Rejoin},
    Cluster, Config,
};

use failure::Fallible;
use futures::{
    future::{pending, FutureExt, TryFutureExt},
    stream::{FuturesUnordered, StreamExt},
};
use std::{
    error::Error,
    future::Future,
    net::SocketAddr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tonic::{
    body::BoxBody,
    codegen::{
        http::{HeaderMap, Request as HttpRequest, Response as HttpResponse},
        Service,
    },
    transport::{
        server::{Router, Unimplemented},
        Body, ClientTlsConfig, NamedService, Server, ServerTlsConfig,
    },
};
use tracing::Span;

/// Specifies observer/subject thresholds for the cut detector.
#[derive(Copy, Clone, Debug)]
pub struct CutDetectorConfig {
    /// Threshold of reports required to place a subject into unstable report mode.
    ///
    /// Must be non-zero, and less than or equal to `stable_threshold`.
    ///
    /// Defaults to 4.
    pub unstable_threshold: usize,

    /// Threshold of reports required to place a subject into stable report mode.
    ///
    /// Must be non-zero, and less than or equal to `subjects_per_observer`.
    ///
    /// Defaults to 9.
    pub stable_threshold: usize,

    /// Set the number of subjects for each observer.
    ///
    /// Must be non-zero.
    ///
    /// Defaults to 10.
    pub subjects_per_observer: usize,
}

impl Default for CutDetectorConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl CutDetectorConfig {
    /// Returns the default configuration.
    pub const fn new() -> Self {
        Self {
            unstable_threshold: 4,
            stable_threshold: 9,
            subjects_per_observer: 10,
        }
    }

    // TODO: add some sensible default configs for different scenarios
}

/// An unstarted member of a blip mesh network.
///
/// This is a wrapper over the grpc server/router in [tonic::transport], and provides a
/// pass-through implementation for most of their configuration methods.
///
/// Most interaction with the membership state of an executing member occurs via services
/// that implement the [MeshService] trait.
pub struct Mesh<St, R> {
    cfg: Config<St>,
    grpc: R,
    svcs: Vec<Box<dyn MeshInner>>,
}

impl Default for Mesh<Rejoin, Server> {
    fn default() -> Self {
        let cd = CutDetectorConfig::new();

        Self {
            cfg: Config {
                strategy: Default::default(),
                lh: (cd.unstable_threshold, cd.stable_threshold),
                k: cd.subjects_per_observer,
                seed: None,
                meta: Default::default(),
                server_tls: false,
                client_tls: None,
            },
            grpc: Server::builder(),
            svcs: Vec::new(),
        }
    }
}

impl Mesh<Rejoin, Server> {
    /// Create a new [Mesh].
    pub fn new() -> Self {
        Self::default()
    }
}

/// Methods for blip-specific membership protocol configuration.
impl<St, R> Mesh<St, R> {
    /// Configure the cut detector.
    ///
    /// # Panics
    /// Panics if any invariants (listed at [CutDetectorConfig]) are not upheld.
    pub fn cd_config(mut self, cfg: CutDetectorConfig) -> Self {
        assert_ne!(0, cfg.unstable_threshold);
        assert_ne!(0, cfg.stable_threshold);
        assert_ne!(0, cfg.subjects_per_observer);
        assert!(cfg.unstable_threshold <= cfg.stable_threshold);
        assert!(cfg.stable_threshold <= cfg.subjects_per_observer);

        self.cfg.lh.0 = cfg.unstable_threshold;
        self.cfg.lh.1 = cfg.stable_threshold;
        self.cfg.k = cfg.subjects_per_observer;
        self
    }

    /// Set a seed node to contact in order to join an existing network. If this is left
    /// unset (the default), a new mesh will be bootstrapped with the local node as the
    /// sole member.
    pub fn join_seed(mut self, addr: SocketAddr, use_tls: bool) -> Self {
        self.cfg.seed = Some((addr, use_tls).into());
        self
    }

    /// Add metadata to distribute to other members of the mesh.
    pub fn add_metadata<I: IntoIterator<Item = (String, Vec<u8>)>>(mut self, iter: I) -> Self {
        self.cfg.meta.extend(iter);
        self
    }

    /// Configure TLS for connections to other mesh members, as well as for any of the
    /// endpoints returned in [MeshService] membership updates.
    pub fn client_tls_config(mut self, tls_config: ClientTlsConfig) -> Self {
        self.cfg.client_tls = Some(Arc::new(tls_config));
        self
    }

    /// Sets the strategy with which to handle network partitions.
    ///
    /// See [partition] for possible options to use here.
    // TODO: actually write different strategies, or remove the parameterization
    pub fn part_strategy<_St: partition::Strategy>(self) -> Mesh<_St, R> {
        let Mesh { cfg, grpc, svcs } = self;
        let strategy = _St::default();

        #[rustfmt::skip]
        let Config { lh, k, seed, meta, server_tls, client_tls, .. } = cfg;
        #[rustfmt::skip]
        let cfg = Config { strategy, lh, k, seed, meta, server_tls, client_tls };

        Mesh { cfg, grpc, svcs }
    }

    /// Add a [MeshService] that doesn't necessarily implement [Service].
    ///
    /// This can be used to receive membership updates without exposing a grpc service to
    /// other mesh members.
    pub fn add_mesh_service<S: MeshService + 'static>(mut self, svc: S) -> Self {
        svc.add_metadata(&mut *self.cfg.meta);
        self.svcs.push(Box::new(svc));
        self
    }
}

/// Methods that pass-through to [tonic::transport].
impl<St: partition::Strategy> Mesh<St, Server> {
    /// Configure TLS for this server.
    pub fn server_tls_config(mut self, tls_config: ServerTlsConfig) -> Self {
        self.cfg.server_tls = true;
        self.grpc = self.grpc.tls_config(tls_config);
        self
    }

    /// Set the concurrency limit applied to on requests inbound per connection.
    pub fn concurrency_limit_per_connection(mut self, limit: usize) -> Self {
        self.grpc = self.grpc.concurrency_limit_per_connection(limit);
        self
    }

    /// Set a timeout for all request handlers.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.grpc.timeout(timeout);
        self
    }

    /// Sets the SETTINGS_INITIAL_WINDOW_SIZE option for HTTP2 stream-level flow control.
    ///
    /// Defaults to 65,535.
    pub fn initial_stream_window_size<S: Into<Option<u32>>>(mut self, sz: S) -> Self {
        self.grpc = self.grpc.initial_stream_window_size(sz);
        self
    }

    /// Sets the max connection-level flow control for HTTP2.
    ///
    /// Defaults to 65,535.
    pub fn initial_connection_window_size<S: Into<Option<u32>>>(mut self, sz: S) -> Self {
        self.grpc = self.grpc.initial_connection_window_size(sz);
        self
    }

    /// Sets the SETTINGS_MAX_CONCURRENT_STREAMS option for HTTP2 connections.
    ///
    /// Defaults to no limit (None).
    pub fn max_concurrent_streams<M: Into<Option<u32>>>(mut self, max: M) -> Self {
        self.grpc = self.grpc.max_concurrent_streams(max);
        self
    }

    /// Set whether TCP keepalive messages are enabled on accepted connections.
    ///
    /// If None is specified, keepalive is disabled, otherwise the duration specified
    /// will be the time to remain idle before sending TCP keepalive probes.
    ///
    /// Defaults to no keepalive (None).
    pub fn tcp_keepalive<D: Into<Option<Duration>>>(mut self, tcp_keepalive: D) -> Self {
        self.grpc = self.grpc.tcp_keepalive(tcp_keepalive.into());
        self
    }

    /// Set the value of TCP_NODELAY option for accepted connections.
    ///
    /// Defaults to enabled (true).
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.grpc = self.grpc.tcp_nodelay(enabled);
        self
    }

    /// Intercept inbound headers and add a [tracing::Span][Span] to each response future.
    pub fn trace_fn<F>(mut self, f: F) -> Self
    where F: Fn(&HeaderMap) -> Span + Send + Sync + 'static {
        self.grpc = self.grpc.trace_fn(f);
        self
    }

    /// Add a service `S` to the mesh. Any endpoints it exposes should use request paths
    /// namespaced by the [NamedService] implementation, so they can be routed alongside
    /// other services.
    pub fn add_service<S>(self, svc: S) -> Mesh<St, Router<S, Unimplemented>>
    where
        S: Service<HttpRequest<Body>, Response = HttpResponse<BoxBody>>
            + NamedService
            + MeshService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn Error + Send + Sync>> + Send,
    {
        #[rustfmt::skip]
        let Mesh { cfg, mut grpc, svcs } = self.add_mesh_service(svc.clone());
        let grpc = grpc.add_service(svc);

        Mesh { cfg, grpc, svcs }
    }

    /// Consume this [Mesh], creating a future that will run on a tokio executor.
    ///
    /// Resolves once the mesh has exited.
    #[cfg(feature = "simulation")]
    #[cfg_attr(docsrs, doc(cfg(feature = "simulation")))]
    pub async fn serve_simulated(self, net: Network, addr: SocketAddr) -> Fallible<()> {
        self.serve_simulated_shutdown(net, addr, pending()).await
    }

    /// Consume this [Mesh], creating a future that will run on a tokio executor.
    ///
    /// Shutdown will be initiated when `signal` resolves.
    ///
    /// Resolves once the mesh has exited.
    #[cfg(feature = "simulation")]
    #[cfg_attr(docsrs, doc(cfg(feature = "simulation")))]
    pub async fn serve_simulated_shutdown<F>(
        mut self,
        net: Network,
        addr: SocketAddr,
        signal: F,
    ) -> Fallible<()>
    where
        F: Future<Output = ()> + Send,
    {
        let conns = net.bind(addr).await?;

        let mut cluster = Cluster::new(self.cfg, addr);
        cluster.use_simulated_net(net);
        let cluster = Arc::new(cluster);

        let f_cuts = cluster.subscribe();
        let w_cuts = cluster.subscribe();

        let svcs: FuturesUnordered<_> = (self.svcs)
            .into_iter()
            .map(|s| s.accept(cluster.subscribe()))
            .collect();

        tokio::select! {
            r = svcs.for_each(|_| async {}).then(|_| pending()) => r,
            r = self.grpc
                    .add_service(Arc::clone(&cluster).into_service())
                    .serve_with_incoming_shutdown(conns, signal)
                    .err_into() => r,
            r = Arc::clone(&cluster).detect_faults(f_cuts) => r,
            r = St::handle_parts(Arc::clone(&cluster), w_cuts) => r,
        }
    }

    /// Consume this [Mesh], creating a future that will run on a tokio executor.
    ///
    /// Resolves once the mesh has exited.
    pub async fn serve(self, addr: SocketAddr) -> Fallible<()> {
        self.serve_with_shutdown(addr, pending()).await
    }

    /// Consume this [Mesh], creating a future that will run on a tokio executor.
    ///
    /// Shutdown will be initiated when `signal` resolves.
    ///
    /// Resolves once the mesh has exited.
    pub async fn serve_with_shutdown<F>(mut self, addr: SocketAddr, signal: F) -> Fallible<()>
    where F: Future<Output = ()> + Send {
        let cluster = Arc::new(Cluster::new(self.cfg, addr));

        let f_cuts = cluster.subscribe();
        let w_cuts = cluster.subscribe();

        let svcs: FuturesUnordered<_> = (self.svcs)
            .into_iter()
            .map(|s| s.accept(cluster.subscribe()))
            .collect();

        tokio::select! {
            r = svcs.for_each(|_| async {}).then(|_| pending()) => r,
            r = self.grpc
                    .add_service(Arc::clone(&cluster).into_service())
                    .serve_with_shutdown(addr, signal)
                    .err_into() => r,
            r = Arc::clone(&cluster).detect_faults(f_cuts) => r,
            r = St::handle_parts(Arc::clone(&cluster), w_cuts) => r,
        }
    }
}

/// These methods are duplicated here because it isn't possible to be generic over [Server]
/// and [Router] in the same impl block. The logic is equivalent to the counterparts above.
#[doc(hidden)]
impl<St: partition::Strategy, A, B> Mesh<St, Router<A, B>>
where
    A: Service<HttpRequest<Body>, Response = HttpResponse<BoxBody>> + Clone + Send + 'static,
    A::Future: Send + 'static,
    A::Error: Into<Box<dyn Error + Send + Sync>> + Send,
    B: Service<HttpRequest<Body>, Response = HttpResponse<BoxBody>> + Clone + Send + 'static,
    B::Future: Send + 'static,
    B::Error: Into<Box<dyn Error + Send + Sync>> + Send,
{
    pub fn add_service<S>(self, svc: S) -> Mesh<St, Router<S, impl Service<HttpRequest<Body>>>>
    where
        S: Service<HttpRequest<Body>, Response = HttpResponse<BoxBody>>
            + NamedService
            + MeshService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn Error + Send + Sync>> + Send,
    {
        let Mesh { cfg, grpc, svcs } = self.add_mesh_service(svc.clone());
        let grpc = grpc.add_service(svc);

        Mesh { cfg, grpc, svcs }
    }

    #[cfg(feature = "simulation")]
    #[cfg_attr(docsrs, doc(cfg(feature = "simulation")))]
    pub async fn serve_simulated(self, net: Network, addr: SocketAddr) -> Fallible<()> {
        self.serve_simulated_shutdown(net, addr, pending()).await
    }

    #[cfg(feature = "simulation")]
    #[cfg_attr(docsrs, doc(cfg(feature = "simulation")))]
    pub async fn serve_simulated_shutdown<F>(
        self,
        net: Network,
        addr: SocketAddr,
        signal: F,
    ) -> Fallible<()>
    where
        F: Future<Output = ()> + Send,
    {
        let conns = net.bind(addr).await?;

        let mut cluster = Cluster::new(self.cfg, addr);
        cluster.use_simulated_net(net);
        let cluster = Arc::new(cluster);

        let f_cuts = cluster.subscribe();
        let w_cuts = cluster.subscribe();

        let svcs: FuturesUnordered<_> = (self.svcs)
            .into_iter()
            .map(|s| s.accept(cluster.subscribe()))
            .collect();

        tokio::select! {
            r = svcs.for_each(|_| async {}).then(|_| pending()) => r,
            r = self.grpc
                    .add_service(Arc::clone(&cluster).into_service())
                    .serve_with_incoming_shutdown(conns, signal)
                    .err_into() => r,
            r = Arc::clone(&cluster).detect_faults(f_cuts) => r,
            r = St::handle_parts(Arc::clone(&cluster), w_cuts) => r,
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> Fallible<()> {
        self.serve_with_shutdown(addr, pending()).await
    }

    pub async fn serve_with_shutdown<F>(self, addr: SocketAddr, signal: F) -> Fallible<()>
    where F: Future<Output = ()> + Send {
        let cluster = Arc::new(Cluster::new(self.cfg, addr));

        let f_cuts = cluster.subscribe();
        let w_cuts = cluster.subscribe();

        let svcs: FuturesUnordered<_> = (self.svcs)
            .into_iter()
            .map(|s| s.accept(cluster.subscribe()))
            .collect();

        tokio::select! {
            r = svcs.for_each(|_| async {}).then(|_| pending()) => r,
            r = self.grpc
                    .add_service(Arc::clone(&cluster).into_service())
                    .serve_with_shutdown(addr, signal)
                    .err_into() => r,
            r = Arc::clone(&cluster).detect_faults(f_cuts) => r,
            r = St::handle_parts(Arc::clone(&cluster), w_cuts) => r,
        }
    }
}

/// A wrapper around an arbitrary [Service] that will implement [MeshService] for free.
///
/// This can be used to serve a service that isn't mesh-aware.
#[derive(Copy, Clone, Debug)]
pub struct GrpcService<S> {
    svc: S,
}

impl<S: Service<HttpRequest<Body>>> GrpcService<S> {
    /// Wrap a type that implements [Service].
    pub fn new(svc: S) -> Self {
        Self { svc }
    }
}

impl<S: NamedService> NamedService for GrpcService<S> {
    const NAME: &'static str = S::NAME;
}

impl<Request, S: Service<Request>> Service<Request> for GrpcService<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.svc.poll_ready(ctx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.svc.call(req)
    }
}

#[crate::async_trait]
impl<S: Send> MeshService for GrpcService<S> {
    async fn accept(self, _: Subscription) {}
}

/// A trait that allows individual services access to any accepted membership view-change
/// proposals.
///
/// # Examples
/// ```
/// use blip::{MeshService, Subscription};
///
/// struct MySvc;
///
/// #[blip::async_trait]
/// impl MeshService for MySvc {
///     fn add_metadata<K: Extend<(String, Vec<u8>)>>(&self, keys: &mut K) {
///         keys.extend(vec![("key".to_owned(), b"value".to_vec())]);
///     }
///
///     async fn accept(self, mut cuts: Subscription) {
///         while let Ok(cut) = cuts.recv().await {
///             // handle membership change
///             let _ = cut.members();
///             let _ = cut.joined();
///             let _ = cut.kicked();
///         }
///     }
/// }
/// ```
#[crate::async_trait]
pub trait MeshService: Send {
    /// Add service-specific metadata to distribute to other members of the mesh.
    fn add_metadata<K: Extend<(String, Vec<u8>)>>(&self, _keys: &mut K) {}

    /// Receive accepted view-change proposals.
    ///
    /// This method is called once for every [MeshService] added to a [Mesh], and will be
    /// polled for the entire period the mesh remains online. Resolving early is fine and
    /// does not constitute an error.
    ///
    /// Blocking (of the non-async variety) within this future should _never_ happen, and
    /// will starve the executor if it does.
    ///
    /// If the mesh exits (for any reason), this future will be dropped if it has not yet
    /// resolved (which may occur at any yield point).
    async fn accept(self, cuts: Subscription);
}

/// An adjacent trait to [MeshService]; this should only be implemented by the blanket
/// impl below. It exists for its ability to be made into a trait object.
#[crate::async_trait]
trait MeshInner: Send {
    async fn accept(self: Box<Self>, cuts: Subscription);
}

#[crate::async_trait]
impl<S: MeshService + 'static> MeshInner for S {
    async fn accept(self: Box<Self>, cuts: Subscription) {
        (*self).accept(cuts).await
    }
}
