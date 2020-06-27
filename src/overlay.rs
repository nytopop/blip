// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Batteries-included grpc service mesh.
use super::cluster::{
    cut::{Closed, Subscription},
    Cluster, Config,
};

use futures::{
    future::{pending, FutureExt, TryFutureExt},
    stream::{FuturesUnordered, StreamExt},
};
use std::{error, future::Future, net::SocketAddr, result, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::select;
use tonic::{
    body::BoxBody,
    codegen::{
        http::{HeaderMap, Request as HttpRequest, Response as HttpResponse},
        Service,
    },
    transport::{
        self,
        server::{Router, Unimplemented},
        Body, ClientTlsConfig, NamedService, Server, ServerTlsConfig,
    },
};
use tracing::Span;

/// A `Result<(), Error>`.
pub type Result = result::Result<(), Error>;

/// An error which causes a mesh to exit.
#[derive(Debug, Error)]
pub enum Error {
    /// An error encountered in the grpc transport.
    #[error("mesh: {}", .0)]
    Transport(#[from] transport::Error),

    /// An error encountered when an internal task exits unexpectedly.
    #[error("mesh: task closed")]
    Closed(#[from] Closed),
}

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
pub struct Mesh<R> {
    cfg: Config,
    grpc: R,
    svcs: Vec<Box<dyn MeshService>>,
}

impl Default for Mesh<Server> {
    fn default() -> Self {
        let cd = CutDetectorConfig::new();

        Self {
            cfg: Config {
                lh: (cd.unstable_threshold, cd.stable_threshold),
                k: cd.subjects_per_observer,
                seed: None,
                meta: Default::default(),
                server_tls: false,
                client_tls: None,
                fd_timeout: Duration::from_secs(2),
                fd_strikes: 3,
            },
            grpc: Server::builder(),
            svcs: Vec::new(),
        }
    }
}

impl Mesh<Server> {
    /// Create a new [Mesh] with the default configuration.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new [Mesh] with reduced timeouts and faster fault detection, at the expense
    /// of significantly more probe traffic.
    ///
    /// This should generally be avoided except in test code.
    #[inline]
    pub fn low_latency() -> Self {
        Self::default()
            .fault_timeout(Duration::from_millis(150))
            .fault_strikes(2)
    }
}

/// Methods for blip-specific membership protocol configuration.
impl<R> Mesh<R> {
    /// Configure the cut detector. All members of the mesh must have the same configuration.
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

    /// Configure TLS for outgoing connections to mesh members that are expecting TLS.
    ///
    /// This will also be exposed in the [MultiNodeCut][cut]s received from [Subscription]s.
    ///
    /// [cut]: super::MultiNodeCut
    pub fn client_tls_config(mut self, tls_config: ClientTlsConfig) -> Self {
        self.cfg.client_tls = Some(Arc::new(tls_config));
        self
    }

    /// Set a timeout for the fault detector. If a subject fails to respond to a probe within
    /// this amount of time (for `fault_strikes` successive attempts), it will be marked as
    /// faulty by the observer.
    ///
    /// The rate at which probe messages are sent is also determined by this value. A probe
    /// is sent to all subjects every `timeout`. Extrapolated to the whole mesh, every node
    /// sends and receives `subjects_per_observer` probes every `timeout`.
    ///
    /// Defaults to 2 seconds.
    pub fn fault_timeout(mut self, timeout: Duration) -> Self {
        self.cfg.fd_timeout = timeout;
        self
    }

    /// Set the number of successive fault detection probe attempts that must fail before a
    /// subject is marked as faulty.
    ///
    /// Defaults to 3.
    ///
    /// # Panics
    /// Panics if `strikes == 0`.
    pub fn fault_strikes(mut self, strikes: usize) -> Self {
        assert!(strikes != 0);
        self.cfg.fd_strikes = strikes;
        self
    }

    /// Add a [MeshService] that doesn't necessarily implement [ExposedService].
    ///
    /// This can be used to receive membership updates without exposing a grpc service to
    /// other mesh members.
    pub fn add_mesh_service<S: MeshService + 'static>(mut self, svc: S) -> Self {
        self.svcs.push(Box::new(svc));
        self
    }
}

/// Methods that pass-through to [tonic::transport].
impl Mesh<Server> {
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
    pub fn add_service<S>(self, svc: S) -> Mesh<Router<S::Service, Unimplemented>>
    where
        S: ExposedService + 'static,
        <<S as ExposedService>::Service as Service<HttpRequest<Body>>>::Future: Send + 'static,
        <<S as ExposedService>::Service as Service<HttpRequest<Body>>>::Error:
            Into<Box<dyn error::Error + Send + Sync>> + Send,
    {
        #[rustfmt::skip]
        let Mesh { mut cfg, mut grpc, svcs } = self.add_mesh_service(svc.clone());
        svc.add_metadata(&mut *cfg.meta);
        let grpc = grpc.add_service(svc.into_service());

        Mesh { cfg, grpc, svcs }
    }

    /// Consume this [Mesh], creating a future that will run on a tokio executor.
    ///
    /// Resolves once the mesh has exited.
    #[inline]
    pub async fn serve(self, addr: SocketAddr) -> Result {
        self.serve_with_shutdown(addr, pending()).await
    }

    /// Consume this [Mesh], creating a future that will run on a tokio executor.
    ///
    /// Shutdown will be initiated when `signal` resolves.
    ///
    /// Resolves once the mesh has exited.
    pub async fn serve_with_shutdown<F>(mut self, addr: SocketAddr, signal: F) -> Result
    where F: Future<Output = ()> + Send {
        let cluster = Arc::new(Cluster::new(self.cfg, addr));

        select! {
            r = self.svcs.into_iter()
                    .map(|s| s.accept(cluster.subscribe()))
                    .collect::<FuturesUnordered<_>>()
                    .for_each(|_| async {})
                    .then(|_| pending()) => r,

            r = Arc::clone(&cluster)
                    .detect_faults(cluster.subscribe())
                    .err_into() => r,

            r = Arc::clone(&cluster)
                    .handle_parts(cluster.subscribe())
                    .err_into() => r,

            r = self.grpc
                    .add_service(cluster.into_service())
                    .serve_with_shutdown(addr, signal)
                    .err_into() => r,
        }
    }
}

/// These methods are duplicated here because it isn't possible to be generic over [Server]
/// and [Router] in the same impl block. The logic is equivalent to the counterparts above.
#[doc(hidden)]
impl<A, B> Mesh<Router<A, B>>
where
    A: Service<HttpRequest<Body>, Response = HttpResponse<BoxBody>> + Clone + Send + 'static,
    A::Future: Send + 'static,
    A::Error: Into<Box<dyn error::Error + Send + Sync>> + Send,
    B: Service<HttpRequest<Body>, Response = HttpResponse<BoxBody>> + Clone + Send + 'static,
    B::Future: Send + 'static,
    B::Error: Into<Box<dyn error::Error + Send + Sync>> + Send,
{
    pub fn add_service<S>(self, svc: S) -> Mesh<Router<S::Service, impl Service<HttpRequest<Body>>>>
    where
        S: ExposedService + 'static,
        <<S as ExposedService>::Service as Service<HttpRequest<Body>>>::Future: Send + 'static,
        <<S as ExposedService>::Service as Service<HttpRequest<Body>>>::Error:
            Into<Box<dyn error::Error + Send + Sync>> + Send,
    {
        #[rustfmt::skip]
        let Mesh { mut cfg, grpc, svcs } = self.add_mesh_service(svc.clone());
        svc.add_metadata(&mut *cfg.meta);
        let grpc = grpc.add_service(svc.into_service());

        Mesh { cfg, grpc, svcs }
    }

    #[inline]
    pub async fn serve(self, addr: SocketAddr) -> Result {
        self.serve_with_shutdown(addr, pending()).await
    }

    pub async fn serve_with_shutdown<F>(self, addr: SocketAddr, signal: F) -> Result
    where F: Future<Output = ()> + Send {
        let cluster = Arc::new(Cluster::new(self.cfg, addr));

        select! {
            r = self.svcs.into_iter()
                    .map(|s| s.accept(cluster.subscribe()))
                    .collect::<FuturesUnordered<_>>()
                    .for_each(|_| async {})
                    .then(|_| pending()) => r,

            r = Arc::clone(&cluster)
                    .detect_faults(cluster.subscribe())
                    .err_into() => r,

            r = Arc::clone(&cluster)
                    .handle_parts(cluster.subscribe())
                    .err_into() => r,

            r = self.grpc
                    .add_service(cluster.into_service())
                    .serve_with_shutdown(addr, signal)
                    .err_into() => r,
        }
    }
}

/// A wrapper around an arbitrary [Service] that will implement [ExposedService] with an empty
/// [MeshService] implementation.
///
/// This can be used to serve a service that isn't mesh-aware.
#[derive(Copy, Clone, Debug)]
pub struct GrpcService<S> {
    svc: S,
}

impl<S> GrpcService<S>
where
    S: Service<HttpRequest<Body>, Response = HttpResponse<BoxBody>>
        + NamedService
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn error::Error + Send + Sync>> + Send,
{
    /// Wrap a type that implements [Service].
    pub fn new(svc: S) -> Self {
        Self { svc }
    }
}

#[crate::async_trait]
impl<S: Send> MeshService for GrpcService<S> {
    #[inline]
    async fn accept(self: Box<Self>, _: Subscription) {}
}

impl<S> ExposedService for GrpcService<S>
where S: Service<HttpRequest<Body>, Response = HttpResponse<BoxBody>>
        + NamedService
        + Clone
        + Send
        + 'static
{
    type Service = S;

    #[inline]
    fn into_service(self) -> Self::Service {
        self.svc
    }
}

/// A service that has access to accepted membership view-change proposals.
///
/// # Examples
/// ```
/// use blip::{MeshService, Subscription};
///
/// struct MySvc;
///
/// #[blip::async_trait]
/// impl MeshService for MySvc {
///     async fn accept(self: Box<Self>, mut cuts: Subscription) {
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
    async fn accept(self: Box<Self>, cuts: Subscription);
}

/// A [MeshService] that can be converted into a [Service] to be served over grpc.
pub trait ExposedService: MeshService + Clone {
    /// Add metadata to distribute to other members of the mesh.
    fn add_metadata<K: Extend<(String, Vec<u8>)>>(&self, _keys: &mut K) {}

    /// The service implementation.
    type Service: Service<HttpRequest<Body>, Response = HttpResponse<BoxBody>>
        + NamedService
        + Clone
        + Send
        + 'static;

    /// Convert self into a [Service].
    fn into_service(self) -> Self::Service;
}
