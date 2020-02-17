// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use super::{
    add_origin::AddOrigin, layer::ServiceBuilderExt, reconnect::Reconnect, socket::Socket, Inner,
};

use failure::{format_err, Fallible};
use futures::future::{FutureExt, TryFutureExt};
use http::Uri;
use hyper::client::{
    conn::Builder, connect::Connection as HyperConnection, service::Connect as HyperConnect,
};
use std::{
    error::Error,
    fmt,
    future::Future,
    net::ToSocketAddrs,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
};
use tonic::{body::BoxBody, client::GrpcService, transport::Channel};
use tower::{layer::Layer, util::BoxService, ServiceBuilder};
use tower_load::Load;
use tower_service::Service;

pub struct Connector {
    pub(super) inner: Arc<Mutex<Inner>>,
}

impl Service<Uri> for Connector {
    type Error = failure::Error;

    type Response = Socket;

    type Future = Pin<Box<dyn Future<Output = Fallible<Socket>> + Send + 'static>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Fallible<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let inner = self.inner.clone();

        Box::pin(async move {
            let authority = req
                .authority()
                .ok_or_else(|| format_err!("missing authority"))?
                .as_str();

            let addr = authority
                .to_socket_addrs()?
                .next()
                .ok_or_else(|| format_err!("failed to parse addr"))?;

            let mut inner = inner.lock().await;

            inner.connect(&addr).await
        })
    }
}

pub type Request = http::Request<BoxBody>;

pub type Response = http::Response<hyper::Body>;

/// A simulated connection.
pub struct Connection {
    inner: BoxService<Request, Response, failure::Error>,
}

impl Connection {
    pub(crate) async fn new<C>(connector: C, uri: Uri) -> Fallible<Self>
    where
        C: Service<Uri> + Send + 'static,
        C::Error: Into<Box<dyn Error + Send + Sync>> + Send,
        C::Future: Unpin + Send,
        C::Response: AsyncRead + AsyncWrite + HyperConnection + Unpin + Send + 'static,
    {
        let mut settings = Builder::new();

        (settings.http2_initial_stream_window_size(65_535))
            .http2_initial_connection_window_size(65_535)
            .http2_only(true);

        let _uri = uri.clone();
        let stack = ServiceBuilder::new()
            .layer_fn(move |s| AddOrigin::new(s, _uri.clone()))
            // NOTE: no (endpoint) timeouts or rate/concurrency limit
            .into_inner();

        let mut connector = HyperConnect::new(connector, settings);
        let initial_conn = connector.call(uri.clone()).await?;
        let conn = Reconnect::new(initial_conn, connector, uri);

        Ok(Self {
            inner: BoxService::new(stack.layer(conn)),
        })
    }
}

impl Service<Request> for Connection {
    type Response = Response;

    type Error = failure::Error;

    type Future = Pin<Box<dyn Future<Output = Fallible<Response>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Fallible<()>> {
        Service::poll_ready(&mut self.inner, cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        Service::call(&mut self.inner, req)
    }
}

impl Load for Connection {
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        0
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

/// A grpc channel that may or may not be simulated.
pub enum ChannelStub {
    /// A simulated channel.
    Simulated(Connection),
    /// A native channel.
    Native(Channel),
}

impl Service<Request> for ChannelStub {
    type Response = Response;

    type Error = failure::Error;

    type Future = Pin<Box<dyn Future<Output = Fallible<Response>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Fallible<()>> {
        use ChannelStub::*;

        match self {
            Simulated(c) => Service::poll_ready(c, cx),
            Native(c) => GrpcService::poll_ready(c, cx).map_err(|e| e.into()),
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        use ChannelStub::*;

        match self {
            Simulated(c) => Service::call(c, req),
            Native(c) => GrpcService::call(c, req).map_err(|e| e.into()).boxed(),
        }
    }
}
