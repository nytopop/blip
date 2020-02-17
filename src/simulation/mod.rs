// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Fault-simulated networks for writing bulletproof distributed protocols.

mod add_origin;
mod connection;
mod layer;
mod reconnect;
mod socket;

use connection::Connector;
pub use connection::{ChannelStub, Connection};
use socket::Socket;

use failure::{format_err, Fallible};
use http::uri::Uri;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    convert::TryInto,
    error::Error,
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex};

/// A simulated network.
// TODO: latency
// TODO: partitions
#[derive(Clone, Default)]
pub struct Network {
    inner: Arc<Mutex<Inner>>,
}

impl Network {
    /// Block new connections to `dst`.
    pub async fn block(&self, dst: SocketAddr) {
        let mut inner = self.inner.lock().await;
        inner.blocked.insert(dst);
    }

    /// Allow new connections to `dst`.
    pub async fn allow(&self, dst: SocketAddr) {
        let mut inner = self.inner.lock().await;
        inner.blocked.remove(&dst);
    }

    /// Bind to a listening socket.
    // TODO: cleanup on drop
    pub async fn bind(&self, addr: SocketAddr) -> Fallible<mpsc::Receiver<Fallible<Socket>>> {
        let mut inner = self.inner.lock().await;
        let (tx, rx) = mpsc::channel(8);

        if let Entry::Vacant(v) = inner.listeners.entry(addr) {
            v.insert(tx);
            Ok(rx)
        } else {
            Err(format_err!("addr in use: {:?}", addr))
        }
    }

    /// Connect to a listening socket.
    pub async fn connect<U>(&self, uri: U) -> Fallible<Connection>
    where
        U: TryInto<Uri>,
        U::Error: Error + Send + Sync + 'static,
    {
        let uri = uri.try_into()?;

        let cnt = Connector {
            inner: Arc::clone(&self.inner),
        };

        Ok(Connection::new(cnt, uri).await?)
    }
}

#[derive(Default)]
struct Inner {
    listeners: HashMap<SocketAddr, mpsc::Sender<Fallible<Socket>>>,
    blocked: HashSet<SocketAddr>,
}

impl Inner {
    async fn connect(&mut self, addr: &SocketAddr) -> Fallible<Socket> {
        if self.blocked.contains(addr) {
            return Err(format_err!("blocked by simulation"));
        }

        let (src, dst) = Socket::new();

        match self.listeners.get_mut(&addr) {
            Some(tx) => match tx.send(Ok(dst)).await {
                Ok(()) => Ok(src),
                Err(_) => Err(format_err!("broken pipe!")),
            },

            None => Err(format_err!("failed to connect!")),
        }
    }
}
