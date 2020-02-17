// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Protocol buffers definitions used in blip's membership protocol.
use failure::Fail;
use rand::random;
use std::{
    cmp,
    collections::HashMap,
    convert::{TryFrom, TryInto},
    hash::{Hash, Hasher},
    net::{IpAddr, SocketAddr},
    ops::{Deref, DerefMut},
};
use tonic::transport;

macro_rules! derive_cmp_with {
    ($type:ty, $access:ident => $get:expr) => {
        impl Eq for $type {}

        impl PartialOrd for $type {
            fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
                fn access<'a>(e: &'a $type) -> impl PartialOrd + 'a {
                    let $access = e;
                    $get
                }

                let x = access(self);
                let y = access(other);
                (&x).partial_cmp(&y)
            }
        }

        impl Ord for $type {
            fn cmp(&self, other: &Self) -> cmp::Ordering {
                self.partial_cmp(&other).unwrap()
            }
        }

        #[allow(clippy::derive_hash_xor_eq)]
        impl Hash for $type {
            fn hash<H: Hasher>(&self, state: &mut H) {
                let $access = self;
                let c = $get;
                c.hash(state);
            }
        }
    };
}

tonic::include_proto!("blip");

derive_cmp_with!(Endpoint, e => (&e.host, e.port, e.tls));
derive_cmp_with!(NodeId, id => u128::from(id));
derive_cmp_with!(Rank, r => (r.round, r.node_idx));

#[derive(Debug, failure_derive::Fail)]
pub enum EndpointError {
    #[fail(display = "invalid uri: {:?}", _0)]
    InvalidUri(http::uri::InvalidUri),
    #[fail(display = "invalid socketaddr: {:?}", _0)]
    InvalidSocketAddr(SocketAddrError),
}

impl TryFrom<&Endpoint> for transport::Endpoint {
    type Error = failure::Compat<EndpointError>;

    fn try_from(e: &Endpoint) -> Result<Self, Self::Error> {
        let addr: SocketAddr = e
            .try_into()
            .map_err(EndpointError::InvalidSocketAddr)
            .map_err(Fail::compat)?;

        let scheme = if e.tls { "https" } else { "http" };

        format!("{}://{}", scheme, addr)
            .try_into()
            .map_err(EndpointError::InvalidUri)
            .map_err(Fail::compat)
    }
}

impl TryFrom<&Endpoint> for http::Uri {
    type Error = failure::Compat<EndpointError>;

    fn try_from(e: &Endpoint) -> Result<Self, Self::Error> {
        let addr: SocketAddr = e
            .try_into()
            .map_err(EndpointError::InvalidSocketAddr)
            .map_err(Fail::compat)?;

        let scheme = if e.tls { "https" } else { "http" };

        format!("{}://{}", scheme, addr)
            .try_into()
            .map_err(EndpointError::InvalidUri)
            .map_err(Fail::compat)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, failure_derive::Fail)]
pub enum SocketAddrError {
    #[fail(display = "invalid host len: {}", _0)]
    InvalidLen(usize),
    #[fail(display = "invalid port: {}", _0)]
    InvalidPort(u32),
}

impl TryFrom<&Endpoint> for SocketAddr {
    type Error = SocketAddrError;

    fn try_from(Endpoint { host, port, .. }: &Endpoint) -> Result<Self, Self::Error> {
        if *port > std::u16::MAX as u32 {
            Err(SocketAddrError::InvalidPort(*port))
        } else if host.len() == 4 {
            Ok(SocketAddr::new(
                IpAddr::from(<[u8; 4]>::try_from(host.as_slice()).unwrap()),
                *port as u16,
            ))
        } else if host.len() == 16 {
            Ok(SocketAddr::new(
                IpAddr::from(<[u8; 16]>::try_from(host.as_slice()).unwrap()),
                *port as u16,
            ))
        } else {
            Err(SocketAddrError::InvalidLen(host.len()))
        }
    }
}

impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        let host = match addr {
            SocketAddr::V4(s) => s.ip().octets().to_vec(),
            SocketAddr::V6(s) => s.ip().octets().to_vec(),
        };

        Self {
            host,
            port: addr.port() as u32,
            tls: false,
        }
    }
}

impl From<(SocketAddr, bool)> for Endpoint {
    fn from((addr, tls): (SocketAddr, bool)) -> Self {
        Self::from(addr).tls(tls)
    }
}

impl Endpoint {
    pub const fn tls(mut self, tls: bool) -> Self {
        self.tls = tls;
        self
    }
}

impl From<&NodeId> for u128 {
    fn from(id: &NodeId) -> Self {
        ((id.high as u128) << 64) | id.low as u128
    }
}

impl From<u128> for NodeId {
    fn from(x: u128) -> Self {
        Self {
            high: (x >> 64) as u64,
            low: ((x << 64) >> 64) as u64,
        }
    }
}

impl NodeId {
    pub fn generate() -> Self {
        random::<u128>().into()
    }
}

impl Deref for Metadata {
    type Target = HashMap<String, Vec<u8>>;

    fn deref(&self) -> &Self::Target {
        &self.keys
    }
}

impl DerefMut for Metadata {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.keys
    }
}

impl Edge {
    pub const fn down(node: Endpoint, ring: u64) -> Self {
        Self {
            node,
            ring,
            join: None,
        }
    }
}

impl Rank {
    pub const fn new(round: u32, node_idx: u64) -> Self {
        Self { round, node_idx }
    }

    pub const fn zero() -> Self {
        Self::new(0, 0)
    }

    pub const fn fast_round() -> Self {
        Self::new(1, 1)
    }
}
