// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Shared code referred to by multiple test modules.
use blip::{MeshService, MultiNodeCut, Subscription};
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU32, Ordering::Relaxed},
};
use tokio::sync::mpsc;

// A quick NOTE about addressing in integration tests: each test should subnet a unique /20
// from the 127.128/9 loopback block to avoid collisions with other tests or os services.
//
// The port should always be 10000.

pub fn subnet() -> u32 {
    static SUBNET: AtomicU32 = AtomicU32::new(0);
    let s = SUBNET.fetch_add(1, Relaxed);
    assert!(s <= 2 ^ 11);
    s
}

pub fn addr_in(subnet: u32, host: u32) -> SocketAddr {
    let mut addr = (subnet & 0x1fff) << 12; // 11 bits of subnet
    addr |= host & 0xfff; // 12 bits of host
    addr |= 0x7f800000; // and the loopback stuff

    (addr.to_be_bytes(), 10000).into()
}

pub fn cfg_handle() -> (CfgHandle, CfgService) {
    let (tx, rx) = mpsc::channel(32);

    let h = CfgHandle { rx };
    let s = CfgService { tx };

    (h, s)
}

pub struct CfgService {
    tx: mpsc::Sender<MultiNodeCut>,
}

#[blip::async_trait]
impl MeshService for CfgService {
    async fn accept(mut self: Box<Self>, mut cuts: Subscription) {
        while let Ok(cut) = cuts.recv().await {
            self.tx.send(cut).await.unwrap();
        }
    }
}

pub struct CfgHandle {
    rx: mpsc::Receiver<MultiNodeCut>,
}

impl CfgHandle {
    /// Blocks until a view-change proposal with `n` peers is accepted.
    pub async fn cfg_change(&mut self, n: usize) -> MultiNodeCut {
        while let Some(cut) = self.rx.recv().await {
            if cut.members().len() == n {
                return cut;
            }
        }
        panic!("cfg_handle sender closed!");
    }
}
