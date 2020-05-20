// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Shared code referred to by multiple test modules.
use blip::{MeshService, MultiNodeCut, Subscription};
use tokio::sync::mpsc;

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
    async fn accept(mut self, mut cuts: Subscription) {
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
