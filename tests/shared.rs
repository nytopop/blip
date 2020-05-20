// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Shared code referred to by multiple test modules.
use blip::{MeshService, MultiNodeCut, Subscription};
use std::sync::Arc;
use tokio::{
    sync::Mutex,
    time::{delay_for, Duration},
};

#[derive(Clone, Default)]
pub struct CfgHandle {
    cfg: Arc<Mutex<Option<MultiNodeCut>>>,
}

#[blip::async_trait]
impl MeshService for CfgHandle {
    async fn accept(self, mut cuts: Subscription) {
        while let Ok(cut) = cuts.recv().await {
            *self.cfg.lock().await = Some(cut);
        }
    }
}

impl CfgHandle {
    /// Blocks until a view-change proposal with `n` peers is accepted.
    pub async fn cfg_change(&self, n: usize) -> MultiNodeCut {
        let mut wait = Duration::from_millis(1);

        let init_cfg = (self.cfg.lock().await.as_ref())
            .map(|cfg| cfg.conf_id())
            .unwrap_or(0);

        loop {
            if let Some(cfg) = self.cfg.lock().await.as_ref() {
                if cfg.conf_id() != init_cfg && cfg.members().len() == n {
                    break cfg.clone();
                }
            }

            delay_for(wait).await;
            wait *= 2;
        }
    }
}
