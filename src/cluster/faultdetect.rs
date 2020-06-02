// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Distributed fault detection.
use super::{
    cut::{self, Subscription},
    proto::{membership_client::MembershipClient, Ack, Edge, Endpoint},
    Cluster,
};
use futures::{
    future::{join, FutureExt},
    stream::{FuturesUnordered, StreamExt},
};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    select,
    time::{delay_for, timeout},
};

impl Cluster {
    /// Run the fault detector until the cluster is brought down.
    pub(crate) async fn detect_faults(self: Arc<Self>, mut cuts: Subscription) -> cut::Result {
        loop {
            select! {
                _ = self.spin_fd_probes() => {}
                cut = cuts.recv() => { cut?; }
            }
        }
    }

    /// Initialize a fault detection round and continuously probe all observed subjects. Edge
    /// failures are reported to the rest of the cluster if the configured number of successive
    /// faults is encountered.
    ///
    /// Resolves (and should be restarted) when the next view-change proposal is accepted.
    async fn spin_fd_probes(self: &Arc<Self>) {
        let (conf_id, mut subjects) = async {
            #[derive(Default)]
            struct Subject {
                rings: Vec<u64>,
                faults: usize,
            }

            let mut subjects: HashMap<_, Subject> = HashMap::with_capacity(self.cfg.k);
            let state = self.state.read().await;

            // we might have been assigned the same subject on multiple rings, so we dedupe
            // by subject and track which rings we're authoritative over.
            for (ring, subject) in (state.nodes.successors(&self.local_node()))
                .cloned()
                .enumerate()
            {
                subjects.entry(subject).or_default().rings.push(ring as u64);
            }

            (state.conf_id, subjects)
        }
        .await;

        loop {
            // start sending off probes, each of which times out after fd_timeout.
            let probes = (subjects.iter_mut())
                .map(|(e, s)| self.probe(e, &mut s.faults))
                .collect::<FuturesUnordered<_>>()
                .for_each(|_| async {});

            // wait for all probes to finish, and for fd_timeout to elapse. this caps the
            // rate at which subjects are probed to k per fd_timeout.
            let mut state = join(delay_for(self.cfg.fd_timeout), probes)
                .then(|_| self.state.write())
                .await;

            // if a there's been a view-change, subjects may have become invalidated.
            if state.conf_id != conf_id {
                break;
            }

            // subjects are marked as faulted if they failed fd_strikes successive probes.
            let faulted = (subjects.iter_mut())
                .filter(|(_, s)| s.faults >= self.cfg.fd_strikes)
                .flat_map(|(e, s)| {
                    s.faults = 0;
                    s.rings.iter().map(move |ring| Edge::down(e.clone(), *ring))
                });

            self.enqueue_edges(&mut *state, faulted);
        }
    }

    /// Probe a subject, modifying the successive `faults` counter appropriately upon success
    /// or failure.
    async fn probe(&self, subject: &Endpoint, faults: &mut usize) {
        let send_probe = timeout(self.cfg.fd_timeout, async {
            let e = self.resolve_endpoint(subject).ok()?;
            let mut c = MembershipClient::connect(e).await.ok()?;
            c.probe(Ack {}).await.ok()
        });

        match send_probe.await.ok().flatten() {
            Some(_) => *faults = 0,
            None => *faults += 1,
        }
    }
}
