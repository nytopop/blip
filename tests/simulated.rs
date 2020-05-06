// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
#![cfg(feature = "simulation")]

use async_ctx::Context;
use blip::{simulation::Network, Mesh, MeshService, MultiNodeCut, Subscription};
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::{
    task,
    time::{timeout, Duration},
};

const JIFFY: Duration = Duration::from_millis(25);

struct Svc<P>(P);

#[blip::async_trait]
impl<P: FnMut(MultiNodeCut) + Send> MeshService for Svc<P> {
    async fn accept(mut self, mut cuts: Subscription) {
        while let Ok(cut) = cuts.recv().await {
            self.0(cut);
        }
    }
}

fn addr<S: ToSocketAddrs>(addr: S) -> SocketAddr {
    addr.to_socket_addrs().unwrap().next().unwrap()
}

#[tokio::test]
async fn single_node_can_bootstrap() {
    let ctx = Context::default();
    let child = ctx.child();
    let net = Network::default();
    let addr = addr("123.123.123.123:100");

    let fut = Mesh::default()
        .add_mesh_service(Svc(move |_| ctx.complete()))
        .serve_simulated_shutdown(net, addr, child);

    timeout(JIFFY, fut).await.unwrap().unwrap()
}

const PER_MEMBER: Duration = Duration::from_millis(400);

// TODO: assert that each node ends up with the same config
async fn join_protocol_with(group_size: usize) {
    let net = Network::default();

    let ctx = Context::default();
    let ctxa = ctx.child();
    let fina = timeout(PER_MEMBER * group_size as u32, ctxa.clone());
    let ctxb = ctx.child();
    let _ctxb = ctxb.clone();

    let mut addrs = (1u16..).map(|port| addr(("1.1.1.1", port)));
    let seed = addrs.next().unwrap();

    let mut handles = vec![];
    let fut = Mesh::default()
        .add_mesh_service(Svc(move |cut: MultiNodeCut| {
            if cut.skipped() != 0 {
                panic!("skipped {}", cut.skipped());
            }

            match cut.members().len() {
                n if n == 1 + group_size => ctxa.complete(),
                n if n == 1 + (group_size * 2) => ctxb.complete(),
                _ => {}
            }
        }))
        .serve_simulated_shutdown(net.clone(), seed, ctx.clone());
    handles.push(task::spawn(fut));

    for addr in (&mut addrs).take(group_size) {
        let fut = Mesh::default()
            .join_seed(seed, false)
            .serve_simulated_shutdown(net.clone(), addr, ctx.clone());

        handles.push(task::spawn(fut));
    }
    assert!(
        fina.await.is_ok(),
        "first group timed out, size: {}",
        group_size
    );

    let finb = timeout(PER_MEMBER * group_size as u32, _ctxb);
    for addr in (&mut addrs).take(group_size) {
        let fut = Mesh::default()
            .join_seed(seed, false)
            .serve_simulated_shutdown(net.clone(), addr, ctx.clone());

        handles.push(task::spawn(fut));
    }

    let r = finb.await;
    ctx.complete();
    futures::future::join_all(handles).await;
    assert!(r.is_ok(), "second group timed out, size: {}", group_size);
}

#[tokio::test(core_threads = 16)]
async fn join_protocol() {
    for n in 1..16 {
        join_protocol_with(n).await;
    }
}
