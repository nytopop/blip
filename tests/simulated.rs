// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
#![cfg(feature = "simulation")]

use async_ctx::Context;
use blip::{simulation::Network, Mesh, MeshService, MultiNodeCut, Subscription};
use std::{
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroU8,
};
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

fn linear(src: u8, ctl: u8, lo: f64, hi: f64) -> usize {
    let src = src as f64;
    let ctl = ctl as f64;

    let min = lo * src;
    let range = (hi * src) - min;
    let step = range / 255.0;

    (min + (ctl * step)).round() as usize
}

const PER_MEMBER: Duration = Duration::from_millis(250);

// TODO: assert that each node ends up with the same config
#[quickcheck_async::tokio(core_threads = 16, max_threads = 16)]
async fn join_protocol(group_size: NonZeroU8) {
    // TODO: to prevent duplicate tests, use constrained type that implements Arbitrary
    let group_size = linear(100, group_size.get(), 0.01, 0.05);

    let net = Network::default();

    let ctx = Context::default();
    let ctxa = ctx.child();
    let fina = timeout(PER_MEMBER * group_size as u32, ctxa.clone());
    let ctxb = ctx.child();
    let finb = timeout(PER_MEMBER * group_size as u32, ctxb.clone());

    let mut addrs = (1u16..).map(|port| addr(("1.1.1.1", port)));
    let seed = addrs.next().unwrap();

    let mut handles = vec![];
    let fut = Mesh::default()
        .add_mesh_service(Svc(move |cut: MultiNodeCut| match cut.members().len() {
            1 => {}
            n if n == 1 + group_size => ctxa.complete(),
            n if n == 1 + (group_size * 2) => ctxb.complete(),
            n => panic!("cut is {}, but group_size is {}", n, group_size),
        }))
        .serve_simulated_shutdown(net.clone(), seed, ctx.clone());
    handles.push(task::spawn(fut));

    for addr in (&mut addrs).take(group_size) {
        let fut = Mesh::default()
            .join_seed(seed, false)
            .serve_simulated_shutdown(net.clone(), addr, ctx.clone());

        handles.push(task::spawn(fut));
    }

    fina.await.unwrap();

    for addr in (&mut addrs).take(group_size) {
        let fut = Mesh::default()
            .join_seed(seed, false)
            .serve_simulated_shutdown(net.clone(), addr, ctx.clone());

        handles.push(task::spawn(fut));
    }

    let r = finb.await;
    ctx.complete();
    futures::future::join_all(handles).await;
    r.unwrap();
}
