// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
mod ctx;

use super::*;
use ctx::Context;

use std::net::{SocketAddr, ToSocketAddrs};
use tokio::{
    task,
    time::{timeout, Duration},
};

#[async_trait]
impl<P: FnMut(MultiNodeCut) + Send> MeshService for P {
    async fn accept(mut self, mut cuts: Subscription) {
        while let Ok(cut) = cuts.recv().await {
            (self)(cut);
        }
    }
}

fn addr<S: ToSocketAddrs>(addr: S) -> SocketAddr {
    addr.to_socket_addrs().unwrap().next().unwrap()
}

#[tokio::test]
async fn bootstrap_works() {
    let ctx = Context::default();
    let fin = timeout(Duration::from_secs(3), ctx.clone());

    task::spawn(
        Mesh::default()
            .add_mesh_service(move |_| ctx.complete())
            .serve(addr(("localhost", 8089))),
    );

    assert_eq!(Ok(()), fin.await);
}

// TODO: assert that each node ends up with the same configuration
// TODO: don't task::spawn because it breaks something in hyper when the futures are dropped
// TODO: sim network that isn't backed by tcp sockets
// TODO: sim attribute macro, such that we can use something like:
// #[simulation::test]
// async fn some_property(env: Simulation) {
//     whatever.await;
// }
async fn join_two_groups(group_n: u16, group_k: u16) {
    const TIMEOUT: Duration = Duration::from_secs(15);
    const N: u16 = 7;
    const K: u16 = N * 2;

    let nctx = Context::default();
    let nfin = timeout(TIMEOUT, nctx.clone());

    let kctx = Context::default();
    let kfin = timeout(TIMEOUT, kctx.clone());

    let seed = addr("127.0.0.1:1234");

    task::spawn(
        Mesh::default()
            .add_mesh_service(move |cut: MultiNodeCut| match cut.members().len() {
                1 => {}

                n if n == (group_n + 1) as usize => {
                    nctx.complete();
                }
                n if n == (group_n + group_k + 1) as usize => {
                    kctx.complete();
                }

                //n => panic!("cut should be 1,{},{} not: {}", group_n, group_k, n),
                _ => {}
            })
            .serve(seed),
    );

    // join seed on it's own should resolve every member quickly, and doesn't require
    // reaching out to do fast paxos.
    let max_n = 1235 + group_n;
    let mut ports = vec![];
    for port in 1235..max_n {
        ports.push(port);
        task::spawn(
            Mesh::default()
                .join_seed(seed, false)
                .serve(addr(("localhost", port))),
        );
    }
    println!("ports: {:?}", ports);
    assert_eq!(Ok(()), nfin.await, "{}-cluster failed to converge", N + 1);

    // once the cluster has N members, joining k requires them to achieve consensus.
    let max_k = max_n + group_k;
    for port in max_n..max_k {
        task::spawn(
            Mesh::default()
                .join_seed(seed, false)
                .serve(addr(("localhost", port))),
        );
    }
    assert_eq!(Ok(()), kfin.await, "{}-cluster failed to converge", K + 1);
}

#[tokio::test(threaded_scheduler)]
async fn join_protocol() {
    // 1 -> 8 -> 17
    join_two_groups(7, 9).await;
}
