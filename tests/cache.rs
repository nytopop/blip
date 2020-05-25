// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
#![cfg(feature = "cache")]

mod shared;

use blip::{service::Cache, Mesh};
use shared::{addr_in, cfg_handle, init_logger, subnet};
use tokio::{join, task};

/// Tests that keys are distributed amongst multiple nodes, and that all nodes in a given
/// configuration agree with each other wrt who owns which key.
#[tokio::test]
async fn keys_are_distributed() {
    init_logger();
    let net = subnet();

    let a = Cache::from_fn(16, |_| b"a".to_vec());
    let (mut ha, hsa) = cfg_handle();
    let af = Mesh::low_latency()
        .add_mesh_service(hsa)
        .add_service(a.clone())
        .serve(addr_in(net, 1));

    let b = Cache::from_fn(16, |_| b"b".to_vec());
    let (mut hb, hsb) = cfg_handle();
    let bf = Mesh::low_latency()
        .add_mesh_service(hsb)
        .add_service(b.clone())
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 2));

    task::spawn(af);
    task::spawn(bf);

    join![ha.cfg_change(2), hb.cfg_change(2)];

    for key in vec![
        ("test"),
        ("these"),
        ("keys"),
        ("for"),
        ("distribution"),
        ("basically"),
        ("each node"),
        ("should"),
        ("agree on"),
        ("placement"),
    ] {
        let (v1, v2) = join![a.get(key), b.get(key)];
        assert_eq!(v1.unwrap(), v2.unwrap());
        let (v1, v2) = join![a.get(key), b.get(key)];
        assert_eq!(v1.unwrap(), v2.unwrap());
    }
}

/// Tests that key distribution amongst multiple nodes is stable when individual cache
/// nodes evict keys.
#[tokio::test]
async fn key_placement_is_stable() {
    init_logger();
    let net = subnet();

    let a = Cache::from_fn(1, |_| b"a".to_vec());
    let (mut ha, hsa) = cfg_handle();
    let af = Mesh::low_latency()
        .add_mesh_service(hsa)
        .add_service(a.clone())
        .serve(addr_in(net, 1));

    let b = Cache::from_fn(1, |_| b"b".to_vec());
    let (mut hb, hsb) = cfg_handle();
    let bf = Mesh::low_latency()
        .add_mesh_service(hsb)
        .add_service(b.clone())
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 2));

    task::spawn(af);
    task::spawn(bf);

    join![ha.cfg_change(2), hb.cfg_change(2)];

    let keys = vec![
        ("these"),
        ("other keys"),
        ("should"),
        ("also be"),
        ("distributed"),
        ("in a consistent"),
        ("manner when"),
        ("max_keys is set"),
        ("to some"),
        ("small value"),
    ];

    for key in keys.iter() {
        let (v1, v2) = join![a.get(*key), b.get(*key)];
        assert_eq!(v1.unwrap(), v2.unwrap());
    }
    for key in keys.iter() {
        let (v1, v2) = join![a.get(*key), b.get(*key)];
        assert_eq!(v1.unwrap(), v2.unwrap());
    }
}
