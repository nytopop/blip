// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
#![type_length_limit = "8388608"]

mod shared;

use blip::Mesh;
use futures::future::{join, join3, FutureExt};
use shared::init_logger;
use shared::{addr_in, cfg_handle, subnet};
use tokio::{select, task};

/// Tests that a single node can bootstrap a configuration without any other nodes.
#[tokio::test]
async fn single_node_cluster_bootstrap() {
    init_logger();
    let (mut h, hs) = cfg_handle();

    let srv = Mesh::low_latency()
        .add_mesh_service(hs)
        .serve(addr_in(subnet(), 1));

    select! {
        e = srv => panic!("mesh exited with: {:?}", e),
        _ = h.cfg_change(1) => {}
    }
}

/// Tests that three nodes can converge on a single configuration that includes all of them.
#[tokio::test]
async fn three_node_cluster_bootstrap() {
    init_logger();
    let net = subnet();

    let (mut h1, hs1) = cfg_handle();
    let s1 = Mesh::low_latency()
        .add_mesh_service(hs1)
        .serve(addr_in(net, 1));

    let (mut h2, hs2) = cfg_handle();
    let s2 = Mesh::low_latency()
        .add_mesh_service(hs2)
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 2));

    let (mut h3, hs3) = cfg_handle();
    let s3 = Mesh::low_latency()
        .add_mesh_service(hs3)
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 3));

    select! {
        e = s1 => panic!("s1 exited with: {:?}", e),
        e = s2 => panic!("s2 exited with: {:?}", e),
        e = s3 => panic!("s3 exited with: {:?}", e),

        (c1, c2, c3) = join3(h1.cfg_change(3), h2.cfg_change(3), h3.cfg_change(3)) => {
            assert!(c1.conf_id() == c2.conf_id());
            assert!(c2.conf_id() == c3.conf_id());
        }
    }
}

/// Tests that in the event a member of a three node configuration becomes partitioned from
/// the others, it is ejected from the configuration. Once it comes back online, it should
/// rejoin the cluster.
#[tokio::test]
async fn three_node_cluster_partition_recovery() {
    init_logger();
    let net = subnet();

    let (mut h1, hs1) = cfg_handle();
    let mut s1 = Mesh::low_latency()
        .add_mesh_service(hs1)
        .serve(addr_in(net, 1))
        .boxed();

    let (mut h2, hs2) = cfg_handle();
    let mut s2 = Mesh::low_latency()
        .add_mesh_service(hs2)
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 2))
        .boxed();

    let (mut h3, hs3) = cfg_handle();
    let mut s3 = Mesh::low_latency()
        .add_mesh_service(hs3)
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 3))
        .boxed();

    // wait for cluster to bootstrap
    select! {
        e = &mut s1 => panic!("s1 exited with: {:?}", e),
        e = &mut s2 => panic!("s2 exited with: {:?}", e),
        e = &mut s3 => panic!("s3 exited with: {:?}", e),

        (c1, c2, c3) = join3(h1.cfg_change(3), h2.cfg_change(3), h3.cfg_change(3)) => {
            assert!(c1.conf_id() == c2.conf_id());
            assert!(c2.conf_id() == c3.conf_id());
        }
    }

    // progress s1/s2 but not s3 and wait until it gets ejected
    select! {
        e = &mut s1 => panic!("s1 exited with: {:?}", e),
        e = &mut s2 => panic!("s2 exited with: {:?}", e),

        (c1, c2) = join(h1.cfg_change(2), h2.cfg_change(2)) => {
            assert!(c1.conf_id() == c2.conf_id());
        }
    }

    // wait for s3 to rejoin
    select! {
        e = &mut s1 => panic!("s1 exited with: {:?}", e),
        e = &mut s2 => panic!("s2 exited with: {:?}", e),
        e = &mut s3 => panic!("s3 exited with: {:?}", e),

        (c1, c2, c3) = join3(h1.cfg_change(3), h2.cfg_change(3), h3.cfg_change(3)) => {
            assert!(c1.conf_id() == c2.conf_id());
            assert!(c2.conf_id() == c3.conf_id());
        }
    }
}

/// Tests that both members of a two node configuration agree on each other's metadata.
#[tokio::test]
async fn two_node_cluster_metadata_consensus() {
    init_logger();
    let net = subnet();

    let (mut h1, hs1) = cfg_handle();
    let s1 = Mesh::low_latency()
        .add_mesh_service(hs1)
        .add_metadata(vec![("this key is s1".to_owned(), b"s1".to_vec())])
        .serve(addr_in(net, 1));
    task::spawn(s1);

    let (mut h2, hs2) = cfg_handle();
    let s2 = Mesh::low_latency()
        .add_mesh_service(hs2)
        .add_metadata(vec![("this key is s2".to_owned(), b"s2".to_vec())])
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 2));
    task::spawn(s2);

    let (c1, c2) = join(h1.cfg_change(2), h2.cfg_change(2)).await;

    assert!(c1.conf_id() == c2.conf_id());

    for m in c1.members().into_iter().chain(c2.members().into_iter()) {
        if m.addr() == addr_in(net, 1) {
            assert_eq!(b"s1".as_ref(), &*m.metadata()["this key is s1"]);
        } else {
            assert_eq!(b"s2".as_ref(), &*m.metadata()["this key is s2"]);
        }
    }
}

/// Tests that all members in a three node configuration agree on each other's metadata, even
/// if a member leaves the cluster and immediately rejoins with changed metadata.
#[tokio::test]
async fn three_node_cluster_metadata_rejoin_consensus() {
    init_logger();
    let net = subnet();

    let (mut h1, hs1) = cfg_handle();
    let mut s1 = Mesh::low_latency()
        .add_mesh_service(hs1)
        .add_metadata(vec![("this key is s1".to_owned(), b"s1".to_vec())])
        .serve(addr_in(net, 1))
        .boxed();

    let (mut h2, hs2) = cfg_handle();
    let mut s2 = Mesh::low_latency()
        .add_mesh_service(hs2)
        .add_metadata(vec![("this key is s2".to_owned(), b"s2".to_vec())])
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 2))
        .boxed();

    let (mut h3, hs3) = cfg_handle();
    let s3 = Mesh::low_latency()
        .add_mesh_service(hs3)
        .add_metadata(vec![("this key is s3".to_owned(), b"s3-original".to_vec())])
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 3));

    // wait for cluster to bootstrap
    select! {
        e = &mut s1 => panic!("s1 exited with {:?}", e),
        e = &mut s2 => panic!("s2 exited with {:?}", e),
        e = s3 => panic!("s3 exited with {:?}", e),

        (c1, c2, c3) = join3(h1.cfg_change(3), h2.cfg_change(3), h3.cfg_change(3)) => {
            assert!(c1.conf_id() == c2.conf_id());
            assert!(c2.conf_id() == c3.conf_id());

            for m in c1.members().into_iter()
              .chain(c2.members().into_iter())
              .chain(c3.members().into_iter())
            {
                if m.addr() == addr_in(net, 1) {
                    assert_eq!(b"s1".as_ref(), &*m.metadata()["this key is s1"]);
                } else if m.addr() == addr_in(net, 2) {
                    assert_eq!(b"s2".as_ref(), &*m.metadata()["this key is s2"]);
                } else {
                    assert_eq!(b"s3-original".as_ref(), &*m.metadata()["this key is s3"]);
                }
            }
        }
    }

    // re-create s3 from scratch, with a new uuid and different metadata
    let (mut h3, hs3) = cfg_handle();
    let s3 = Mesh::low_latency()
        .add_mesh_service(hs3)
        .add_metadata(vec![("this key is s3".to_owned(), b"s3-modified".to_vec())])
        .join_seed(addr_in(net, 1), false)
        .serve(addr_in(net, 3));

    // wait for the new s3 to rejoin
    select! {
        e = &mut s1 => panic!("s1 exited with {:?}", e),
        e = &mut s2 => panic!("s2 exited with {:?}", e),
        e = s3 => panic!("s3 exited with {:?}", e),

        (c1, c2, c3) = join3(h1.cfg_change(3), h2.cfg_change(3), h3.cfg_change(3)) => {
            assert!(c1.conf_id() == c2.conf_id());
            assert!(c2.conf_id() == c3.conf_id());

            for m in c1.members().into_iter()
              .chain(c2.members().into_iter())
              .chain(c3.members().into_iter())
            {
                if m.addr() == addr_in(net, 1) {
                    assert_eq!(b"s1".as_ref(), &*m.metadata()["this key is s1"]);
                } else if m.addr() == addr_in(net, 2) {
                    assert_eq!(b"s2".as_ref(), &*m.metadata()["this key is s2"]);
                } else {
                    assert_eq!(b"s3-modified".as_ref(), &*m.metadata()["this key is s3"]);
                }
            }
        }
    }
}
