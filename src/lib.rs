// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! A crate for writing fast and highly resilient in-process gRPC service meshes.
//!
//! # Overview
//! `blip` provides an implementation of distributed membership based on [rapid], exposed
//! as a gRPC service. Groups of servers become aware of each other through the membership
//! protocol, and any given member may expose its own metadata or linked services through
//! the same backing gRPC server.
//!
//! In essence, this crate provides a membership list with strong consistency semantics
//! (as opposed to weakly consistent protocols like [SWIM]), distributed fault detection,
//! and grpc routing.
//!
//! # Service Discovery
//! `blip` is designed to build heterogenous meshes. As such, members may expose arbitrary
//! (immutable) key-value metadata when they join a mesh, which can be used for the purpose
//! of service discovery.
//!
//! # Sharding and State
//! `blip` does not enforce any invariants with regard to state held by members of a mesh.
//! For maximal flexibility, state and sharding are deferred to implementations of member
//! services.
//!
//! # Feature Flags
//! * `full`: Enables all optional features.
//! * `cache`: Enables the [cache][service::cache] service.
//!
//! # References
//! * [Stable and Consistent Membership at Scale with Rapid][rapid]
//! * [Fast Paxos][fpx]
//!
//! [rapid]: https://arxiv.org/abs/1803.03620
//! [fpx]: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
//! [SWIM]: https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
#![feature(entry_insert)]
#![warn(rust_2018_idioms, missing_docs)]
#![doc(
    issue_tracker_base_url = "https://github.com/nytopop/blip/issues/",
    html_logo_url = "https://raw.githubusercontent.com/nytopop/blip/master/blip.png",
    html_root_url = "https://docs.rs/blip/0.1.0",
    test(
        no_crate_inject,
        attr(
            deny(rust_2018_idioms, unused_imports, unused_mut),
            allow(unused_variables)
        )
    )
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(test)]
#[test]
fn test_html_root_url() {
    version_sync::assert_html_root_url_updated!("src/lib.rs");
}

#[macro_use]
mod macros;

mod collections;

pub mod cluster;
pub mod overlay;
pub mod service;

#[doc(inline)]
pub use cluster::cut::{Member, MultiNodeCut, Subscription};
#[doc(inline)]
pub use overlay::{ExposedService, Mesh, MeshService};

/// A re-export of [async_trait] for convenience.
///
/// [async_trait]: https://docs.rs/async-trait/latest/async_trait/attr.async_trait.html
#[doc(inline)]
pub use tonic::async_trait;
