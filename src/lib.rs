// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! A Rust framework for writing fast and highly resilient gRPC mesh services.
//!
//! # Feature Flags
//! * `simulation`: Enables the simulation network for testing purposes.
#![feature(trait_alias, entry_insert, doc_cfg)]
#![warn(rust_2018_idioms, missing_docs)]
#![doc(
    issue_tracker_base_url = "https://github.com/nytopop/blip/issues/",
    html_logo_url = "https://raw.githubusercontent.com/nytopop/blip/master/blip.png",
    html_root_url = "https://docs.rs/blip/0.1.0-alpha.5",
    test(no_crate_inject, attr(deny(rust_2018_idioms)))
)]

#[macro_use]
mod macros;

mod collections;

pub mod cluster;
pub mod overlay;
#[cfg(feature = "simulation")]
#[cfg_attr(docsrs, doc(cfg(feature = "simulation")))]
pub mod simulation;

#[doc(inline)]
pub use cluster::{
    cut::{Member, MultiNodeCut, Subscription},
    partition::Rejoin,
};
#[doc(inline)]
pub use overlay::{Mesh, MeshService};

/// A re-export of [async_trait](https://docs.rs/async-trait/latest/async_trait/) for
/// convenience.
#[doc(inline)]
pub use tonic::async_trait;
