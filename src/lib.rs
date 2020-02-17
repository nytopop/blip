// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! A Rust framework for writing fast and highly resilient gRPC mesh services.
#![feature(trait_alias, entry_insert)]
#![warn(rust_2018_idioms, missing_docs)]
#![doc(html_logo_url = "https://github.com/nytopop/blib/raw/master/blip.png")]

#[cfg(test)]
mod tests;
#[macro_use]
mod macros;
pub mod cluster;
mod collections;
pub mod overlay;

#[doc(inline)]
pub use cluster::{
    cut::{Member, MultiNodeCut, Subscription},
    partition::Rejoin,
};
#[doc(inline)]
pub use overlay::{Mesh, MeshService};
#[doc(inline)]
pub use tonic::async_trait;
