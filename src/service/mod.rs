// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Ready-to-use mesh-aware services.

/// Evaluates to the routable name of an [ExposedService].
///
/// This is useful for inclusion as a ~unique key in service metadata.
#[cfg(feature = "cache")]
macro_rules! key {
    ($t:ty) => {
        <<$t as $crate::ExposedService>::Service as tonic::transport::server::NamedService>::NAME
    };
}

#[cfg(feature = "cache")]
#[cfg_attr(docsrs, doc(cfg(feature = "cache")))]
pub mod cache;

#[cfg(feature = "cache")]
#[cfg_attr(docsrs, doc(cfg(feature = "cache")))]
#[doc(inline)]
pub use cache::Cache;
