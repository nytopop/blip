// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! Ready-to-use mesh-aware services.

#[cfg(feature = "cache")]
#[cfg_attr(docsrs, doc(cfg(feature = "cache")))]
pub mod cache;

#[cfg(feature = "cache")]
#[cfg_attr(docsrs, doc(cfg(feature = "cache")))]
#[doc(inline)]
pub use cache::Cache;
