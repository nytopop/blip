// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use std::io;

fn main() -> io::Result<()> {
    tonic_build::compile_protos("proto/blip.proto")?;

    #[cfg(feature = "cache")]
    tonic_build::compile_protos("proto/cache.proto")?;

    Ok(())
}
