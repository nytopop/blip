// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

/// Conditionally return from an `-> Option<T>` context based on a boolean expression.
macro_rules! guard {
    ($e:expr) => {{
        if !$e {
            return None;
        }
    }};

    ($e:expr, $v:expr) => {{
        if $e {
            Some($v)
        } else {
            None
        }
    }};
}
