// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use rand::random;
use std::{
    cmp::Ordering,
    collections::BTreeSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn unixtime() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("not to be approaching a spacetime singularity")
        .as_secs()
}

/// An asynchronous event id.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Id {
    unix: u64,
    uniq: u64,
}

impl PartialOrd for Id {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Id {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.unix, self.uniq)
            .cmp(&(other.unix, other.uniq))
            .reverse()
    }
}

impl Id {
    /// Reconstruct an event id from a timestamp and unique identifier.
    pub const fn new(unix: u64, uniq: u64) -> Self {
        Self { unix, uniq }
    }

    /// Generates a new event id.
    pub fn generate() -> Self {
        Self::new(unixtime(), random())
    }

    /// Returns an event id that is ~always expired.
    #[cfg(test)]
    const fn max_age(uniq: u64) -> Self {
        Self::new(u64::min_value(), uniq)
    }

    /// Returns an event id that is ~never expired.
    #[cfg(test)]
    const fn min_age(uniq: u64) -> Self {
        Self::new(u64::max_value(), uniq)
    }

    /// Returns the (logically) smallest event id with the provided timestamp.
    const fn least_at(unix: u64) -> Self {
        Self::new(unix, u64::max_value())
    }

    /// Returns the time this event id was generated (as seconds since the unix epoch).
    pub const fn timestamp(&self) -> u64 {
        self.unix
    }

    /// Returns the unique component of this event id.
    pub const fn unique(&self) -> u64 {
        self.uniq
    }
}

/// A filter for ignoring duplicate or expired instances of asynchronous events.
///
/// The expiration mechanism serves to prevent the filter from infinitely increasing in size
/// as events are added; after a configurable amount of time, events are purged.
pub struct Filter {
    events: BTreeSet<Id>,
    oldest: u64,
}

impl Filter {
    /// Create a new filter that will consider events older than `dur` expired.
    pub fn new(dur: Duration) -> Self {
        Self {
            events: BTreeSet::new(),
            oldest: dur.as_secs(),
        }
    }

    /// Insert an event into the filter.
    ///
    /// Returns `true` if the event hasn't been seen before, and isn't expired.
    pub fn insert(&mut self, event: Id) -> bool {
        let expires_at = unixtime().saturating_sub(self.oldest);

        // expires_at = 2;
        //
        //    v-- newest                                      v-- oldest
        // [ (5, 3), (4, 1), (3, 5), (2, 3), (2, 2), (2, 1), (1, 8) ]
        //                          ^-- (2, u64::max)       ^-- (2, u64::min)
        // |--------- kept ---------|----------- purged ------------|
        self.events.split_off(&Id::least_at(expires_at));

        if event.unix <= expires_at {
            return false;
        }

        self.events.insert(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;
    use std::{collections::HashSet, time::Duration};

    #[quickcheck]
    fn unique_events_can_be_inserted_once(max: Duration, input: HashSet<u64>) -> bool {
        let mut f = Filter::new(max);

        (input.into_iter())
            .map(|uq| Id::min_age(uq))
            .all(|id| f.insert(id) && !f.insert(id))
    }

    #[quickcheck]
    fn expired_events_are_always_false(max: Duration, input: HashSet<u64>) -> bool {
        let mut f = Filter::new(max);

        (input.into_iter())
            .map(|uq| Id::max_age(uq))
            .all(|id| !f.insert(id))
    }
}
