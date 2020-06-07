// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use fnv::FnvBuildHasher;
use std::{
    collections::{btree_set::IntoIter, BTreeSet},
    hash::{BuildHasher, Hash, Hasher},
    iter::{FromIterator, Map},
    mem::{self, ManuallyDrop},
    ops::{Bound, RangeBounds},
    ptr,
};

/// A bound around `T` that doesn't drop any `T`s.
struct UnsafeBounds<T> {
    s: Bound<ManuallyDrop<(u64, T)>>,
    e: Bound<ManuallyDrop<(u64, T)>>,
}

impl<T> RangeBounds<(u64, T)> for UnsafeBounds<T> {
    fn start_bound(&self) -> Bound<&(u64, T)> {
        match &self.s {
            Bound::Excluded(b) => Bound::Excluded(&b),
            Bound::Included(b) => Bound::Included(&b),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&(u64, T)> {
        match &self.e {
            Bound::Excluded(b) => Bound::Excluded(&b),
            Bound::Included(b) => Bound::Included(&b),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

/// A tumbled hash set. It can be thought of as an ordered set with k unique rings of all
/// contained items, in which adjacent edges form an [expander graph][expander].
///
/// ```text
/// [ 0, 1, 2, 3, 4, 5 ] k: 4
///  |                |
///  v                v
/// [ 0, 4, 3, 1, 2, 5 ] ring: 0
/// [ 4, 0, 2, 1, 5, 3 ] ring: 1
/// [ 1, 0, 3, 4, 5, 2 ] ring: 2
/// [ 3, 2, 5, 0, 1, 4 ] ring: 3
/// ```
///
/// [expander]: https://en.wikipedia.org/wiki/Expander_graph
pub struct Tumbler<T, S = FnvBuildHasher> {
    hasher: S,
    rings: Vec<BTreeSet<(u64, T)>>,
}

impl<T: Ord + Hash + Clone, S: BuildHasher> Extend<T> for Tumbler<T, S> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for t in iter {
            self.insert(t);
        }
    }
}

impl<T: Ord + Hash + Clone> FromIterator<T> for Tumbler<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut tumbler = Self::new(1);
        tumbler.extend(iter);
        tumbler
    }
}

impl<T: Ord, S> IntoIterator for Tumbler<T, S> {
    type Item = T;

    #[allow(clippy::type_complexity)]
    type IntoIter = Map<IntoIter<(u64, T)>, fn((u64, T)) -> T>;

    fn into_iter(mut self) -> Self::IntoIter {
        mem::replace(&mut self.rings[0], BTreeSet::new())
            .into_iter()
            .map(|(_, t)| t)
    }
}

impl<T: Ord + Hash + Clone> Tumbler<T> {
    /// Create a new tumbler with `size` rings.
    ///
    /// # Panics
    /// Panics if `size == 0`.
    pub fn new(size: usize) -> Self {
        Self::with_hasher(size, Default::default())
    }
}

impl<T: Ord + Hash + Clone, S: BuildHasher> Tumbler<T, S> {
    /// Create a new tumbler with `size` rings and the provided `hasher`.
    ///
    /// # Panics
    /// Panics if `size == 0`.
    pub fn with_hasher(size: usize, hasher: S) -> Self {
        assert!(size >= 1);

        let mut rings = Vec::with_capacity(size);
        rings.resize_with(size, Default::default);
        Self { hasher, rings }
    }

    fn hash(&self, seed: usize, val: &T) -> u64 {
        let mut h = self.hasher.build_hasher();
        seed.hash(&mut h);
        val.hash(&mut h);
        h.finish()
    }

    /// Returns the number of rings in the tumbler.
    pub fn size(&self) -> usize {
        self.rings.len()
    }

    /// Returns the number of entries in the tumbler.
    pub fn len(&self) -> usize {
        self.rings[0].len()
    }

    /// Returns whether the tumbler is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over all entries in the tumbler, in the first ring's order.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
        self.ring(0)
    }

    fn ring(&self, idx: usize) -> impl DoubleEndedIterator<Item = &T> {
        self.rings[idx].iter().map(|(_, v)| v)
    }

    fn range<R: RangeBounds<T>>(
        &self,
        idx: usize,
        range: R,
    ) -> impl DoubleEndedIterator<Item = &T>
    {
        // NOTE(safety): should be safe because we prevent side-effects
        let convert = |t: &T| unsafe {
            let h = self.hash(idx, t);
            let v = ptr::read(t);
            ManuallyDrop::new((h, v))
        };

        let bound_from = |bound| match bound {
            Bound::Excluded(t) => Bound::Excluded(convert(t)),
            Bound::Included(t) => Bound::Included(convert(t)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let bounds = UnsafeBounds {
            s: bound_from(range.start_bound()),
            e: bound_from(range.end_bound()),
        };

        self.rings[idx].range(bounds).map(|(_, v)| v)
    }

    /// Insert `val` into the tumbler. Returns whether it was inserted.
    pub fn insert(&mut self, val: T) -> bool {
        // if there's only one ring, we can take ownership right away.
        if self.rings.len() == 1 {
            let h = self.hash(0, &val);
            return self.rings[0].insert((h, val));
        }

        // insert the first entry to know if it should succeed.
        let h = self.hash(0, &val);
        let e = self.rings[0].insert((h, val.clone())); // NOTE: check if its its_none() on hmap

        // insert cloned entries in the middle.
        for ring in 1..self.rings.len() - 1 {
            let h = self.hash(ring, &val);
            assert_eq!(e, self.rings[ring].insert((h, val.clone())));
        }

        // take ownership of val for the last entry.
        if self.rings.len() > 1 {
            let ring = self.rings.len() - 1;
            let h = self.hash(ring, &val);
            assert_eq!(e, self.rings[ring].insert((h, val)));
        }

        e
    }

    /// Remove `val` from the tumbler. Returns whether it was removed.
    pub fn remove(&mut self, val: &T) -> bool {
        // NOTE(invariant): if present in any ring, val is equivalent in all rings.
        let mut entry = false;
        for ring in 0..self.rings.len() {
            let h = self.hash(ring, val);

            // NOTE(safety): should be safe because we prevent side-effects
            let k = unsafe { ManuallyDrop::new((h, ptr::read(val))) };
            entry = self.rings[ring].remove(&k);
        }
        entry
    }

    /// Returns whether the tumbler contains `val`.
    pub fn contains(&self, val: &T) -> bool {
        // NOTE(invariant): if present in any ring, val is present in all rings.
        let h = self.hash(0, val);

        // NOTE(safety): should be safe because we prevent side-effects
        let k = unsafe { ManuallyDrop::new((h, ptr::read(val))) };
        self.rings[0].contains(&k)
    }

    /// Returns an iterator over every leading edge of `e`, across all rings.
    ///
    /// ```text
    /// where e: 3
    ///            |
    ///       [ 0, 4, 3, 1, 2, 5 ]    ring: 0
    /// [ 0, 2, 1, 5, 3, 4 ]          ring: 1
    ///       [ 1, 0, 3, 4, 5, 2 ]    ring: 2
    ///          [ 4, 3, 2, 5, 0, 1 ] ring: 3
    ///            |
    ///
    /// (4, 3) -> 4
    /// (5, 3) -> 5
    /// (0, 3) -> 0
    /// (4, 3) -> 4
    ///
    /// [4, 5, 0, 4]
    /// ```
    pub fn predecessors<'a>(&'a self, e: &'a T) -> impl DoubleEndedIterator<Item = &T> {
        (0..self.size()).flat_map(move |k| {
            self.range(k, ..e)
                .next_back()
                .or_else(|| self.ring(k).next_back())
        })
    }

    /// Returns an iterator over every trailing edge of `e`, across all rings.
    ///
    /// ```text
    /// where e: 3
    ///                  |
    ///       [ 0, 4, 3, 1, 2, 5 ]    ring: 0
    /// [ 0, 2, 1, 5, 3, 4 ]          ring: 1
    ///       [ 1, 0, 3, 4, 5, 2 ]    ring: 2
    ///          [ 4, 3, 2, 5, 0, 1 ] ring: 3
    ///                  |
    ///
    /// (3, 1) -> 1
    /// (3, 4) -> 4
    /// (3, 4) -> 4
    /// (3, 2) -> 2
    ///
    /// [1, 4, 4, 2]
    /// ```
    pub fn successors<'a>(&'a self, e: &'a T) -> impl DoubleEndedIterator<Item = &T> {
        (0..self.size()).flat_map(move |k| {
            self.range(k, (Bound::Excluded(e), Bound::Unbounded))
                .next()
                .or_else(|| self.ring(k).next())
        })
    }

    /// Clear the tumbler, removing all entries.
    pub fn clear(&mut self) {
        for ring in self.rings.iter_mut() {
            ring.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;
    use std::{collections::HashSet, iter, num::NonZeroUsize};

    #[quickcheck]
    fn rings_contain_all_elements(k: NonZeroUsize, input: HashSet<u32>) -> bool {
        let mut t = Tumbler::new(k.get());
        t.extend(input.iter().copied());

        let rings: Vec<HashSet<_>> = (0..k.get()).map(|k| t.ring(k).collect()).collect();

        let mut r = None;
        for ring in rings {
            if ring.len() != input.len() {
                return false;
            }
            if matches!(r, Some(r) if r != ring) {
                return false;
            }

            r = Some(ring);
        }

        true
    }

    #[quickcheck]
    fn is_deterministic(k: NonZeroUsize, input: Vec<u32>) -> bool {
        let k = k.get();

        let mut a = Tumbler::new(k);
        a.extend(input.iter().copied());
        let mut b = Tumbler::new(k);
        b.extend(input.iter().copied());

        let a = (0..k).flat_map(|k| a.ring(k)).collect::<Vec<_>>();
        let b = (0..k).flat_map(|k| b.ring(k)).collect::<Vec<_>>();

        a == b
    }

    #[test]
    fn rings_are_uniquely_ordered() {
        let mut t = Tumbler::new(3);
        t.extend(0..8);
        let rings: Vec<Vec<_>> = (0..3).map(|k| t.ring(k).collect()).collect();

        assert_ne!(rings[0], rings[1]);
        assert_ne!(rings[1], rings[2]);
        assert_ne!(rings[2], rings[0]);
    }

    #[test]
    fn drop_values_dont_explode() {
        let mut t = Tumbler::new(2);

        t.extend(iter::repeat("afdsa".to_owned()).take(8));

        for x in t.ring(0) {
            dbg!(x);
        }

        t.remove(&"fdsa".to_owned());

        t.contains(&"fdsa".to_owned());

        for x in t.range(1, "a".to_owned()..) {
            dbg!(x);
        }
    }
}
