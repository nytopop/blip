// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use std::{
    collections::{
        hash_map::{IntoIter, Iter},
        HashMap,
    },
    hash::Hash,
    iter::FromIterator,
};

/// A set of values mapped to the frequency of their occurrence.
pub struct FreqSet<T> {
    inner: HashMap<T, usize>,
}

impl<T: Hash + Eq> Extend<T> for FreqSet<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for t in iter {
            *self.inner.entry(t).or_insert(0) += 1;
        }
    }
}

impl<T: Hash + Eq> FromIterator<T> for FreqSet<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (sz, u) = it.size_hint();

        let mut fs = Self {
            inner: HashMap::with_capacity(u.unwrap_or(sz)),
        };

        fs.extend(it);
        fs
    }
}

impl<T: Hash + Eq> IntoIterator for FreqSet<T> {
    type Item = (T, usize);

    type IntoIter = IntoIter<T, usize>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<T> FreqSet<T> {
    /// Create a new set.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns the total number of elements in the set, counting duplicates.
    pub fn total(&self) -> usize {
        self.inner.values().sum()
    }

    /// Returns an iterator over all elements in the set.
    pub fn iter(&self) -> Iter<'_, T, usize> {
        self.inner.iter()
    }

    /// Remove all elements from the set.
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<T: Hash + Eq> FreqSet<T> {
    /// Insert an element into the set.
    ///
    /// Returns the updated number of times the element has been inserted.
    pub fn insert(&mut self, key: T) -> usize {
        *self.inner.entry(key).and_modify(|v| *v += 1).or_insert(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;
    use std::{collections::HashSet, num::NonZeroU8};

    #[quickcheck]
    fn counts_are_accurate(n: NonZeroU8, input: HashSet<u32>) -> bool {
        let mut fs: FreqSet<u32> = input.iter().copied().collect();
        let n = n.get() as usize;

        for _ in 0..n - 1 {
            fs.extend(input.iter().copied());
        }

        if fs.total() != input.len() * n {
            return false;
        }

        fs.into_iter().all(|(_, c)| c == n)
    }
}
