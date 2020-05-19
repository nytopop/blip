// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//! A distributed binary cache with an immutable cache filling mechanism.
//!
//! The design is similar in many ways to (and is based on) [groupcache].
//!
//! ## Like groupcache, this:
//! * Shards by key to select which node is responsible for a load.
//!
//! * Is wholly in-process. No separate server deployments are required to use it.
//!
//! * Uses a deduplicated cache-filling mechanism. Cache misses will induce exactly one
//!   coordinated fill operation across all interested nodes in the cluster, the result
//!   of which is multiplexed to each simultaneous request.
//!
//! * Only supports immutable values. Once a key has been filled, the value for that key
//!   cannot be changed. It may be forgotten (given enough time), but that behavior can't
//!   be relied upon.
//!
//! * Supports automatically mirroring popular keys to multiple nodes.
//!
//! ## Unlike groupcache, this:
//! * Automatically discovers and monitors peers via integration with a blip [Mesh].
//!
//! [groupcache]: https://github.com/golang/groupcache
//! [Mesh]: crate::Mesh
mod proto {
    tonic::include_proto!("cache");
}

use crate::{ExposedService, MeshService, MultiNodeCut, Subscription};
use bytes::Bytes;
use cache_2q::Cache as Cache2q;
use consistent_hash_ring::Ring;
use once_cell::sync::OnceCell;
use proto::{cache_client::CacheClient, cache_server::CacheServer, Key, Value};
use rand::{thread_rng, Rng};
use std::{
    cmp,
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tonic::{transport::Endpoint, Request, Response, Status};

/// A type that can produce a binary value, given a key.
#[crate::async_trait]
pub trait Source: Sync + Send + 'static {
    /// Retrieve a value for `key`.
    async fn get(&self, key: &[u8]) -> Result<Vec<u8>, Status>;
}

struct FnSource<F>(F);

#[crate::async_trait]
impl<F> Source for FnSource<F>
where F: 'static + Sync + Send + Fn(&[u8]) -> Vec<u8>
{
    async fn get(&self, key: &[u8]) -> Result<Vec<u8>, Status> {
        Ok(self.0(key))
    }
}

/// A distributed binary cache. May be used standalone, or added to a [Mesh] to operate in
/// networked mode.
///
/// # Examples
/// ```
/// use blip::service::Cache;
/// use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
/// # use tonic::Status;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Status> {
/// let loads = AtomicUsize::new(0);
/// let c = Cache::from_fn(32, move |key| {
///     assert_eq!(0, loads.swap(1, SeqCst));
///     key.into()
/// });
///
/// let val = c.get("test key").await?;
/// assert_eq!(&*val, b"test key");
/// let val = c.get("test key").await?;
/// assert_eq!(&*val, b"test key");
/// # Ok(())
/// # }
/// ```
/// [Mesh]: crate::Mesh
pub struct Cache<S: ?Sized = dyn Source>(Arc<Inner<S>>);

struct Inner<S: ?Sized> {
    inflight: Mutex<HashMap<Bytes, Arc<Lazy>>>,
    remote: RwLock<Remote>,
    local_keys: Mutex<Cache2q<Bytes, Bytes>>,
    hot_keys: Mutex<Cache2q<Bytes, Bytes>>,
    source: S,
}

struct Lazy {
    sem: Semaphore,
    val: OnceCell<Result<Bytes, Status>>,
}

enum Flight {
    Leader(Arc<Lazy>),
    Follower(Arc<Lazy>),
}

struct Remote {
    config: Option<MultiNodeCut>,
    indices: HashMap<SocketAddr, usize>,
    shards: Ring<SocketAddr>,
}

impl<S: ?Sized> Clone for Cache<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

const META_KEY: &str = "blip.cache";

#[crate::async_trait]
impl MeshService for Cache {
    async fn accept(self: Box<Self>, mut cuts: Subscription) {
        while let Ok(cut) = cuts.recv().await {
            let mut r = self.0.remote.write().await;
            let r = &mut *r;

            r.indices.clear();
            r.indices.extend(
                (cut.members().iter())
                    .enumerate()
                    .filter(|(_, m)| m.metadata().contains_key(META_KEY))
                    .map(|(i, m)| (m.addr(), i)),
            );

            r.shards.clear();
            r.shards.extend(r.indices.keys().copied());

            r.config = Some(cut);
        }
    }
}

impl ExposedService for Cache {
    #[inline]
    fn add_metadata<K: Extend<(String, Vec<u8>)>>(&self, keys: &mut K) {
        keys.extend(vec![(META_KEY.to_owned(), vec![])]);
    }

    type Service = CacheServer<Self>;

    #[inline]
    fn into_service(self) -> Self::Service {
        CacheServer::new(self)
    }
}

#[crate::async_trait]
impl proto::cache_server::Cache for Cache {
    #[inline]
    async fn get(&self, req: Request<Key>) -> Result<Response<Value>, Status> {
        self.get(req.into_inner().key)
            .await
            .map(|buf| Value { buf: buf.to_vec() })
            .map(Response::new)
    }
}

impl Cache {
    /// Create a new cache from a [Source]. At most `max_keys + (max_keys / 8)` keys will be
    /// cached locally at any point in time.
    ///
    /// # Panics
    /// Panics if `max_keys == 0`.
    ///
    /// # Examples
    /// ```
    /// use blip::service::{cache::Source, Cache};
    /// use tonic::Status;
    ///
    /// struct Echo;
    ///
    /// #[blip::async_trait]
    /// impl Source for Echo {
    ///     async fn get(&self, key: &[u8]) -> Result<Vec<u8>, Status> {
    ///         Ok(key.into())
    ///     }
    /// }
    ///
    /// let cache = Cache::new(1024, Echo);
    /// ```
    pub fn new<S: Source>(max_keys: usize, source: S) -> Self {
        let remote = Remote {
            config: None,
            indices: HashMap::new(),
            shards: Ring::default(),
        };

        let max_hot = cmp::max(1, max_keys / 8);

        let inner = Inner {
            inflight: HashMap::new().into(),
            remote: remote.into(),
            local_keys: Cache2q::new(max_keys).into(),
            hot_keys: Cache2q::new(max_hot).into(),
            source,
        };

        Self(Arc::new(inner))
    }

    /// Create a new cache from a source `Fn`. At most `max_keys + (max_keys / 8)` keys will
    /// be cached locally at any point in time.
    ///
    /// # Panics
    /// Panics if `max_keys == 0`.
    ///
    /// # Examples
    /// ```
    /// use blip::service::Cache;
    ///
    /// let cache = Cache::from_fn(1024, |key| key.into());
    /// ```
    pub fn from_fn<F>(max_keys: usize, source: F) -> Self
    where F: Sync + Send + 'static + Fn(&[u8]) -> Vec<u8> {
        Self::new(max_keys, FnSource(source))
    }

    /// Retrieve the value associated with `key`.
    pub async fn get<K: Into<Bytes>>(&self, key: K) -> Result<Bytes, Status> {
        let key = key.into();

        match self.liftoff(key.clone()).await {
            Flight::Leader(lazy) => {
                let call = self.get_inner(key.clone()).await;
                lazy.val.set(call).unwrap();
                lazy.sem.add_permits(2 ^ 24);
                self.0.inflight.lock().await.remove(&key);
                lazy.val.get().unwrap().clone()
            }

            Flight::Follower(lazy) => {
                drop(lazy.sem.acquire().await);
                lazy.val.get().unwrap().clone()
            }
        }
    }

    /// Start a coordinated load operation.
    #[inline]
    async fn liftoff(&self, key: Bytes) -> Flight {
        match self.0.inflight.lock().await.entry(key) {
            // we must set a value and notify any followers
            Entry::Vacant(v) => Flight::Leader(Arc::clone(v.insert(Arc::new(Lazy {
                sem: Semaphore::new(0),
                val: OnceCell::new(),
            })))),

            // we can read the value when leader notifies us
            Entry::Occupied(o) => Flight::Follower(Arc::clone(o.get())),
        }
    }

    /// Retrieve the value associated with `key`. Unlike [Cache::get], this does _not_
    /// deduplicate requests for the same key.
    async fn get_inner(&self, key: Bytes) -> Result<Bytes, Status> {
        // check if key is already loaded in the hot cache
        if let Some(buf) = load(&self.0.hot_keys, &key).await {
            return Ok(buf);
        }

        // check if key hashes onto another node
        if let Some(shard) = self.lookup_shard(&key).await {
            let mut c = CacheClient::connect(shard)
                .await
                .map_err(|e| format!("{}", e))
                .map_err(Status::unavailable)?;

            let val = c.get(Key { key: key.to_vec() }).await?;
            let buf = Bytes::from(val.into_inner().buf);

            // store in the hot cache 1/8 of the time (space is limited)
            if thread_rng().gen_range(0, 8) == 4 {
                store(&self.0.hot_keys, key, buf.clone()).await;
            }

            return Ok(buf);
        }

        // check if key is already loaded in the local cache
        if let Some(buf) = load(&self.0.local_keys, &key).await {
            return Ok(buf);
        }

        // otherwise, generate from source
        let buf = Bytes::from(self.0.source.get(&key).await?);
        store(&self.0.local_keys, key, buf.clone()).await;
        Ok(buf)
    }

    /// Lookup the shard that is authoritative over `key`. If this returns `None`, it belongs
    /// to the local node.
    ///
    /// The use of a consistent hash ring allows key distribution to remain relatively stable
    /// in the event of cluster membership changes.
    #[inline]
    async fn lookup_shard(&self, key: &[u8]) -> Option<Endpoint> {
        let r = self.0.remote.read().await;
        // if there's no configuration, we're in standalone mode or the mesh hasn't yet
        // bootstrapped; in either case, assume we're the owner of key
        let cut = r.config.as_ref()?;

        let shard = r.shards[key];
        // if the shard's addr is our addr, it's us
        guard!(cut.local_addr() != shard);

        let i = r.indices[&shard];
        let m = &cut.members()[i];

        Some(Endpoint::from(m))
    }
}

/// Load a key's value from the cache.
#[inline]
async fn load(cache: &Mutex<Cache2q<Bytes, Bytes>>, key: &[u8]) -> Option<Bytes> {
    cache.lock().await.get(key).cloned()
}

/// Store a key/value pair in the cache.
#[inline]
async fn store(cache: &Mutex<Cache2q<Bytes, Bytes>>, key: Bytes, buf: Bytes) {
    cache.lock().await.insert(key, buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[quickcheck_async::tokio]
    async fn values_are_cached(keys: HashSet<Vec<u8>>) {
        if keys.is_empty() {
            return;
        }

        struct Src(Mutex<HashSet<Vec<u8>>>);

        #[crate::async_trait]
        impl Source for Src {
            async fn get(&self, key: &[u8]) -> Result<Vec<u8>, Status> {
                let mut seen = self.0.lock().await;
                assert!(seen.insert(key.to_vec()), "key was fetched twice :(");
                Ok(key.to_vec())
            }
        }

        let cache = Cache::new(keys.len(), Src(Mutex::default()));

        for key in keys.into_iter() {
            assert_eq!(key, cache.get(key.clone()).await.unwrap());
        }
    }
}
