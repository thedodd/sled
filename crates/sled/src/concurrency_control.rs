use std::convert::TryFrom;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::crc32;
use pagecache::FastSet8;

const N_SHARDS: usize = 32;

pub(crate) struct SharedLock<'a>(RwLockReadGuard<'a, ()>);

pub(crate) struct ExclusiveLock<'a>(Vec<RwLockWriteGuard<'a, ()>>);

#[derive(Default)]
pub(crate) struct ConcurrencyControl {
    shards: [RwLock<()>; N_SHARDS],
}

impl ConcurrencyControl {
    pub(crate) fn pessimistic_shared<'a>(
        &'a self,
        key: &[u8],
    ) -> SharedLock<'a> {
        let shard = key_to_shard(key);
        let guard = self.shards[shard].read();
        SharedLock(guard)
    }

    pub(crate) fn pessimistic_exclusive_tree<'a>(
        &'a self,
    ) -> ExclusiveLock<'a> {
        let guards = self.shards.iter().map(|s| s.write()).collect();
        ExclusiveLock(guards)
    }

    pub(crate) fn pessimistic_exclusive<'a, 'b, T>(
        &'a self,
        keys: T,
    ) -> ExclusiveLock<'a>
    where
        T: IntoIterator<Item = &'b [u8]>,
    {
        // hash the keys and deduplicate them
        let shard_indices: FastSet8<usize> =
            keys.into_iter().map(key_to_shard).collect();

        // sort them to avoid deadlocks on acquisition
        let mut shard_index_vec: Vec<usize> =
            shard_indices.into_iter().collect();
        shard_index_vec.sort_unstable();

        // acquire locks
        let guards = shard_index_vec
            .into_iter()
            .map(|shard_idx| self.shards[shard_idx].write())
            .collect();

        ExclusiveLock(guards)
    }
}

fn key_to_shard(key: &[u8]) -> usize {
    let hash: u32 = crc32(key);
    usize::try_from(hash).unwrap() % N_SHARDS
}
