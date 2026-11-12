//! One value per scheduler thread, each on its own cache line, so threads
//! working on their own values never contend.

use crate::ffi::scheduler_count;
use std::cell::Cell;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A scheduler's index into every `PerScheduler`. Only a `PerScheduler` makes
/// one, and every `PerScheduler` has one value per scheduler, so a shard from
/// one is in range for all of them.
#[derive(Clone, Copy)]
pub struct Shard(usize);

// Matches crossbeam_utils::CachePadded for x86_64 and aarch64
#[repr(align(128))]
struct CachePadded<T>(T);

/// One value per scheduler.
pub struct PerScheduler<T> {
    values: Box<[CachePadded<T>]>,
}

impl<T> PerScheduler<T> {
    pub fn new(mut make: impl FnMut() -> T) -> Self {
        let values = (0..scheduler_count().max(1))
            .map(|_| CachePadded(make()))
            .collect();
        PerScheduler { values }
    }

    /// The calling scheduler thread's shard.
    pub fn current(&self) -> Shard {
        Shard(thread_index() % self.values.len())
    }

    pub fn get(&self, shard: Shard) -> &T {
        &self.values[shard.0].0
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.values.iter().map(|value| &value.0)
    }

    pub fn shards(&self) -> impl Iterator<Item = (Shard, &T)> {
        self.values
            .iter()
            .enumerate()
            .map(|(index, value)| (Shard(index), &value.0))
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// The values' own size, padding included, but not anything they point to.
    pub fn memory(&self) -> usize {
        self.values.len() * size_of::<CachePadded<T>>()
    }
}

/// An index fixed per thread, so each scheduler thread keeps to its own value.
/// Chosen in the NIF rather than in `resolve/1`, because a process can migrate
/// to another scheduler between the two calls.
fn thread_index() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        static INDEX: Cell<Option<usize>> = const { Cell::new(None) };
    }

    INDEX.with(|index| {
        index.get().unwrap_or_else(|| {
            let assigned = NEXT.fetch_add(1, Ordering::Relaxed);
            index.set(Some(assigned));
            assigned
        })
    })
}
