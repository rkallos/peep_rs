//! Tags maps, stored once per scheduler and shared by every metric.
//!
//! Each scheduler has a tag table that stores each tags map inserted on it
//! once, under a `TagId`, and metrics key their series by that id. Ids are
//! never reused, so an id names one tags map for its table's lifetime, and a
//! prune keeps the survivors' ids.
//!
//! A table's lock also orders access to the metrics' series on its scheduler,
//! and the types here are the only way to reach those series:
//!
//! - An insert stores samples through a `Batch`, which holds the table's
//!   upgradable lock from interning a tags map until its samples are stored,
//!   so no prune can remove the id in between.
//! - A prune hands the metrics a `Doomed` set of ids while it holds the write
//!   lock, so no insert can store under an id the metrics have dropped.
//! - A scrape reads series through a `Reader`, which holds the read lock, so
//!   every id it meets is still in the table.
//!
//! The table is always locked before a metric's series, so these never
//! deadlock.

use crate::error::StorageError;
use crate::ffi::{terms_identical, TupleElements};
use crate::per_scheduler::{PerScheduler, Shard};
use crate::term_store::TermStore;
use hashbrown::hash_map::HashMap;
use hashbrown::HashSet;
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard};
use rustler::sys::ErlNifTerm;
use rustler::types::map::MapIterator;
use rustler::{Env, Term};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::mem::size_of;

/// Identifies a tags map within one scheduler's tag table.
pub type TagId = u64;

/// A map keyed by `TagId`.
pub type IdMap<V> = HashMap<TagId, V, IdHashBuilder>;

/// A map keyed by tags maps copied into a NIF call's environment.
pub type TagsMap<V> = HashMap<TagsKey, V, TermHashBuilder>;

/// The size of one entry of a hashbrown map. Counted per entry rather than by
/// allocated capacity, so that every new entry adds to `storage_size`.
pub fn entry_size<K, V>() -> usize {
    size_of::<(K, V)>() + 1
}

pub struct TagTables {
    tables: PerScheduler<RwLock<TagTable>>,
}

const _: () = assert!(size_of::<RwLock<TagTable>>() <= 128);

impl TagTables {
    pub fn new() -> Self {
        TagTables {
            tables: PerScheduler::new(|| RwLock::new(TagTable::new())),
        }
    }

    /// Starts storing one event's samples on the calling scheduler.
    /// `tag_results` is the event's tuple of tags maps.
    pub fn batch<'t, 'a>(&'t self, tag_results: Term<'a>) -> Result<Batch<'t, 'a>, StorageError> {
        let tags = TupleElements::new(tag_results)
            .ok_or(StorageError::BadArgument("tag_results must be a tuple"))?;
        let shard = self.tables.current();

        Ok(Batch {
            table: self.tables.get(shard).upgradable_read(),
            shard,
            tags,
            tag_ids: [None; 8],
        })
    }

    /// One reader per scheduler, each taken as the iterator reaches it.
    pub fn readers(&self) -> impl Iterator<Item = Reader<'_>> {
        self.tables.shards().map(|(shard, table)| Reader {
            table: table.read(),
            shard,
        })
    }

    /// Removes every tags map matching one of `patterns`, calling `remove` with
    /// each scheduler's matching ids so the metrics can drop their series.
    ///
    /// Matching holds only the read lock, so a table with no match never
    /// blocks inserts. Ids are never reused, so the matches still name the
    /// same tags maps once the write lock is held; one that a concurrent prune
    /// removed in between is skipped.
    pub fn prune(&self, patterns: &[Term], mut remove: impl FnMut(&Doomed)) {
        for (shard, table) in self.tables.shards() {
            let ids = table.read().matching(patterns);
            if ids.is_empty() {
                continue;
            }

            let doomed = Doomed { shard, ids };
            let mut table = table.write();
            remove(&doomed);
            table.remove(&doomed.ids);
        }
    }

    pub fn memory(&self) -> usize {
        self.tables.memory()
            + self
                .tables
                .iter()
                .map(|table| table.read().memory())
                .sum::<usize>()
    }

    pub fn len(&self) -> usize {
        self.tables.len()
    }
}

/// One event's samples being stored on one scheduler. Holds the scheduler's
/// tag table locked until dropped.
pub struct Batch<'t, 'a> {
    table: RwLockUpgradableReadGuard<'t, TagTable>,
    shard: Shard,
    tags: TupleElements<'a>,
    /// An event's metrics usually share a tags map, so each `tag_idx` is
    /// resolved to a `TagId` once. A `tag_idx` past the end is resolved per
    /// sample.
    tag_ids: [Option<TagId>; 8],
}

impl Batch<'_, '_> {
    pub fn shard(&self) -> Shard {
        self.shard
    }

    /// The id of the event's tags map at `tag_idx`, interning it if it is new.
    #[inline]
    pub fn tag_id(&mut self, tag_idx: usize) -> Result<TagId, StorageError> {
        match self.tag_ids.get(tag_idx) {
            Some(Some(tag_id)) => Ok(*tag_id),
            _ => self.resolve(tag_idx),
        }
    }

    /// Kept out of line, so a cached lookup doesn't pay for interning's
    /// register saves.
    #[cold]
    #[inline(never)]
    fn resolve(&mut self, tag_idx: usize) -> Result<TagId, StorageError> {
        let tags = self
            .tags
            .get(tag_idx)
            .ok_or(StorageError::BadTagIndex(tag_idx))?;
        let tag_id = self.intern(tags);

        if let Some(cached) = self.tag_ids.get_mut(tag_idx) {
            *cached = Some(tag_id);
        }
        Ok(tag_id)
    }

    fn intern(&mut self, tags: Term) -> TagId {
        let hash = tags.hash_internal(0);

        if let Some((_, tag_id)) = self
            .table
            .ids
            .raw_entry()
            .from_hash(hash, |key| key.matches(tags))
        {
            return *tag_id;
        }

        // Upgradable locks exclude each other, so no other insert has added
        // `tags` since the lookup. Upgrading never releases the lock, so no
        // prune runs before the batch's samples are stored.
        self.table.with_upgraded(|table| table.insert(hash, tags))
    }
}

/// One scheduler's tag table, locked for reading.
pub struct Reader<'a> {
    table: RwLockReadGuard<'a, TagTable>,
    shard: Shard,
}

impl Reader<'_> {
    pub fn shard(&self) -> Shard {
        self.shard
    }

    /// The tags map stored under `tag_id`, which is valid only while this
    /// reader lives.
    pub fn get(&self, tag_id: TagId) -> Option<TagsKey> {
        self.table.tags.get(&tag_id).copied()
    }

    /// Copies a tags map from `get` into `env`.
    pub fn copy(&self, env: Env, key: &TagsKey) -> TagsKey {
        TagsKey {
            hash: key.hash,
            term: self.table.store.copy_out(env, key.term).as_c_arg(),
        }
    }
}

/// The ids a prune is removing from one scheduler's tag table.
pub struct Doomed {
    shard: Shard,
    ids: HashSet<TagId, IdHashBuilder>,
}

impl Doomed {
    pub fn shard(&self) -> Shard {
        self.shard
    }

    pub fn contains(&self, tag_id: &TagId) -> bool {
        self.ids.contains(tag_id)
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &TagId> {
        self.ids.iter()
    }
}

/// A tags map's hash and term. The term is either stored in a tag table, and
/// valid while that table is locked, or copied into a NIF call's environment.
#[derive(Clone, Copy)]
pub struct TagsKey {
    hash: u64,
    term: ErlNifTerm,
}

impl TagsKey {
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Wraps rather than copies: sound only for a key `Reader::copy` copied
    /// into this same `env`.
    pub fn term<'a>(&self, env: Env<'a>) -> Term<'a> {
        unsafe { Term::new(env, self.term) }
    }

    /// Whether both keys are the same term, not merely equal ones.
    pub fn is(&self, other: &TagsKey) -> bool {
        self.term == other.term
    }

    fn matches(&self, tags: Term) -> bool {
        terms_identical(self.term, tags.as_c_arg())
    }
}

impl PartialEq for TagsKey {
    fn eq(&self, other: &Self) -> bool {
        terms_identical(self.term, other.term)
    }
}
impl Eq for TagsKey {}

impl Hash for TagsKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

/// The tags maps inserted on one scheduler.
struct TagTable {
    store: TermStore,
    ids: TagsMap<TagId>,
    tags: IdMap<TagsKey>,
    next_id: TagId,
}

impl TagTable {
    fn new() -> Self {
        TagTable {
            store: TermStore::new(),
            ids: HashMap::default(),
            tags: HashMap::default(),
            next_id: 0,
        }
    }

    fn insert(&mut self, hash: u64, tags: Term) -> TagId {
        let key = TagsKey {
            hash,
            term: self.store.store(tags.as_c_arg()),
        };
        let tag_id = self.next_id;
        self.next_id += 1;

        self.ids.insert(key, tag_id);
        self.tags.insert(tag_id, key);
        tag_id
    }

    /// Copies each stored tags map into the patterns' environment to match it.
    fn matching(&self, patterns: &[Term]) -> HashSet<TagId, IdHashBuilder> {
        let Some(env) = patterns.first().map(|pattern| pattern.get_env()) else {
            return HashSet::default();
        };

        self.tags
            .iter()
            .filter(|(_, key)| {
                let tags = self.store.copy_out(env, key.term);

                patterns.iter().any(|pattern| {
                    MapIterator::new(*pattern).is_some_and(|mut pairs| {
                        pairs.all(|(name, value)| {
                            tags.map_get(name).is_ok_and(|found| found == value)
                        })
                    })
                })
            })
            .map(|(tag_id, _)| *tag_id)
            .collect()
    }

    /// The store has no per-term free, so reclaiming means copying the
    /// survivors into a fresh one and dropping the old. Survivors keep their
    /// ids, so the metrics' entries for them stay valid.
    fn remove(&mut self, doomed: &HashSet<TagId, IdHashBuilder>) {
        let mut store = TermStore::new();
        let capacity = self.tags.len();
        let mut ids = HashMap::with_capacity_and_hasher(capacity, TermHashBuilder::default());
        let mut tags = HashMap::with_capacity_and_hasher(capacity, IdHashBuilder::default());

        for (tag_id, key) in self.tags.iter() {
            if doomed.contains(tag_id) {
                continue;
            }

            let moved = TagsKey {
                hash: key.hash,
                term: store.store(key.term),
            };
            ids.insert(moved, *tag_id);
            tags.insert(*tag_id, moved);
        }

        self.store = store;
        self.ids = ids;
        self.tags = tags;
    }

    fn memory(&self) -> usize {
        self.ids.len() * entry_size::<TagsKey, TagId>()
            + self.tags.len() * entry_size::<TagId, TagsKey>()
            + self
                .tags
                .values()
                .map(|key| self.store.size_of(key.term))
                .sum::<usize>()
    }
}

#[derive(Default)]
pub struct TermPassthroughHasher(u64);

impl Hasher for TermPassthroughHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("TagsKey is the only key type, and it hashes via write_u64")
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

pub type TermHashBuilder = BuildHasherDefault<TermPassthroughHasher>;

/// `TagId`s are sequential, so spread them across the bits hashbrown uses.
#[derive(Default)]
pub struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("TagId is the only key type, and it hashes via write_u64")
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

pub type IdHashBuilder = BuildHasherDefault<IdHasher>;
