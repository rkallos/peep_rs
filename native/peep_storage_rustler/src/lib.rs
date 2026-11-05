// Never panic; handle our own errors, so we avoid poisoning write locks. Errors
// should raise exceptions and detach :telemetry handlers.
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented
)]

use hashbrown::hash_map::{Entry, HashMap, RawEntryMut};
use hashbrown::HashSet;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use rustler::env::OwnedEnv;
use rustler::sys::{
    enif_get_double, enif_is_identical, enif_make_copy, enif_monotonic_time,
    enif_system_info, ErlNifSysInfo, ErlNifTimeUnit,
};
use rustler::types::map::MapIterator;
use rustler::types::tuple::get_tuple;
use rustler::wrapper::NIF_TERM;
use rustler::{Atom, Encoder, Env, NifMap, Resource, ResourceArc, Term, TermType};
use std::cell::Cell;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::mem::size_of;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

mod atoms {
    rustler::atoms! {
        metric_counter = "Elixir.Telemetry.Metrics.Counter",
        metric_sum = "Elixir.Telemetry.Metrics.Sum",
        metric_last_value = "Elixir.Telemetry.Metrics.LastValue",
        metric_distribution = "Elixir.Telemetry.Metrics.Distribution",
        peep_buckets,
        infinity,
        sum,
        peep_storage_error,
        bad_tags_map,
        bad_argument,
        already_registered,
        not_registered,
        unknown_metric_id,
        bad_tag_index,
        bad_measurement,
        map_build_failed,
    }
}

///////////////////////////////////////////////////////////////////////////////
//                                   unsafe                                  //
///////////////////////////////////////////////////////////////////////////////

fn scheduler_count() -> usize {
    let mut info: ErlNifSysInfo = unsafe { std::mem::zeroed() };
    unsafe {
        enif_system_info(&mut info, size_of::<ErlNifSysInfo>());
    }
    info.scheduler_threads as usize
}

/// A shard index fixed per scheduler thread, so each shard has one inserting
/// thread. Chosen here rather than in `resolve/1`, because a process can
/// migrate to another scheduler between the two calls.
fn thread_shard() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        static SHARD: Cell<Option<usize>> = const { Cell::new(None) };
    }

    SHARD.with(|shard| {
        shard.get().unwrap_or_else(|| {
            let assigned = NEXT.fetch_add(1, Ordering::Relaxed);
            shard.set(Some(assigned));
            assigned
        })
    })
}

fn monotonic_time_ns() -> i64 {
    unsafe { enif_monotonic_time(ErlNifTimeUnit::ERL_NIF_NSEC) }
}

struct TagsEnv(OwnedEnv);

// SAFETY: a tag table's `RwLock` guards its `TagsEnv`, and the bucket labels'
// `TagsEnv` is written only during registration, before `OnceLock` publishes
// it. `store` is the only operation that allocates and takes `&mut self`, so it
// never runs while a stored term is being read.
unsafe impl Sync for TagsEnv {}

impl TagsEnv {
    fn new() -> Self {
        TagsEnv(OwnedEnv::new())
    }

    fn store(&mut self, term: NIF_TERM) -> NIF_TERM {
        self.0.run(|owned| unsafe { enif_make_copy(owned.as_c_arg(), term) })
    }

    fn copy_out<'a>(&self, env: Env<'a>, term: NIF_TERM) -> Term<'a> {
        unsafe { Term::new(env, enif_make_copy(env.as_c_arg(), term)) }
    }

    fn copy_key(&self, env: Env, key: &TagsKey) -> TagsKey {
        TagsKey {
            hash: key.hash,
            term: self.copy_out(env, key.term).as_c_arg(),
        }
    }

    fn size_of(&self, term: NIF_TERM) -> usize {
        self.0.run(|owned| unsafe { Term::new(owned, term) }.size())
    }
}

fn terms_identical(a: NIF_TERM, b: NIF_TERM) -> bool {
    a == b || unsafe { enif_is_identical(a, b) == 1 }
}

/// Wraps rather than copies: sound only for a key `TagsEnv::copy_key` produced
/// for this same `env`. A key still owned by a tag table needs
/// `TagsEnv::copy_out`.
fn tags_term<'a>(env: Env<'a>, key: &TagsKey) -> Term<'a> {
    unsafe { Term::new(env, key.term) }
}

// rustler's f64 decoder falls back to integer decoding.
// We don't want this behavior in Measurement::decode().
// 5 and 5.0 must not be stored as the same value.
fn term_as_f64(term: Term) -> Option<f64> {
    let mut value = 0f64;
    let found =
        unsafe { enif_get_double(term.get_env().as_c_arg(), term.as_c_arg(), &mut value) } == 1;
    found.then_some(value)
}

struct TupleElements<'a> {
    env: Env<'a>,
    elements: &'a [NIF_TERM],
}

impl<'a> TupleElements<'a> {
    fn new(tuple: Term<'a>) -> Option<Self> {
        let env = tuple.get_env();
        let elements =
            unsafe { rustler::wrapper::tuple::get_tuple(env.as_c_arg(), tuple.as_c_arg()) }.ok()?;
        Some(TupleElements { env, elements })
    }

    fn get(&self, index: usize) -> Option<Term<'a>> {
        let element = *self.elements.get(index)?;
        Some(unsafe { Term::new(self.env, element) })
    }
}

///////////////////////////////////////////////////////////////////////////////
//                                   types                                   //
///////////////////////////////////////////////////////////////////////////////

struct Storage {
    registered: OnceLock<RegisteredMetrics>,
}

#[rustler::resource_impl]
impl Resource for Storage {}

// parking_lot doesn't poison, so it can't assert this for us. Nothing inside a
// critical section panics, so a guard can't drop mid-unwind.
impl std::panic::RefUnwindSafe for Storage {}

struct RegisteredMetrics {
    metrics: Vec<MetricSlot>,
    /// One per scheduler, indexed like every metric's shards.
    tag_tables: Box<[CachePadded<RwLock<TagTable>>]>,
    boundaries_arena: Box<[f64]>,
    /// Parallel to `boundaries_arena`: each bucket's label, owned by `labels_env`.
    labels_arena: Box<[NIF_TERM]>,
    labels_env: TagsEnv,
}

enum MetricSlot {
    Counter(Shards<AtomicI64>),
    Sum(Shards<AtomicI64>),
    LastValue(Shards<LastValueCell>),
    Distribution {
        /// Offset and length into `boundaries_arena` and `labels_arena`.
        boundaries: (usize, usize),
        shards: Shards<DistributionCell>,
    },
}

// Matches crossbeam_utils::CachePadded for x86_64 and aarch64
#[repr(align(128))]
struct CachePadded<T>(T);

/// Identifies a tags map within one `TagTable`.
type TagId = u64;

type IdMap<V> = HashMap<TagId, V, IdHashBuilder>;

/// The tags maps inserted on one scheduler, stored once for every metric.
/// Metric shards key their series by `TagId`.
///
/// Lock a tag table before any metric shard. An insert holds its table's
/// upgradable lock for the whole batch, and a prune holds the write lock while
/// it removes ids from the metrics, so an insert never stores under an id that
/// a prune has removed.
struct TagTable {
    env: TagsEnv,
    ids: HashMap<TagsKey, TagId, TermHashBuilder>,
    tags: IdMap<TagsKey>,
    /// Ids are never reused, so an id names one tags map for the table's
    /// lifetime.
    next_id: TagId,
}

struct Shards<V> {
    shards: Box<[CachePadded<RwLock<IdMap<V>>>]>,
}

const _: () = assert!(size_of::<RwLock<TagTable>>() <= 128);
const _: () = assert!(size_of::<RwLock<IdMap<AtomicI64>>>() <= 128);

type LastValueCell = Mutex<(i64, Measurement)>;

struct DistributionCell {
    buckets: Box<[AtomicU64]>,
    sum: AtomicI64,
}

///////////////////////////////////////////////////////////////////////////////
//                                   errors                                  //
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum StorageError {
    BadTagsMap,
    BadArgument(&'static str),
    BadMeasurement(&'static str),
    MapBuildFailed(&'static str),
    UnknownMetricId(usize),
    BadTagIndex(usize),
    AlreadyRegistered,
    NotRegistered,
}

impl StorageError {
    fn reason(&self) -> Atom {
        match self {
            StorageError::BadTagsMap => atoms::bad_tags_map(),
            StorageError::BadArgument(_) => atoms::bad_argument(),
            StorageError::BadMeasurement(_) => atoms::bad_measurement(),
            StorageError::MapBuildFailed(_) => atoms::map_build_failed(),
            StorageError::UnknownMetricId(_) => atoms::unknown_metric_id(),
            StorageError::BadTagIndex(_) => atoms::bad_tag_index(),
            StorageError::AlreadyRegistered => atoms::already_registered(),
            StorageError::NotRegistered => atoms::not_registered(),
        }
    }

    fn detail(&self) -> String {
        match self {
            StorageError::BadTagsMap => "tags must be a map".into(),
            StorageError::BadArgument(detail) => (*detail).into(),
            StorageError::BadMeasurement(detail) => (*detail).into(),
            StorageError::MapBuildFailed(detail) => (*detail).into(),
            StorageError::UnknownMetricId(id) => format!("no metric registered for id {id}"),
            StorageError::BadTagIndex(idx) => format!("no tags map at index {idx}"),
            StorageError::AlreadyRegistered => "register_metrics was already called".into(),
            StorageError::NotRegistered => "register_metrics has not been called".into(),
        }
    }
}

impl Encoder for StorageError {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        (atoms::peep_storage_error(), self.reason(), self.detail()).encode(env)
    }
}

impl From<StorageError> for rustler::Error {
    fn from(error: StorageError) -> Self {
        rustler::Error::RaiseTerm(Box::new(error))
    }
}

///////////////////////////////////////////////////////////////////////////////
//                                    new                                    //
///////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
fn new(_opts: Term) -> ResourceArc<Storage> {
    ResourceArc::new(Storage {
        registered: OnceLock::new(),
    })
}

///////////////////////////////////////////////////////////////////////////////
//                              register_metrics                             //
///////////////////////////////////////////////////////////////////////////////

// Copies bucket labels into an environment of their own.
#[rustler::nif(schedule = "DirtyCpu")]
fn register_metrics(storage: &Storage, ids_to_metrics: Term) -> Result<Atom, rustler::Error> {
    let n_shards = scheduler_count().max(1);
    let mut layouts = BucketLayouts::new();

    let metrics = get_tuple(ids_to_metrics)
        .map_err(|_| StorageError::BadArgument("ids_to_metrics must be a tuple"))?
        .into_iter()
        .map(|metric| metric_slot_for(metric, &mut layouts, n_shards))
        .collect::<Result<_, _>>()?;

    let registered = RegisteredMetrics {
        metrics,
        tag_tables: (0..n_shards)
            .map(|_| CachePadded(RwLock::new(TagTable::new())))
            .collect(),
        boundaries_arena: layouts.boundaries.into_boxed_slice(),
        labels_arena: layouts.labels.into_boxed_slice(),
        labels_env: layouts.labels_env,
    };

    storage
        .registered
        .set(registered)
        .map_err(|_| StorageError::AlreadyRegistered)?;

    Ok(rustler::types::atom::ok())
}

fn metric_slot_for(
    metric: Term,
    layouts: &mut BucketLayouts,
    n_shards: usize,
) -> Result<MetricSlot, StorageError> {
    let struct_name: Atom = metric
        .map_get(rustler::types::atom::__struct__())
        .map_err(|_| StorageError::BadArgument("metric must be a struct"))?
        .decode()
        .map_err(|_| StorageError::BadArgument("__struct__ must be an atom"))?;

    if struct_name == atoms::metric_counter() {
        Ok(MetricSlot::Counter(Shards::new(n_shards)))
    } else if struct_name == atoms::metric_sum() {
        Ok(MetricSlot::Sum(Shards::new(n_shards)))
    } else if struct_name == atoms::metric_last_value() {
        Ok(MetricSlot::LastValue(Shards::new(n_shards)))
    } else if struct_name == atoms::metric_distribution() {
        // Peep sorts these by bound, with no repeated bound or label.
        let buckets: Vec<(f64, Term)> = metric
            .map_get(atoms::peep_buckets())
            .map_err(|_| StorageError::BadArgument("Distribution metric must carry :peep_buckets"))?
            .decode()
            .map_err(|_| {
                StorageError::BadArgument(":peep_buckets must be a list of {bound, label} tuples")
            })?;

        Ok(MetricSlot::Distribution {
            boundaries: layouts.intern(&buckets),
            shards: Shards::new(n_shards),
        })
    } else {
        Err(StorageError::BadArgument("unrecognized metric struct"))
    }
}

/// Distributions usually share a bucket layout, so each distinct layout is
/// stored once.
struct BucketLayouts {
    boundaries: Vec<f64>,
    labels: Vec<NIF_TERM>,
    labels_env: TagsEnv,
    interned: Vec<(usize, usize)>,
}

impl BucketLayouts {
    fn new() -> Self {
        BucketLayouts {
            boundaries: Vec::new(),
            labels: Vec::new(),
            labels_env: TagsEnv::new(),
            interned: Vec::new(),
        }
    }

    fn intern(&mut self, buckets: &[(f64, Term)]) -> (usize, usize) {
        let matches = |&(offset, len): &(usize, usize)| {
            len == buckets.len()
                && buckets.iter().enumerate().all(|(idx, (bound, label))| {
                    self.boundaries[offset + idx] == *bound
                        && terms_identical(self.labels[offset + idx], label.as_c_arg())
                })
        };

        if let Some(layout) = self.interned.iter().find(|layout| matches(layout)) {
            return *layout;
        }

        let layout = (self.boundaries.len(), buckets.len());
        for (bound, label) in buckets {
            self.boundaries.push(*bound);
            self.labels.push(self.labels_env.store(label.as_c_arg()));
        }
        self.interned.push(layout);
        layout
    }
}

///////////////////////////////////////////////////////////////////////////////
//                               insert_metrics                              //
///////////////////////////////////////////////////////////////////////////////
/// `metric` is unused here; it stays in the item shape because the ETS backends
/// dispatch on it.
#[rustler::nif]
fn insert_metrics(
    storage: &Storage,
    tag_results: Term,
    batch: Term,
) -> Result<Atom, rustler::Error> {
    let registered = storage.registered.get().ok_or(StorageError::NotRegistered)?;
    let shard_id = thread_shard() % registered.tag_tables.len();

    let tag_terms = TupleElements::new(tag_results)
        .ok_or(StorageError::BadArgument("tag_results must be a tuple"))?;

    let batch = batch
        .into_list_iterator()
        .map_err(|_| StorageError::BadArgument("batch must be a list"))?;

    let mut table = registered.tag_tables[shard_id].0.upgradable_read();

    // A batch's metrics usually share a tags map, so each `tag_idx` is resolved
    // to a `TagId` once. A `tag_idx` past the end is resolved per sample.
    let mut tag_ids: [Option<TagId>; 8] = [None; 8];

    for item in batch {
        // Avoids `Vec<Term>` allocations by `rustler::types::tuple::get_tuple`
        let (id, _metric, value, tag_idx): (usize, Term, Term, usize) =
            item.decode().map_err(|_| {
                StorageError::BadArgument("batch items must be {id, metric, value, tag_idx} tuples")
            })?;

        let tag_id = || {
            if let Some(Some(tag_id)) = tag_ids.get(tag_idx) {
                return Ok(*tag_id);
            }

            let tags = tag_terms
                .get(tag_idx)
                .ok_or(StorageError::BadTagIndex(tag_idx))?;
            let tag_id = intern(&mut table, tags);

            if let Some(cached) = tag_ids.get_mut(tag_idx) {
                *cached = Some(tag_id);
            }
            Ok(tag_id)
        };

        store_one(registered, shard_id, id, value, tag_id)?;
    }

    Ok(rustler::types::atom::ok())
}

fn intern(table: &mut RwLockUpgradableReadGuard<TagTable>, tags: Term) -> TagId {
    let hash = tags.hash_internal(0);

    if let Some((_, tag_id)) = table.ids.raw_entry().from_hash(hash, |key| key.matches(tags)) {
        return *tag_id;
    }

    // Upgradable locks exclude each other, so no other insert has added `tags`
    // since the lookup. Upgrading never releases the lock, so no prune runs
    // before the batch's samples are stored.
    table.with_upgraded(|table| table.insert(hash, tags))
}

/// Resolves the sample's `TagId` only once its value is valid, so a rejected
/// sample stores no tags.
#[inline]
fn store_one(
    registered: &RegisteredMetrics,
    shard_id: usize,
    id: usize,
    value: Term,
    tag_id: impl FnOnce() -> Result<TagId, StorageError>,
) -> Result<(), StorageError> {
    match registered.metrics.get(id) {
        Some(MetricSlot::Counter(shards)) => {
            shards.add(shard_id, tag_id()?, 1);
            Ok(())
        }
        Some(MetricSlot::Sum(shards)) => {
            let delta = value
                .decode()
                .map_err(|_| StorageError::BadMeasurement("sum value must be an integer"))?;

            shards.add(shard_id, tag_id()?, delta);
            Ok(())
        }
        Some(MetricSlot::LastValue(shards)) => {
            let value = Measurement::decode(value)?;
            let sample = (monotonic_time_ns(), value);
            let make = LastValueCell::new;
            let apply = |cell: &LastValueCell, sample: (i64, Measurement)| {
                let mut current = cell.lock();
                if sample.0 > current.0 {
                    *current = sample;
                }
            };

            shards.upsert(shard_id, tag_id()?, sample, make, apply);
            Ok(())
        }
        Some(MetricSlot::Distribution { boundaries, shards }) => {
            let (offset, len) = *boundaries;
            let boundaries = &registered.boundaries_arena[offset..offset + len];
            let measurement = Measurement::decode(value)?;
            let sample = (measurement.as_f64(), measurement.rounded()?);
            let make = |sample: (f64, i64)| {
                let cell = DistributionCell::new(len);
                cell.record(boundaries, sample);
                cell
            };
            let apply = |cell: &DistributionCell, sample: (f64, i64)| {
                cell.record(boundaries, sample);
            };

            shards.upsert(shard_id, tag_id()?, sample, make, apply);
            Ok(())
        }
        None => Err(StorageError::UnknownMetricId(id)),
    }
}

///////////////////////////////////////////////////////////////////////////////
//                                storage_size                               //
///////////////////////////////////////////////////////////////////////////////

#[derive(NifMap)]
struct StorageSize {
    size: usize,
    memory: usize,
}

#[rustler::nif(schedule = "DirtyCpu")]
fn storage_size(storage: &Storage) -> StorageSize {
    let Some(registered) = storage.registered.get() else {
        return StorageSize { size: 0, memory: 0 };
    };

    let mut size = 0;
    let mut memory = size_of::<RegisteredMetrics>()
        + registered.boundaries_arena.len() * size_of::<f64>()
        + registered.labels_arena.len() * size_of::<NIF_TERM>()
        + registered.tag_tables.len() * size_of::<CachePadded<RwLock<TagTable>>>();

    for label in registered.labels_arena.iter() {
        memory += registered.labels_env.size_of(*label);
    }

    for table in registered.tag_tables.iter() {
        let table = table.0.read();
        memory += table.ids.len() * entry_size::<TagsKey, TagId>()
            + table.tags.len() * entry_size::<TagId, TagsKey>();
        for key in table.tags.values() {
            memory += table.env.size_of(key.term);
        }
    }

    for slot in &registered.metrics {
        let (slot_size, slot_memory) = match slot {
            MetricSlot::Counter(shards) | MetricSlot::Sum(shards) => {
                shards_size_and_memory(shards, |_| 0)
            }
            MetricSlot::LastValue(shards) => shards_size_and_memory(shards, |_| 0),
            MetricSlot::Distribution { shards, .. } => {
                shards_size_and_memory(shards, |cell| cell.buckets.len() * size_of::<AtomicU64>())
            }
        };

        size += slot_size;
        memory += slot_memory;
    }

    StorageSize { size, memory }
}

/// Counted per entry rather than by allocated capacity, so that every new
/// series adds to the total. The `1` is hashbrown's control byte.
fn entry_size<K, V>() -> usize {
    size_of::<(K, V)>() + 1
}

fn shards_size_and_memory<V>(
    shards: &Shards<V>,
    value_heap_size: impl Fn(&V) -> usize,
) -> (usize, usize) {
    let mut total_size = 0;
    let mut total_memory = shards.len() * size_of::<CachePadded<RwLock<IdMap<V>>>>();
    for shard in shards.iter() {
        let map = shard.read();
        total_size += map.len();
        total_memory += map.len() * entry_size::<TagId, V>();
        for value in map.values() {
            total_memory += value_heap_size(value);
        }
    }
    (total_size, total_memory)
}

///////////////////////////////////////////////////////////////////////////////
//                              get_all_metrics                              //
///////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyCpu")]
fn nif_get_all_metrics<'a>(
    storage: &Storage,
    ids_to_metrics: Term<'a>,
) -> Result<Term<'a>, rustler::Error> {
    let env = ids_to_metrics.get_env();

    let Some(registered) = storage.registered.get() else {
        return Ok(Term::map_new(env));
    };

    let metric_terms = get_tuple(ids_to_metrics)
        .map_err(|_| StorageError::BadArgument("ids_to_metrics must be a tuple"))?;

    let mut copies: Vec<TagCopies> =
        registered.tag_tables.iter().map(|_| TagCopies::default()).collect();

    let mut outer_keys = Vec::new();
    let mut outer_vals = Vec::new();

    for (metric_term, slot) in metric_terms.iter().zip(&registered.metrics) {
        let (len, inner) = match slot {
            MetricSlot::Counter(shards) | MetricSlot::Sum(shards) => {
                encode_counter_map(env, registered, &mut copies, shards)?
            }
            MetricSlot::LastValue(shards) => {
                encode_last_value_map(env, registered, &mut copies, shards)?
            }
            MetricSlot::Distribution { boundaries, shards } => {
                encode_distribution_map(env, registered, &mut copies, *boundaries, shards)?
            }
        };

        if len > 0 {
            outer_keys.push(*metric_term);
            outer_vals.push(inner);
        }
    }

    let map = Term::map_from_term_arrays(env, &outer_keys, &outer_vals)
        .map_err(|_| StorageError::MapBuildFailed("duplicate metric definitions"))?;

    Ok(map)
}

fn combined_capacity<V>(shards: &Shards<V>) -> usize {
    shards.iter().map(|shard| shard.read().len()).max().unwrap_or(0)
}

/// For one shard, the caller-environment copy of each tags map a scrape has
/// met so far, shared by every metric the scrape encodes. Ids are never
/// reused, so a copy stays correct even if a prune runs between two metrics.
type TagCopies = IdMap<TagsKey>;

/// The tag table's read lock is held for each shard's whole pass, which is
/// what makes `copy_key` sound: a key never leaves its table uncopied.
///
/// A tags map present on several shards is copied once per shard, and all
/// but the first copy are then replaced by the first. That one copy keys the
/// tags map in every metric's output, and finding it in `combined` takes a
/// pointer comparison rather than `enif_is_identical`.
fn merge_shards<V, A>(
    env: Env,
    registered: &RegisteredMetrics,
    copies: &mut [TagCopies],
    shards: &Shards<V>,
    init: impl Fn(&V) -> A,
    merge: impl Fn(&mut A, &V),
) -> HashMap<TagsKey, A, TermHashBuilder> {
    let mut combined =
        HashMap::with_capacity_and_hasher(combined_capacity(shards), TermHashBuilder::default());

    let tables = registered.tag_tables.iter();

    for ((table, shard), copies) in tables.zip(shards.iter()).zip(copies.iter_mut()) {
        let table = table.0.read();
        let map = shard.read();

        for (tag_id, cell) in map.iter() {
            let copy = copies.get(tag_id).copied();
            let key = match copy {
                Some(copy) => copy,
                // Pruning removes an id from the metrics and the table
                // together, under the table's write lock, so this never skips.
                None => match table.tags.get(tag_id) {
                    Some(key) => *key,
                    None => continue,
                },
            };

            match combined.raw_entry_mut().from_hash(key.hash, |seen: &TagsKey| *seen == key) {
                RawEntryMut::Occupied(mut entry) => {
                    if copy.map(|copy| copy.term) != Some(entry.key().term) {
                        copies.insert(*tag_id, *entry.key());
                    }
                    merge(entry.get_mut(), cell);
                }
                RawEntryMut::Vacant(entry) => {
                    let copy = copy.unwrap_or_else(|| {
                        let copy = table.env.copy_key(env, &key);
                        copies.insert(*tag_id, copy);
                        copy
                    });
                    entry.insert(copy, init(cell));
                }
            }
        }
    }

    combined
}

fn encode_merged<'a, A>(
    env: Env<'a>,
    combined: &HashMap<TagsKey, A, TermHashBuilder>,
    label: &'static str,
    mut encode: impl FnMut(Env<'a>, &A) -> Result<Term<'a>, StorageError>,
) -> Result<(usize, Term<'a>), StorageError> {
    let mut keys = Vec::with_capacity(combined.len());
    let mut vals = Vec::with_capacity(combined.len());

    for (key, value) in combined {
        keys.push(tags_term(env, key));
        vals.push(encode(env, value)?);
    }

    let map = Term::map_from_term_arrays(env, &keys, &vals)
        .map_err(|_| StorageError::MapBuildFailed(label))?;

    Ok((combined.len(), map))
}

fn encode_counter_map<'a>(
    env: Env<'a>,
    registered: &RegisteredMetrics,
    copies: &mut [TagCopies],
    counters: &Shards<AtomicI64>,
) -> Result<(usize, Term<'a>), StorageError> {
    let combined = merge_shards(
        env,
        registered,
        copies,
        counters,
        |cell: &AtomicI64| cell.load(Ordering::Relaxed),
        |total, cell| *total = total.wrapping_add(cell.load(Ordering::Relaxed)),
    );

    encode_merged(env, &combined, "counter map", |env, total| {
        Ok(total.encode(env))
    })
}

fn encode_last_value_map<'a>(
    env: Env<'a>,
    registered: &RegisteredMetrics,
    copies: &mut [TagCopies],
    last_values: &Shards<LastValueCell>,
) -> Result<(usize, Term<'a>), StorageError> {
    let combined = merge_shards(
        env,
        registered,
        copies,
        last_values,
        |cell: &LastValueCell| *cell.lock(),
        |newest, cell| {
            let sample = *cell.lock();
            if sample.0 > newest.0 {
                *newest = sample;
            }
        },
    );

    encode_merged(env, &combined, "last_value map", |env, (_, value)| {
        Ok(value.encode(env))
    })
}

fn encode_distribution_map<'a>(
    env: Env<'a>,
    registered: &RegisteredMetrics,
    copies: &mut [TagCopies],
    (offset, len): (usize, usize),
    distributions: &Shards<DistributionCell>,
) -> Result<(usize, Term<'a>), StorageError> {
    let combined = merge_shards(
        env,
        registered,
        copies,
        distributions,
        |cell: &DistributionCell| {
            let buckets: Vec<u64> =
                cell.buckets.iter().map(|c| c.load(Ordering::Relaxed)).collect();
            (buckets, cell.sum.load(Ordering::Relaxed))
        },
        |(buckets, sum), cell| {
            for (bucket, cell) in buckets.iter_mut().zip(cell.buckets.iter()) {
                *bucket = bucket.wrapping_add(cell.load(Ordering::Relaxed));
            }
            *sum = sum.wrapping_add(cell.sum.load(Ordering::Relaxed));
        },
    );

    if combined.is_empty() {
        return Ok((0, Term::map_new(env)));
    }

    // One key per bucket, in bucket order, then `:sum`. Every series of this
    // metric shares these keys.
    let mut keys: Vec<Term> = registered.labels_arena[offset..offset + len]
        .iter()
        .map(|label| registered.labels_env.copy_out(env, *label))
        .collect();
    keys.push(atoms::infinity().encode(env));
    keys.push(atoms::sum().encode(env));

    let mut vals = Vec::with_capacity(keys.len());

    encode_merged(env, &combined, "distribution map", |env, (buckets, sum)| {
        vals.clear();
        vals.extend(buckets.iter().map(|count| count.encode(env)));
        vals.push(sum.encode(env));

        Term::map_from_term_arrays(env, &keys, &vals)
            .map_err(|_| StorageError::MapBuildFailed("distribution bucket map"))
    })
}

///////////////////////////////////////////////////////////////////////////////
//                                 prune_tags                                //
///////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyCpu")]
fn prune_tags(storage: &Storage, patterns: Term) -> Result<Atom, rustler::Error> {
    if let Some(registered) = storage.registered.get() {
        let env = patterns.get_env();
        let patterns: Vec<Term> = patterns
            .decode()
            .map_err(|_| StorageError::BadArgument("patterns must be a list"))?;

        if patterns.iter().any(|pattern| !pattern.is_map()) {
            return Err(StorageError::BadTagsMap.into());
        }

        for (shard_id, table) in registered.tag_tables.iter().enumerate() {
            prune_shard(env, registered, shard_id, &mut table.0.write(), &patterns);
        }
    }

    Ok(rustler::types::atom::ok())
}

/// The table's write lock is held throughout, so no insert can store under a
/// removed id after the metrics have dropped it.
fn prune_shard<'a>(
    env: Env<'a>,
    registered: &RegisteredMetrics,
    shard_id: usize,
    table: &mut TagTable,
    patterns: &[Term<'a>],
) {
    let doomed: HashSet<TagId, IdHashBuilder> = table
        .tags
        .iter()
        .filter(|(_, key)| matches_any_pattern(env, &table.env, key, patterns))
        .map(|(tag_id, _)| *tag_id)
        .collect();

    if doomed.is_empty() {
        return;
    }

    for slot in &registered.metrics {
        match slot {
            MetricSlot::Counter(shards) | MetricSlot::Sum(shards) => {
                shards.remove(shard_id, &doomed)
            }
            MetricSlot::LastValue(shards) => shards.remove(shard_id, &doomed),
            MetricSlot::Distribution { shards, .. } => shards.remove(shard_id, &doomed),
        }
    }

    table.remove(&doomed);
}

fn matches_any_pattern<'a>(
    env: Env<'a>,
    tags_env: &TagsEnv,
    key: &TagsKey,
    patterns: &[Term<'a>],
) -> bool {
    let tags = tags_env.copy_out(env, key.term);

    patterns.iter().any(|pattern| {
        MapIterator::new(*pattern).is_some_and(|mut pairs| {
            pairs.all(|(name, value)| tags.map_get(name).is_ok_and(|found| found == value))
        })
    })
}

///////////////////////////////////////////////////////////////////////////////
//                                   shards                                  //
///////////////////////////////////////////////////////////////////////////////

impl TagTable {
    fn new() -> Self {
        TagTable {
            env: TagsEnv::new(),
            ids: HashMap::default(),
            tags: HashMap::default(),
            next_id: 0,
        }
    }

    fn insert(&mut self, hash: u64, tags: Term) -> TagId {
        let key = TagsKey {
            hash,
            term: self.env.store(tags.as_c_arg()),
        };
        let tag_id = self.next_id;
        self.next_id += 1;

        self.ids.insert(key, tag_id);
        self.tags.insert(tag_id, key);
        tag_id
    }

    /// An environment has no per-term free, so reclaiming means copying the
    /// survivors into a fresh one and dropping the old. Survivors keep their
    /// ids, so the metrics' entries for them stay valid.
    fn remove(&mut self, doomed: &HashSet<TagId, IdHashBuilder>) {
        let mut env = TagsEnv::new();
        let capacity = self.tags.len();
        let mut ids = HashMap::with_capacity_and_hasher(capacity, TermHashBuilder::default());
        let mut tags = HashMap::with_capacity_and_hasher(capacity, IdHashBuilder::default());

        for (tag_id, key) in self.tags.iter() {
            if doomed.contains(tag_id) {
                continue;
            }

            let moved = TagsKey {
                hash: key.hash,
                term: env.store(key.term),
            };
            ids.insert(moved, *tag_id);
            tags.insert(*tag_id, moved);
        }

        self.env = env;
        self.ids = ids;
        self.tags = tags;
    }
}

impl<V> Shards<V> {
    fn new(n_shards: usize) -> Self {
        let shards = (0..n_shards)
            .map(|_| CachePadded(RwLock::new(HashMap::default())))
            .collect();
        Shards { shards }
    }

    fn iter(&self) -> impl Iterator<Item = &RwLock<IdMap<V>>> {
        self.shards.iter().map(|shard| &shard.0)
    }

    fn len(&self) -> usize {
        self.shards.len()
    }

    fn shard(&self, shard_id: usize) -> &RwLock<IdMap<V>> {
        &self.shards[shard_id].0
    }

    /// Exactly one of `make` and `apply` runs.
    fn upsert<S>(
        &self,
        shard_id: usize,
        tag_id: TagId,
        sample: S,
        make: impl FnOnce(S) -> V,
        apply: impl FnOnce(&V, S),
    ) {
        let shard_lock = self.shard(shard_id);

        // Fast path.
        if let Some(value) = shard_lock.read().get(&tag_id) {
            apply(value, sample);
            return;
        }

        match shard_lock.write().entry(tag_id) {
            Entry::Occupied(entry) => apply(entry.get(), sample),
            Entry::Vacant(entry) => {
                entry.insert(make(sample));
            }
        }
    }

    fn remove(&self, shard_id: usize, doomed: &HashSet<TagId, IdHashBuilder>) {
        self.shard(shard_id).write().retain(|tag_id, _| !doomed.contains(tag_id));
    }
}

impl Shards<AtomicI64> {
    #[inline]
    fn add(&self, shard_id: usize, tag_id: TagId, delta: i64) {
        self.upsert(
            shard_id,
            tag_id,
            delta,
            AtomicI64::new,
            |counter: &AtomicI64, delta| {
                counter.fetch_add(delta, Ordering::Relaxed);
            },
        );
    }
}

impl DistributionCell {
    fn new(num_boundaries: usize) -> Self {
        let buckets: Box<[AtomicU64]> = (0..=num_boundaries).map(|_| AtomicU64::new(0)).collect();
        DistributionCell {
            buckets,
            sum: AtomicI64::new(0),
        }
    }

    fn record(&self, boundaries: &[f64], (value, rounded): (f64, i64)) {
        let idx = boundaries.partition_point(|&boundary| boundary <= value);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(rounded, Ordering::Relaxed);
    }
}

///////////////////////////////////////////////////////////////////////////////
//                               caching tags                                //
///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct TermPassthroughHasher(u64);

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

type TermHashBuilder = BuildHasherDefault<TermPassthroughHasher>;

/// `TagId`s are sequential, so spread them across the bits hashbrown uses.
#[derive(Default)]
struct IdHasher(u64);

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

type IdHashBuilder = BuildHasherDefault<IdHasher>;

/// `term` lives in the owning tag table's `TagsEnv` and is valid only while
/// that table's lock is held.
#[derive(Clone, Copy)]
struct TagsKey {
    hash: u64,
    term: NIF_TERM,
}

impl TagsKey {
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

/// `Peep.EventHandler` drops non-numeric measurements before any backend sees
/// them, so these are the only two shapes that arrive.
#[derive(Clone, Copy)]
enum Measurement {
    Int(i64),
    Float(f64),
}

impl Measurement {
    fn decode(term: Term) -> Result<Self, StorageError> {
        match term.get_type() {
            TermType::Integer => term
                .decode()
                .map(Measurement::Int)
                .map_err(|_| StorageError::BadMeasurement("integer does not fit in 64 bits")),
            TermType::Float => term_as_f64(term)
                .map(Measurement::Float)
                .ok_or(StorageError::BadMeasurement("float")),
            _ => Err(StorageError::BadMeasurement("measurement must be a number")),
        }
    }

    /// Bucket bounds are floats, so an integer above 2^53 is bucketed as the
    /// nearest float.
    fn as_f64(self) -> f64 {
        match self {
            Measurement::Int(int) => int as f64,
            Measurement::Float(float) => float,
        }
    }

    /// The contribution to a distribution's sum. A float is rounded as
    /// `round/1` does, and one that does not fit in an i64 is rejected rather
    /// than saturated by `as`.
    fn rounded(self) -> Result<i64, StorageError> {
        const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

        match self {
            Measurement::Int(int) => Ok(int),
            Measurement::Float(float) => {
                let rounded = float.round();
                if (-TWO_POW_63..TWO_POW_63).contains(&rounded) {
                    Ok(rounded as i64)
                } else {
                    Err(StorageError::BadMeasurement("distribution value does not fit in 64 bits"))
                }
            }
        }
    }
}

impl Encoder for Measurement {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            Measurement::Int(i) => i.encode(env),
            Measurement::Float(f) => f.encode(env),
        }
    }
}



rustler::init!("Elixir.Peep.Storage.RustNIF");
