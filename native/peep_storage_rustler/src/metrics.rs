//! Each metric's series: one map per scheduler, from a tags map's `TagId` to
//! the series' value.
//!
//! Series are stored, removed and scanned only through the tag tables' lock
//! holders, which keeps the tables' locking order: `record` takes a `Batch`,
//! `remove` a `Doomed`, and `Metric::scan` a `Reader`. Sizing them takes no tag
//! table lock, and holds one series lock at a time.

use crate::atoms;
use crate::distribution::{self, Layout, Layouts, Totals};
use crate::error::StorageError;
use crate::measurement::Measurement;
use crate::per_scheduler::PerScheduler;
use crate::tags::{entry_size, Batch, Doomed, IdMap, Reader, TagId};
use hashbrown::hash_map::Entry;
use parking_lot::{Mutex, RwLock};
use rustler::types::tuple::get_tuple;
use rustler::{Atom, Encoder as _, Env, Term};
use std::mem::size_of;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// The registered metrics, indexed by Peep's metric id.
pub struct Metrics {
    metrics: Box<[Metric]>,
    layouts: Layouts,
}

pub struct Metric(Series);

enum Series {
    Counter(Shards<AtomicI64>),
    Sum(Shards<AtomicI64>),
    LastValue(Shards<LastValueCell>),
    Distribution {
        layout: Arc<Layout>,
        shards: Shards<distribution::Cell>,
    },
}

type Shards<V> = PerScheduler<RwLock<IdMap<V>>>;

/// A `last_value` timestamp and measurement. The OS clock rather than
/// `enif_monotonic_time`, which adds ERTS's time correction: ordering samples
/// needs only a clock that is monotonic across threads.
type LastValueCell = Mutex<(Instant, Measurement)>;

const _: () = assert!(size_of::<RwLock<IdMap<AtomicI64>>>() <= 128);

/// A series' value on one scheduler, of whichever type its metric stores.
trait SeriesValue {
    fn sample(&self) -> Sample<'_>;

    /// The memory the value owns outside itself.
    fn heap_size(&self) -> usize {
        0
    }
}

impl SeriesValue for AtomicI64 {
    fn sample(&self) -> Sample<'_> {
        Sample::Count(self.load(Ordering::Relaxed))
    }
}

impl SeriesValue for LastValueCell {
    fn sample(&self) -> Sample<'_> {
        Sample::LastValue(*self.lock())
    }
}

impl SeriesValue for distribution::Cell {
    fn sample(&self) -> Sample<'_> {
        Sample::Distribution(self)
    }

    fn heap_size(&self) -> usize {
        distribution::Cell::heap_size(self)
    }
}

/// Evaluates `$body` with `$shards` bound to the series' shards, whatever
/// their value type.
macro_rules! with_shards {
    ($series:expr, $shards:ident => $body:expr) => {
        match $series {
            Series::Counter($shards) | Series::Sum($shards) => $body,
            Series::LastValue($shards) => $body,
            Series::Distribution {
                shards: $shards, ..
            } => $body,
        }
    };
}

impl Metrics {
    /// Decodes Peep's tuple of metric structs, in metric id order.
    pub fn decode(ids_to_metrics: Term) -> Result<Self, StorageError> {
        let mut layouts = Layouts::default();

        let metrics = get_tuple(ids_to_metrics)
            .map_err(|_| StorageError::BadArgument("ids_to_metrics must be a tuple"))?
            .into_iter()
            .map(|metric| Metric::decode(metric, &mut layouts))
            .collect::<Result<_, _>>()?;

        Ok(Metrics { metrics, layouts })
    }

    pub fn iter(&self) -> impl Iterator<Item = &Metric> {
        self.metrics.iter()
    }

    /// Stores one sample of metric `id` under the batch's tags map at
    /// `tag_idx`. The tags map is resolved only once the value is valid, so a
    /// rejected sample stores no tags.
    pub fn record(
        &self,
        batch: &mut Batch,
        id: usize,
        value: Term,
        tag_idx: usize,
    ) -> Result<(), StorageError> {
        let shard = batch.shard();

        match self.metrics.get(id).map(|metric| &metric.0) {
            Some(Series::Counter(shards)) => {
                add(shards.get(shard), batch.tag_id(tag_idx)?, 1);
                Ok(())
            }
            Some(Series::Sum(shards)) => {
                let delta = value
                    .decode()
                    .map_err(|_| StorageError::BadMeasurement("sum value must be an integer"))?;

                add(shards.get(shard), batch.tag_id(tag_idx)?, delta);
                Ok(())
            }
            Some(Series::LastValue(shards)) => {
                let sample = (Instant::now(), Measurement::decode(value)?);
                let apply = |cell: &LastValueCell, sample: (Instant, Measurement)| {
                    let mut current = cell.lock();
                    if sample.0 > current.0 {
                        *current = sample;
                    }
                };

                let shard = shards.get(shard);
                upsert(
                    shard,
                    batch.tag_id(tag_idx)?,
                    sample,
                    LastValueCell::new,
                    apply,
                );
                Ok(())
            }
            Some(Series::Distribution { layout, shards }) => {
                let measurement = Measurement::decode(value)?;
                let sample = (measurement.as_f64(), measurement.rounded()?);
                let make = |sample| {
                    let cell = distribution::Cell::new(layout);
                    cell.record(layout, sample);
                    cell
                };
                let apply = |cell: &distribution::Cell, sample| cell.record(layout, sample);

                upsert(
                    shards.get(shard),
                    batch.tag_id(tag_idx)?,
                    sample,
                    make,
                    apply,
                );
                Ok(())
            }
            None => Err(StorageError::UnknownMetricId(id)),
        }
    }

    /// Drops every metric's series under the doomed ids.
    pub fn remove(&self, doomed: &Doomed) {
        for metric in self.metrics.iter() {
            with_shards!(&metric.0, shards => remove(shards, doomed));
        }
    }

    /// The number of series, counting a series once per scheduler it is on,
    /// and the memory they take.
    pub fn size(&self) -> (usize, usize) {
        let mut size = 0;
        let mut memory = self.layouts.memory();

        for metric in self.metrics.iter() {
            let (series, bytes) = with_shards!(&metric.0, shards => size_of_shards(shards));

            size += series;
            memory += bytes;
        }

        (size, memory)
    }
}

impl Metric {
    fn decode(metric: Term, layouts: &mut Layouts) -> Result<Self, StorageError> {
        let struct_name: Atom = metric
            .map_get(rustler::types::atom::__struct__())
            .map_err(|_| StorageError::BadArgument("metric must be a struct"))?
            .decode()
            .map_err(|_| StorageError::BadArgument("__struct__ must be an atom"))?;

        let series = if struct_name == atoms::metric_counter() {
            Series::Counter(new_shards())
        } else if struct_name == atoms::metric_sum() {
            Series::Sum(new_shards())
        } else if struct_name == atoms::metric_last_value() {
            Series::LastValue(new_shards())
        } else if struct_name == atoms::metric_distribution() {
            let buckets = metric.map_get(atoms::peep_buckets()).map_err(|_| {
                StorageError::BadArgument("Distribution metric must carry :peep_buckets")
            })?;

            Series::Distribution {
                layout: layouts.intern(buckets)?,
                shards: new_shards(),
            }
        } else {
            return Err(StorageError::BadArgument("unrecognized metric struct"));
        };

        Ok(Metric(series))
    }

    /// Calls `f` with each of this metric's series on the reader's scheduler.
    pub fn scan(&self, reader: &Reader, mut f: impl FnMut(TagId, Sample)) {
        let shard = reader.shard();

        with_shards!(&self.0, shards => {
            for (tag_id, value) in shards.get(shard).read().iter() {
                f(*tag_id, value.sample());
            }
        })
    }

    /// The most series this metric has on any one scheduler.
    pub fn largest_shard(&self) -> usize {
        fn largest<V>(shards: &Shards<V>) -> usize {
            shards
                .iter()
                .map(|shard| shard.read().len())
                .max()
                .unwrap_or(0)
        }

        with_shards!(&self.0, shards => largest(shards))
    }

    /// Encodes this metric's merged series. A distribution's encoder makes
    /// its bucket keys in `env` up front, to share them across series.
    pub fn encoder<'a>(&self, env: Env<'a>) -> SeriesEncoder<'a> {
        SeriesEncoder {
            buckets: match &self.0 {
                Series::Distribution { layout, .. } => {
                    Some(distribution::Encoder::new(env, layout))
                }
                _ => None,
            },
        }
    }
}

/// A series' value on one scheduler, as a scrape reads it.
pub enum Sample<'a> {
    Count(i64),
    LastValue((Instant, Measurement)),
    Distribution(&'a distribution::Cell),
}

/// A series' value merged across schedulers.
pub enum Merged {
    Count(i64),
    LastValue((Instant, Measurement)),
    Distribution(Totals),
}

impl Merged {
    pub fn new(sample: Sample) -> Self {
        match sample {
            Sample::Count(count) => Merged::Count(count),
            Sample::LastValue(last) => Merged::LastValue(last),
            Sample::Distribution(cell) => Merged::Distribution(Totals::new(cell)),
        }
    }

    /// Counts and sums add, and the newer last value wins. A metric's samples
    /// are all of one kind, so mismatched kinds don't occur.
    pub fn add(&mut self, sample: Sample) {
        match (self, sample) {
            (Merged::Count(total), Sample::Count(count)) => *total = total.wrapping_add(count),
            (Merged::LastValue(newest), Sample::LastValue(last)) => {
                if last.0 > newest.0 {
                    *newest = last;
                }
            }
            (Merged::Distribution(totals), Sample::Distribution(cell)) => totals.add(cell),
            _ => {}
        }
    }
}

pub struct SeriesEncoder<'a> {
    buckets: Option<distribution::Encoder<'a>>,
}

impl<'a> SeriesEncoder<'a> {
    pub fn encode(&mut self, env: Env<'a>, merged: &Merged) -> Result<Term<'a>, StorageError> {
        match (merged, &mut self.buckets) {
            (Merged::Count(total), _) => Ok(total.encode(env)),
            (Merged::LastValue((_, value)), _) => Ok(value.encode(env)),
            (Merged::Distribution(totals), Some(buckets)) => buckets.encode(env, totals),
            (Merged::Distribution(_), None) => {
                Err(StorageError::MapBuildFailed("distribution without buckets"))
            }
        }
    }
}

fn new_shards<V>() -> Shards<V> {
    PerScheduler::new(|| RwLock::new(IdMap::default()))
}

/// Exactly one of `make` and `apply` runs.
fn upsert<V, S>(
    shard: &RwLock<IdMap<V>>,
    tag_id: TagId,
    sample: S,
    make: impl FnOnce(S) -> V,
    apply: impl FnOnce(&V, S),
) {
    // Fast path.
    if let Some(value) = shard.read().get(&tag_id) {
        apply(value, sample);
        return;
    }

    match shard.write().entry(tag_id) {
        Entry::Occupied(entry) => apply(entry.get(), sample),
        Entry::Vacant(entry) => {
            entry.insert(make(sample));
        }
    }
}

#[inline]
fn add(shard: &RwLock<IdMap<AtomicI64>>, tag_id: TagId, delta: i64) {
    upsert(shard, tag_id, delta, AtomicI64::new, |total, delta| {
        total.fetch_add(delta, Ordering::Relaxed);
    });
}

/// Walks whichever of `doomed` and the shard's series is smaller.
fn remove<V>(shards: &Shards<V>, doomed: &Doomed) {
    let mut map = shards.get(doomed.shard()).write();

    if doomed.len() < map.len() {
        for tag_id in doomed.iter() {
            map.remove(tag_id);
        }
    } else {
        map.retain(|tag_id, _| !doomed.contains(tag_id));
    }
}

fn size_of_shards<V: SeriesValue>(shards: &Shards<V>) -> (usize, usize) {
    let mut series = 0;
    let mut memory = shards.memory();

    for shard in shards.iter() {
        let map = shard.read();
        series += map.len();
        memory += map.len() * entry_size::<TagId, V>();
        memory += map.values().map(V::heap_size).sum::<usize>();
    }

    (series, memory)
}
