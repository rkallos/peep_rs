//! `Peep.Storage.RustNIF`: a Peep storage backend whose metrics live in Rust.
//!
//! This file holds the NIFs, each a few lines over the modules that own the
//! data:
//!
//! - `tags`: the tags maps inserted on each scheduler, stored once under an id
//!   and shared by every metric, and the locking that keeps inserts, prunes
//!   and scrapes consistent.
//! - `metrics`: each metric's series, keyed by tag id, one map per scheduler.
//! - `distribution`: distribution buckets, from registration to scrape.
//! - `scrape`: merging series across schedulers into the maps Peep returns.
//! - `measurement`, `per_scheduler`, `term_store`, `ffi`, `error`: the
//!   pieces those share.

// Never panic; handle our own errors, so we avoid poisoning write locks. Errors
// should raise exceptions and detach :telemetry handlers.
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented
)]

mod distribution;
mod error;
mod ffi;
mod measurement;
mod metrics;
mod per_scheduler;
mod scrape;
mod tags;
mod term_store;

use error::StorageError;
use metrics::Metrics;
use rustler::types::tuple::get_tuple;
use rustler::{Atom, NifMap, Resource, ResourceArc, Term};
use std::mem::size_of;
use std::sync::OnceLock;
use tags::TagTables;

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

struct Storage {
    registered: OnceLock<Registered>,
}

#[rustler::resource_impl]
impl Resource for Storage {}

// parking_lot doesn't poison, so it can't assert this for us. Nothing inside a
// critical section panics, so a guard can't drop mid-unwind.
impl std::panic::RefUnwindSafe for Storage {}

struct Registered {
    tags: TagTables,
    metrics: Metrics,
}

impl Storage {
    fn registered(&self) -> Result<&Registered, StorageError> {
        self.registered.get().ok_or(StorageError::NotRegistered)
    }
}

#[rustler::nif]
fn new(_opts: Term) -> ResourceArc<Storage> {
    ResourceArc::new(Storage {
        registered: OnceLock::new(),
    })
}

// Copies bucket labels into environments of their own.
#[rustler::nif(schedule = "DirtyCpu")]
fn register_metrics(storage: &Storage, ids_to_metrics: Term) -> Result<Atom, rustler::Error> {
    let registered = Registered {
        tags: TagTables::new(),
        metrics: Metrics::decode(ids_to_metrics)?,
    };

    storage
        .registered
        .set(registered)
        .map_err(|_| StorageError::AlreadyRegistered)?;

    Ok(rustler::types::atom::ok())
}

/// `metric` is unused here; it stays in the item shape because the ETS backends
/// dispatch on it.
#[rustler::nif]
fn insert_metrics(
    storage: &Storage,
    tag_results: Term,
    batch: Term,
) -> Result<Atom, rustler::Error> {
    let Registered { tags, metrics } = storage.registered()?;
    let mut tags = tags.batch(tag_results)?;

    let items = batch
        .into_list_iterator()
        .map_err(|_| StorageError::BadArgument("batch must be a list"))?;

    for item in items {
        // Avoids `Vec<Term>` allocations by `rustler::types::tuple::get_tuple`
        let (id, _metric, value, tag_idx): (usize, Term, Term, usize) =
            item.decode().map_err(|_| {
                StorageError::BadArgument("batch items must be {id, metric, value, tag_idx} tuples")
            })?;

        metrics.record(&mut tags, id, value, tag_idx)?;
    }

    Ok(rustler::types::atom::ok())
}

#[derive(NifMap)]
struct StorageSize {
    size: usize,
    memory: usize,
}

#[rustler::nif(schedule = "DirtyCpu")]
fn storage_size(storage: &Storage) -> StorageSize {
    let Ok(Registered { tags, metrics }) = storage.registered() else {
        return StorageSize { size: 0, memory: 0 };
    };

    let (size, memory) = metrics.size();

    StorageSize {
        size,
        memory: size_of::<Registered>() + tags.memory() + memory,
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn nif_get_all_metrics<'a>(
    storage: &Storage,
    ids_to_metrics: Term<'a>,
) -> Result<Term<'a>, rustler::Error> {
    let env = ids_to_metrics.get_env();

    let Ok(Registered { tags, metrics }) = storage.registered() else {
        return Ok(Term::map_new(env));
    };

    let metric_terms = get_tuple(ids_to_metrics)
        .map_err(|_| StorageError::BadArgument("ids_to_metrics must be a tuple"))?;

    Ok(scrape::get_all_metrics(env, tags, metrics, &metric_terms)?)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn prune_tags(storage: &Storage, patterns: Term) -> Result<Atom, rustler::Error> {
    let Ok(Registered { tags, metrics }) = storage.registered() else {
        return Ok(rustler::types::atom::ok());
    };

    let patterns: Vec<Term> = patterns
        .decode()
        .map_err(|_| StorageError::BadArgument("patterns must be a list"))?;

    if patterns.iter().any(|pattern| !pattern.is_map()) {
        return Err(StorageError::BadTagsMap.into());
    }

    tags.prune(&patterns, |doomed| metrics.remove(doomed));

    Ok(rustler::types::atom::ok())
}

rustler::init!("Elixir.Peep.Storage.RustNIF");
