//! Distribution buckets: the bounds and labels Peep registers, the counts
//! recorded into them, and the bucket maps a scrape returns.

use crate::atoms;
use crate::error::StorageError;
use crate::ffi::terms_identical;
use crate::term_store::TermStore;
use rustler::wrapper::NIF_TERM;
use rustler::{Encoder as _, Env, Term};
use std::mem::size_of;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// A distribution's finite buckets, each an exclusive upper bound and the
/// label a scrape returns for it. Peep sorts them by bound, with no repeated
/// bound or label.
pub struct Layout {
    bounds: Box<[f64]>,
    labels: Box<[NIF_TERM]>,
    /// Owns `labels`. Written only while the layout is built, before it is
    /// shared.
    store: TermStore,
}

impl Layout {
    /// The bucket a value belongs to; `bounds.len()` is the overflow bucket.
    fn bucket(&self, value: f64) -> usize {
        self.bounds.partition_point(|&bound| bound <= value)
    }

    fn memory(&self) -> usize {
        size_of::<Layout>()
            + self.bounds.len() * size_of::<f64>()
            + self.labels.len() * size_of::<NIF_TERM>()
            + self
                .labels
                .iter()
                .map(|label| self.store.size_of(*label))
                .sum::<usize>()
    }
}

/// The layouts registered so far. Distributions usually share a layout, so
/// each distinct one is built once and shared.
#[derive(Default)]
pub struct Layouts {
    layouts: Vec<Arc<Layout>>,
}

impl Layouts {
    /// The layout for `buckets`, a list of `{bound, label}` tuples.
    pub fn intern(&mut self, buckets: Term) -> Result<Arc<Layout>, StorageError> {
        let buckets: Vec<(f64, Term)> = buckets.decode().map_err(|_| {
            StorageError::BadArgument(":peep_buckets must be a list of {bound, label} tuples")
        })?;

        let matches = |layout: &&Arc<Layout>| {
            layout.bounds.len() == buckets.len()
                && buckets
                    .iter()
                    .zip(layout.bounds.iter().zip(layout.labels.iter()))
                    .all(|((bound, label), (stored_bound, stored_label))| {
                        stored_bound == bound && terms_identical(*stored_label, label.as_c_arg())
                    })
        };

        if let Some(layout) = self.layouts.iter().find(matches) {
            return Ok(Arc::clone(layout));
        }

        let mut store = TermStore::new();
        let layout = Arc::new(Layout {
            bounds: buckets.iter().map(|(bound, _)| *bound).collect(),
            labels: buckets
                .iter()
                .map(|(_, label)| store.store(label.as_c_arg()))
                .collect(),
            store,
        });
        self.layouts.push(Arc::clone(&layout));
        Ok(layout)
    }

    pub fn memory(&self) -> usize {
        self.layouts.iter().map(|layout| layout.memory()).sum()
    }
}

/// One series' counts: one per bucket, the overflow bucket last, and the sum
/// of the values recorded.
pub struct Cell {
    buckets: Box<[AtomicU64]>,
    sum: AtomicI64,
}

impl Cell {
    pub fn new(layout: &Layout) -> Self {
        Cell {
            buckets: (0..=layout.bounds.len())
                .map(|_| AtomicU64::new(0))
                .collect(),
            sum: AtomicI64::new(0),
        }
    }

    /// Records a value, given as the float it is bucketed by and the integer
    /// it adds to the sum.
    pub fn record(&self, layout: &Layout, (value, rounded): (f64, i64)) {
        self.buckets[layout.bucket(value)].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(rounded, Ordering::Relaxed);
    }

    pub fn heap_size(&self) -> usize {
        self.buckets.len() * size_of::<AtomicU64>()
    }
}

/// A series' counts summed across schedulers.
pub struct Totals {
    buckets: Vec<u64>,
    sum: i64,
}

impl Totals {
    pub fn new(cell: &Cell) -> Self {
        Totals {
            buckets: cell
                .buckets
                .iter()
                .map(|count| count.load(Ordering::Relaxed))
                .collect(),
            sum: cell.sum.load(Ordering::Relaxed),
        }
    }

    pub fn add(&mut self, cell: &Cell) {
        for (total, count) in self.buckets.iter_mut().zip(cell.buckets.iter()) {
            *total = total.wrapping_add(count.load(Ordering::Relaxed));
        }
        self.sum = self.sum.wrapping_add(cell.sum.load(Ordering::Relaxed));
    }
}

/// Encodes a distribution's series as bucket maps, sharing one set of keys
/// across all of them: each bucket's label in bucket order, `:infinity` for
/// the overflow bucket, then `:sum`.
pub struct Encoder<'a> {
    keys: Vec<Term<'a>>,
    values: Vec<Term<'a>>,
}

impl<'a> Encoder<'a> {
    pub fn new(env: Env<'a>, layout: &Layout) -> Self {
        let mut keys: Vec<Term> = layout
            .labels
            .iter()
            .map(|label| layout.store.copy_out(env, *label))
            .collect();
        keys.push(atoms::infinity().encode(env));
        keys.push(atoms::sum().encode(env));

        Encoder {
            values: Vec::with_capacity(keys.len()),
            keys,
        }
    }

    pub fn encode(&mut self, env: Env<'a>, totals: &Totals) -> Result<Term<'a>, StorageError> {
        self.values.clear();
        self.values
            .extend(totals.buckets.iter().map(|count| count.encode(env)));
        self.values.push(totals.sum.encode(env));

        Term::map_from_term_arrays(env, &self.keys, &self.values)
            .map_err(|_| StorageError::MapBuildFailed("distribution bucket map"))
    }
}
