//! Distribution buckets: the boundaries and upper bounds Peep registers, the
//! counts recorded into them, and the bucket maps a scrape returns.

use crate::atoms;
use crate::error::StorageError;
use rustler::{Encoder as _, Env, Term};
use std::mem::size_of;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// A distribution's finite buckets, each the exclusive boundary its values
/// fall below and the upper bound a scrape keys it by. Peep sorts them by
/// boundary, with no repeated boundary or upper bound.
pub struct Layout {
    boundaries: Box<[f64]>,
    upper_bounds: Box<[f64]>,
}

impl Layout {
    /// The bucket a value belongs to; `boundaries.len()` is the overflow
    /// bucket.
    fn bucket(&self, value: f64) -> usize {
        self.boundaries
            .partition_point(|&boundary| boundary <= value)
    }

    fn memory(&self) -> usize {
        size_of::<Layout>() + (self.boundaries.len() + self.upper_bounds.len()) * size_of::<f64>()
    }
}

/// The layouts registered so far. Distributions usually share a layout, so
/// each distinct one is built once and shared.
#[derive(Default)]
pub struct Layouts {
    layouts: Vec<Arc<Layout>>,
}

impl Layouts {
    /// The layout for `buckets`, a list of `{boundary, upper_bound}` tuples of
    /// floats.
    pub fn intern(&mut self, buckets: Term) -> Result<Arc<Layout>, StorageError> {
        let buckets: Vec<(f64, f64)> = buckets.decode().map_err(|_| {
            StorageError::BadArgument(
                ":peep_buckets must be a list of {boundary, upper_bound} tuples of floats",
            )
        })?;
        let (boundaries, upper_bounds): (Vec<f64>, Vec<f64>) = buckets.into_iter().unzip();

        let matches = |layout: &&Arc<Layout>| {
            *layout.boundaries == *boundaries && *layout.upper_bounds == *upper_bounds
        };

        if let Some(layout) = self.layouts.iter().find(matches) {
            return Ok(Arc::clone(layout));
        }

        let layout = Arc::new(Layout {
            boundaries: boundaries.into(),
            upper_bounds: upper_bounds.into(),
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
            buckets: (0..=layout.boundaries.len())
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
/// across all of them: each bucket's upper bound in bucket order, `:infinity` for
/// the overflow bucket, then `:sum`.
pub struct Encoder<'a> {
    keys: Vec<Term<'a>>,
    values: Vec<Term<'a>>,
}

impl<'a> Encoder<'a> {
    pub fn new(env: Env<'a>, layout: &Layout) -> Self {
        let mut keys: Vec<Term> = layout
            .upper_bounds
            .iter()
            .map(|upper_bound| upper_bound.encode(env))
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
