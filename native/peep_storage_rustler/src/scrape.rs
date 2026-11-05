//! `get_all_metrics`: each metric's series merged across schedulers and
//! encoded as Erlang maps.

use crate::error::StorageError;
use crate::metrics::{Merged, Metric, Metrics};
use crate::tags::{IdMap, TagTables, TagsKey, TagsMap};
use rustler::{Env, Term};

/// For one scheduler, the caller-environment copy of each tags map a scrape
/// has met so far, shared by every metric the scrape encodes. Ids are never
/// reused, so a copy stays correct even if a prune runs between two metrics.
type TagCopies = IdMap<TagsKey>;

/// A map from each metric in `metric_terms` with at least one series to its
/// series. `metric_terms` are the metrics Peep registered, in the same order.
pub fn get_all_metrics<'a>(
    env: Env<'a>,
    tags: &TagTables,
    metrics: &Metrics,
    metric_terms: &[Term<'a>],
) -> Result<Term<'a>, StorageError> {
    let mut copies: Vec<TagCopies> = (0..tags.len()).map(|_| TagCopies::default()).collect();
    let mut keys = Vec::new();
    let mut values = Vec::new();

    for (metric_term, metric) in metric_terms.iter().zip(metrics.iter()) {
        let merged = merge(env, tags, &mut copies, metric);

        if !merged.is_empty() {
            keys.push(*metric_term);
            values.push(encode(env, metric, &merged)?);
        }
    }

    Term::map_from_term_arrays(env, &keys, &values)
        .map_err(|_| StorageError::MapBuildFailed("duplicate metric definitions"))
}

/// A tags map present on several schedulers is copied once per scheduler, and
/// all but the first copy are then replaced by the first. That one copy keys
/// the tags map in every metric's output, and finding it in `merged` takes a
/// pointer comparison rather than `enif_is_identical`.
fn merge(env: Env, tags: &TagTables, copies: &mut [TagCopies], metric: &Metric) -> TagsMap<Merged> {
    let mut merged: TagsMap<Merged> =
        TagsMap::with_capacity_and_hasher(metric.largest_shard(), Default::default());

    for (reader, copies) in tags.readers().zip(copies.iter_mut()) {
        metric.scan(&reader, |tag_id, sample| {
            let copy = copies.get(&tag_id).copied();
            let key = match copy {
                Some(copy) => copy,
                // Pruning removes an id from the metrics and the table
                // together, under the table's write lock, so this never skips.
                None => match reader.get(tag_id) {
                    Some(key) => key,
                    None => return,
                },
            };

            match merged
                .raw_entry_mut()
                .from_hash(key.hash(), |seen: &TagsKey| *seen == key)
            {
                hashbrown::hash_map::RawEntryMut::Occupied(mut entry) => {
                    if !copy.is_some_and(|copy| copy.is(entry.key())) {
                        copies.insert(tag_id, *entry.key());
                    }
                    entry.get_mut().add(sample);
                }
                hashbrown::hash_map::RawEntryMut::Vacant(entry) => {
                    let copy = copy.unwrap_or_else(|| {
                        let copy = reader.copy(env, &key);
                        copies.insert(tag_id, copy);
                        copy
                    });
                    entry.insert(copy, Merged::new(sample));
                }
            }
        });
    }

    merged
}

fn encode<'a>(
    env: Env<'a>,
    metric: &Metric,
    merged: &TagsMap<Merged>,
) -> Result<Term<'a>, StorageError> {
    let mut encoder = metric.encoder(env);
    let mut keys = Vec::with_capacity(merged.len());
    let mut values = Vec::with_capacity(merged.len());

    for (key, series) in merged {
        keys.push(key.term(env));
        values.push(encoder.encode(env, series)?);
    }

    Term::map_from_term_arrays(env, &keys, &values)
        .map_err(|_| StorageError::MapBuildFailed("series map"))
}
