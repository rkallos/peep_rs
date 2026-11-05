//! The errors the NIFs raise, as `{:peep_storage_error, reason, detail}`.
//! Callers match `reason`; `detail` is a diagnostic.

use crate::atoms;
use rustler::{Atom, Encoder, Env, Term};

#[derive(Debug)]
pub enum StorageError {
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
