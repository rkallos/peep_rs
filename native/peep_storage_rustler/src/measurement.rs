//! A sample's numeric value, kept as the integer or float it arrived as.

use crate::error::StorageError;
use crate::ffi::term_as_f64;
use rustler::{Encoder, Env, Term, TermType};

/// `Peep.EventHandler` drops non-numeric measurements before any backend sees
/// them, so these are the only two shapes that arrive.
#[derive(Clone, Copy)]
pub enum Measurement {
    Int(i64),
    Float(f64),
}

impl Measurement {
    pub fn decode(term: Term) -> Result<Self, StorageError> {
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
    pub fn as_f64(self) -> f64 {
        match self {
            Measurement::Int(int) => int as f64,
            Measurement::Float(float) => float,
        }
    }

    /// The contribution to a distribution's sum. A float is rounded as
    /// `round/1` does, and one that does not fit in an i64 is rejected rather
    /// than saturated by `as`.
    pub fn rounded(self) -> Result<i64, StorageError> {
        const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

        match self {
            Measurement::Int(int) => Ok(int),
            Measurement::Float(float) => {
                let rounded = float.round();
                if (-TWO_POW_63..TWO_POW_63).contains(&rounded) {
                    Ok(rounded as i64)
                } else {
                    Err(StorageError::BadMeasurement(
                        "distribution value does not fit in 64 bits",
                    ))
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
