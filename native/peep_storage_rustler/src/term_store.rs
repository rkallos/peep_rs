//! Copies of terms kept outside any process, so they outlive the NIF call
//! that handed them over.

use rustler::env::OwnedEnv;
use rustler::sys::{enif_make_copy, ErlNifTerm};
use rustler::{Env, Term};

/// An environment of its own that stored terms live in. An environment has no
/// per-term free, so a stored term lives until the store is dropped.
///
/// The terms it hands back are valid only while the store lives, and only
/// terms it handed back may be passed to its other methods.
pub struct TermStore(OwnedEnv);

// SAFETY: `store` is the only operation that allocates in the environment, and
// it takes `&mut self`, so it never runs while a stored term is read through
// `&self`. An owner that shares a store between threads either guards it with
// a lock or stops storing before sharing it.
unsafe impl Sync for TermStore {}

impl TermStore {
    pub fn new() -> Self {
        TermStore(OwnedEnv::new())
    }

    pub fn store(&mut self, term: ErlNifTerm) -> ErlNifTerm {
        self.0
            .run(|owned| unsafe { enif_make_copy(owned.as_c_arg(), term) })
    }

    pub fn copy_out<'a>(&self, env: Env<'a>, term: ErlNifTerm) -> Term<'a> {
        unsafe { Term::new(env, enif_make_copy(env.as_c_arg(), term)) }
    }

    pub fn size_of(&self, term: ErlNifTerm) -> usize {
        self.0.run(|owned| unsafe { Term::new(owned, term) }.size())
    }
}
