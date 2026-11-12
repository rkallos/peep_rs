//! Safe wrappers over the erl_nif calls Rustler doesn't wrap the way this
//! crate needs.

use rustler::sys::{
    enif_get_double, enif_get_tuple, enif_is_identical, enif_system_info, ErlNifSysInfo, ErlNifTerm,
};
use rustler::{Env, Term};
use std::mem::size_of;

pub fn scheduler_count() -> usize {
    let mut info: ErlNifSysInfo = unsafe { std::mem::zeroed() };
    unsafe {
        enif_system_info(&mut info, size_of::<ErlNifSysInfo>());
    }
    info.scheduler_threads as usize
}

/// Whether two terms are `=:=`. The terms may belong to different
/// environments, as long as both are live.
pub fn terms_identical(a: ErlNifTerm, b: ErlNifTerm) -> bool {
    a == b || unsafe { enif_is_identical(a, b) == 1 }
}

// rustler's f64 decoder falls back to integer decoding.
// We don't want this behavior in Measurement::decode().
// 5 and 5.0 must not be stored as the same value.
pub fn term_as_f64(term: Term) -> Option<f64> {
    let mut value = 0f64;
    let found =
        unsafe { enif_get_double(term.get_env().as_c_arg(), term.as_c_arg(), &mut value) } == 1;
    found.then_some(value)
}

/// A tuple's elements, read in place rather than collected into a `Vec` as
/// `rustler::types::tuple::get_tuple` does.
pub struct TupleElements<'a> {
    env: Env<'a>,
    elements: &'a [ErlNifTerm],
}

impl<'a> TupleElements<'a> {
    pub fn new(tuple: Term<'a>) -> Option<Self> {
        let env = tuple.get_env();
        let mut arity = 0;
        let mut first = std::ptr::null();
        let found =
            unsafe { enif_get_tuple(env.as_c_arg(), tuple.as_c_arg(), &mut arity, &mut first) };
        if found != 1 {
            return None;
        }

        // An empty tuple's pointer need not be one a slice can be made from.
        let elements = match usize::try_from(arity) {
            Ok(arity) if arity > 0 => unsafe { std::slice::from_raw_parts(first, arity) },
            _ => &[],
        };
        Some(TupleElements { env, elements })
    }

    pub fn get(&self, index: usize) -> Option<Term<'a>> {
        let element = *self.elements.get(index)?;
        Some(unsafe { Term::new(self.env, element) })
    }
}
