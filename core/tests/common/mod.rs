//! Shared helpers for core integration tests; include with `mod common;`.
//! Nesting helpers here keeps Cargo from treating them as test executables.

mod failing_reader;

pub(crate) use failing_reader::FailingReader;
