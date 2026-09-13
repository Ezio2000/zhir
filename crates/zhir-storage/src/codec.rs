//! Only compact checkpoint cores and accepted history deltas enter commit I/O.
#![cfg(any(feature = "sqlite", feature = "mysql", feature = "redis"))]
use zhir_core::error::Error;
pub(crate) fn storage_error(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}
