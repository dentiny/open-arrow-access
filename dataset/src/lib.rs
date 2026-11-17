//! Dataset composition over shared record and byte accessors.

#[cfg(feature = "directory")]
pub mod directory;
#[cfg(any(feature = "directory", feature = "manifest"))]
pub mod file_set;
#[cfg(feature = "manifest")]
pub mod manifest;
