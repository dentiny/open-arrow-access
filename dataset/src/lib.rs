//! Dataset composition over shared record and byte accessors.

#[cfg(feature = "auto-format")]
pub mod auto_format;

#[cfg(feature = "directory")]
pub mod directory;
#[cfg(feature = "directory")]
pub mod file_set;
