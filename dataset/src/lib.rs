//! Dataset composition over shared record and byte accessors.

#[cfg(feature = "directory")]
mod directory;
#[cfg(feature = "directory")]
mod file_set;

#[cfg(feature = "directory")]
pub use directory::{DirectoryAccessor, DirectoryRequest};
#[cfg(feature = "directory")]
pub use file_set::{FileSetAccessor, FileSetRequest};
