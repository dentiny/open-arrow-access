//! JSON object streams, including JSONL, decoded with an explicit Arrow schema.

mod accessor;
mod error;
mod format;
mod nonfinite;
mod planning;
mod reader;

pub use accessor::JsonAccessor;
pub use format::JsonFormat;
