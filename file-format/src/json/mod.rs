//! JSON object streams, including JSONL, decoded with an explicit or inferred Arrow schema.

mod accessor;
mod error;
mod format;
mod inference;
mod nonfinite;
mod planning;
mod reader;

pub use accessor::JsonAccessor;
pub use format::JsonFormat;
pub use inference::JsonInferenceOptions;
