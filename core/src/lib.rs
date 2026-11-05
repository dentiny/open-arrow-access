//! Shared contracts with no dependency on a storage backend or file format.

mod accessor;
mod capability;
mod error;
mod layer;
mod reader;
mod request;

pub use accessor::{Accessor, ByteAccessor, ObjectStream};
pub use capability::Capability;
pub use error::{Error, ErrorKind, Result};
pub use layer::{AccessorExt, Layer};
pub use reader::{
    ByteMetadata, ByteReader, Reader, RecordMetadata, RecordReader, RecordStream, into_stream,
};
pub use request::{DEFAULT_BATCH_SIZE, ObjectRef, ReadOptions, ReadOptionsProvider, ReadRequest};

pub use arrow_array::RecordBatch;
pub use arrow_schema::{Schema, SchemaRef};
/// Attribute for implementing the asynchronous accessor and reader traits.
pub use async_trait::async_trait;
pub use bytes::Bytes;
