//! Shared contracts with no dependency on a storage backend or file format.

mod accessor;
mod async_read;
mod capability;
mod error;
mod layer;
mod read_chunks;
mod read_limited;
mod read_lines;
mod read_state;
mod reader;
mod reader_factory;
mod request;
mod stream_reader;

pub use accessor::{Accessor, ByteAccessor, ObjectStream};
pub use async_read::into_async_read;
pub use capability::Capability;
pub use error::{Error, ErrorKind, Result};
pub use layer::{AccessorExt, Layer};
pub use read_chunks::read_chunks;
pub use read_limited::read_limited;
pub use read_lines::read_lines;
pub use read_state::ReadState;
pub use reader::{
    ByteMetadata, ByteReader, Reader, RecordMetadata, RecordReader, RecordStream, into_stream,
};
pub use reader_factory::{ReaderFactory, open_reader};
pub use request::{
    ByteReadOptions, ByteReadRequest, DEFAULT_BATCH_SIZE, ListRequest, ObjectRef, ReadOptions,
    ReadOptionsProvider, ReadRequest,
};
pub use stream_reader::StreamReader;

pub use arrow_array::RecordBatch;
pub use arrow_schema::{Schema, SchemaRef};
pub use async_trait::async_trait;
pub use bytes::Bytes;
