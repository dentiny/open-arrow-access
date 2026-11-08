//! Shared contracts with no dependency on a storage backend or file format.

mod accessor;
mod async_read;
mod async_seek;
mod capability;
mod decode_batches;
mod document;
mod error;
mod factory_accessor;
mod layer;
mod list_files;
mod project_batch;
mod read_all;
mod read_chunks;
mod read_lines;
mod read_object;
mod read_state;
mod reader;
mod reader_factory;
mod record_batch_builder;
mod request;
mod stream_reader;

pub use accessor::{Accessor, ByteAccessor, ObjectStream};
pub use async_read::into_async_read;
pub use async_seek::{SeekableInput, into_async_seek};
pub use capability::Capability;
pub use decode_batches::{BatchDecoder, decode_batches};
pub use document::{DocumentAccessor, DocumentDecoder};
pub use error::{Error, ErrorKind, Result};
pub use factory_accessor::FactoryAccessor;
pub use layer::{AccessorExt, Layer};
pub use list_files::list_files;
pub use project_batch::project_batch;
pub use read_all::read_all;
pub use read_chunks::read_chunks;
pub use read_lines::read_lines;
pub use read_object::read_object;
pub use read_state::ReadState;
pub use reader::{
    ByteMetadata, ByteReader, Reader, RecordMetadata, RecordReader, RecordStream, into_stream,
};
pub use reader_factory::{FactoryToken, ReaderFactory, ReaderFactoryExt};
pub use record_batch_builder::{FieldValue, RecordBatchBuilder, RecordBatchRow};
pub use request::{
    ByteReadOptions, ByteReadRequest, DEFAULT_BATCH_SIZE, ListRequest, ObjectRef, ReadOptions,
    ReadOptionsProvider, ReadRequest,
};
pub use stream_reader::StreamReader;

pub use arrow_array::RecordBatch;
pub use arrow_schema::{Schema, SchemaRef};
pub use async_trait::async_trait;
pub use bytes::Bytes;
