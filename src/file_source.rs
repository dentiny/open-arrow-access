use std::sync::Arc;

use crate::{
    Accessor, AccessorExt, BoundSource, ByteAccessor, FactoryAccessor, ReadRequest, ReaderFactory,
    RecordMetadata, RecordReader, Result, SchemaRef,
    decompression::AutoDecompressionLayer,
    storage::{OpenDalAccessor, opendal},
};

/// Compression surrounding a complete file, independent of format-internal codecs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OuterCompression {
    /// Decode the original bytes, preserving storage range access. Use for Parquet.
    None,
    /// Select an enabled streaming decoder from the final object-key suffix.
    /// Unknown suffixes pass through; disabled codecs return Unsupported.
    /// ZIP and TAR member selection remains explicit.
    Auto,
}

/// Compose storage, optional outer decompression and a typed record format.
///
/// Construction performs no I/O or schema inspection. The returned accessor can
/// be opened directly, passed to [`crate::PreparedSource`], or bound through
/// [`BoundSource`]. The concrete factory type is retained. Format validation runs
/// before storage I/O on each open through [`FactoryAccessor`].
/// Configure backend credentials and policies on the supplied operator; object
/// keys are supplied separately in read requests. This helper does not resolve URIs.
/// Enable the chosen format and decompression codecs through their existing features.
/// This generic accessor does not add format-specific partition planning; use
/// `file_scan_accessor` with an enabled file format when that capability is needed.
///
/// ```no_run
/// # #[cfg(feature = "format-json")]
/// # async fn example(operator: open_arrow_access::storage::opendal::Operator)
/// # -> open_arrow_access::Result<()> {
/// use open_arrow_access::{
///     OuterCompression, PreparedSource, ReadOptions, ReadRequest, file_accessor,
/// };
/// use open_arrow_access::file_format::json::JsonFormat;
///
/// let accessor = file_accessor(operator, JsonFormat::infer(), OuterCompression::Auto);
/// let prepared = PreparedSource::prepare(accessor, ReadRequest::new("events.jsonl")).await?;
/// let options = ReadOptions {
///     projection: Some(vec!["id".into()]),
///     ..Default::default()
/// };
/// let mut reader = prepared.scan(options).await?;
/// while let Some(batch) = reader.next().await? { /* consume projected rows */ }
/// # Ok(())
/// # }
/// ```
pub fn file_accessor<F>(
    operator: opendal::Operator,
    format: F,
    compression: OuterCompression,
) -> FactoryAccessor<Arc<dyn ByteAccessor>, F>
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
{
    FactoryAccessor::new(file_storage(operator, compression), format)
}

fn file_storage(
    operator: opendal::Operator,
    compression: OuterCompression,
) -> Arc<dyn ByteAccessor> {
    let storage = OpenDalAccessor::new(operator);
    match compression {
        OuterCompression::None => Arc::new(storage),
        OuterCompression::Auto => Arc::new(storage.layer(AutoDecompressionLayer::new())),
    }
}

/// Compose file storage and a typed format while retaining native scan planning.
///
/// Like [`file_accessor`], construction performs no I/O, URI resolution, or schema
/// inspection. The returned [`crate::file_format::FileAccessor`] retains the
/// format's [`crate::ScanPlanner`] implementation and partition options. It can
/// be passed to [`crate::PreparedSource::prepare_plannable`] or composed with
/// record layers such as [`crate::layers::ParallelReadLayer`]. Planning may
/// inspect input; partition readers remain independently owned and lazy.
///
/// Available when a `format-*` feature is enabled. The format must support scan
/// planning over the composed byte accessor. Use [`OuterCompression::None`] for
/// Parquet range reads; text inputs can use [`OuterCompression::Auto`] with the
/// required decompression features. This helper does not schedule partitions.
///
/// ```no_run
/// # #[cfg(feature = "format-json")]
/// # async fn example(operator: open_arrow_access::storage::opendal::Operator)
/// # -> open_arrow_access::Result<()> {
/// use open_arrow_access::{
///     OuterCompression, PreparedSource, ReadOptions, ReadRequest, file_scan_accessor,
/// };
/// use open_arrow_access::file_format::json::JsonFormat;
///
/// let accessor = file_scan_accessor(operator, JsonFormat::infer(), OuterCompression::Auto);
/// let source = PreparedSource::prepare_plannable(accessor, ReadRequest::new("events.jsonl")).await?;
/// let plan = source.plan_scan(ReadOptions {
///     projection: Some(vec!["id".into()]),
///     ..Default::default()
/// }).await?;
/// // Read partitions sequentially, or hand them to an executor honoring plan policies.
/// let mut reader = plan.into_reader();
/// while let Some(batch) = reader.next().await? { /* consume projected rows */ }
/// # Ok(())
/// # }
/// ```
#[cfg(any(
    feature = "format-warc",
    feature = "format-parquet",
    feature = "format-json",
    feature = "format-atif",
    feature = "format-xml",
    feature = "format-csv",
))]
pub fn file_scan_accessor<F>(
    operator: opendal::Operator,
    format: F,
    compression: OuterCompression,
) -> crate::file_format::FileAccessor<Arc<dyn ByteAccessor>, F>
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
    crate::file_format::FileAccessor<Arc<dyn ByteAccessor>, F>: crate::ScanPlanner,
{
    crate::file_format::FileAccessor::from_format(file_storage(operator, compression), format)
}

/// Open a typed file source with metadata available and independently owned readers.
///
/// Configure the OpenDAL operator (backend, endpoint, credentials and policies)
/// in the application. `request.object` is its object key, not a URI resolver.
/// Format preparation precedes storage I/O. This function binds storage, optional
/// decompression, format and the complete request without creating a runtime.
/// This utility is always available. Enable the chosen format and automatic
/// decompression codecs through their existing features.
///
/// ```no_run
/// # #[cfg(feature = "format-json")]
/// # async fn example(operator: open_arrow_access::storage::opendal::Operator)
/// # -> open_arrow_access::Result<()> {
/// use open_arrow_access::{open_file_source, OuterCompression, ReadRequest};
/// use open_arrow_access::file_format::json::JsonFormat;
///
/// let source = open_file_source(
///     operator,
///     ReadRequest::new("events.jsonl"),
///     JsonFormat::infer(),
///     OuterCompression::Auto,
/// ).await?.into_bound_source();
/// println!("{:?}", source.schema());
/// let mut reader = source.reader().await?;
/// while let Some(batch) = reader.next().await? { /* consume batch */ }
/// # Ok(())
/// # }
/// ```
pub async fn open_file_source<F>(
    operator: opendal::Operator,
    request: ReadRequest,
    format: F,
    compression: OuterCompression,
) -> Result<FileSource>
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    FileSource::bind(file_accessor(operator, format, compression), request).await
}

/// A file selection bound to its initial metadata and read options.
///
/// The reader opened during binding is retained after schema inspection, avoiding
/// another open or inference pass on first consumption. Later calls reopen independently and
/// require the same output schema, including metadata. Reader cursors are never
/// shared; readers remain usable after dropping the source. Binding fixes the
/// schema, not the file contents, and adds no snapshot guarantee.
pub struct FileSource {
    source: BoundSource,
}

impl FileSource {
    /// Consume this file source as a provider-independent bound source.
    ///
    /// Transfers the bound metadata, retained first reader and original selection
    /// without opening the file again or polling any rows. Subsequent readers
    /// keep the same reopening and schema-validation behavior.
    pub fn into_bound_source(self) -> BoundSource {
        self.source
    }

    async fn bind(
        accessor: impl Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
        request: ReadRequest,
    ) -> Result<Self> {
        Ok(Self {
            source: BoundSource::bind(accessor, request).await?,
        })
    }

    /// Metadata established by the initial open, available before consuming rows.
    pub fn metadata(&self) -> &RecordMetadata {
        self.source.metadata()
    }

    /// Share the fixed output schema without opening or polling another reader.
    pub fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    /// Take the bound reader once, then reopen with the original request.
    /// Schema drift returns Decode before yielding a reader; source errors pass through.
    pub async fn reader(&self) -> Result<Box<dyn RecordReader>> {
        self.source.reader().await
    }
}
