use futures::stream;
use std::sync::Arc;

use crate::{
    Accessor, ByteAccessor, ByteReadRequest, OrderRequirement, ReadRequest, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, StreamReader, async_trait,
    scan_partition,
};

use super::{ArchiveOptions, reader::open_reader, validation};

/// Integrated package expansion from a byte accessor to an Arrow record accessor.
///
/// # Pipeline and output
///
/// One `ReadRequest` selects one source object. The configured outer compression
/// is decoded first; the explicit `ArchiveFormat` then selects TAR traversal or
/// one complete file. Every selected file becomes one row in [`super::archive_schema`]:
/// `source_uri: LargeUtf8`, `entry_index: UInt64`, `entry_name: LargeBinary`, and
/// `data: LargeBinary`, all non-null. `source_uri` is the requested object locator;
/// TAR indices preserve physical header order, including skipped directories and
/// members. Duplicate and non-UTF-8 names are retained unchanged. A single file
/// has index zero and the configured name. An empty/fully filtered TAR emits no
/// rows. Application-specific validation and schema mapping belong downstream.
///
/// # Consumption and memory
///
/// Construction, cloning, metadata inspection and planning perform no storage
/// I/O. Opening forwards the request's byte options to `open_sequential`; header
/// reads, decompression and output begin when the reader is consumed. Request
/// projection and batch size apply to the final member rows, not storage reads.
/// Excluded members are drained without body-sized buffers. Selected files are
/// always buffered in full before row construction, even when `data` is projected
/// out. Projection controls output columns. The byte target is
/// soft: one large selected file may exceed it. Codec state, upstream chunks and
/// caller-retained batches are additional memory costs; there is no background
/// producer or per-archive collection of all output batches.
///
/// Completion always consumes underlying EOF, including after TAR end markers,
/// so decoder checksum/trailer and source errors can follow earlier batches.
/// Consumers must reach EOF before committing output as a successful archive.
/// Errors are terminal; dropping the reader cancels further reads. After
/// cancelling a pending read, drop the reader under the shared `Reader` contract.
///
/// # Composition
///
/// Like `CrawlAccessor`, this is an integrated accessor. It is not a `Layer<A>`
/// implementation: bytes-to-records changes the reader/request interface. Record
/// layers can wrap its output, or pass it to `CrawlAccessor::files` with ordered
/// emission to process multiple archives. Native partitions own complete output
/// batches, allowing record processing to overlap sequential archive discovery
/// under the executor's resident partition budget. Row/byte batching is retained.
/// A single archive is read once; multiple archives use file-level scheduling
/// through `CrawlAccessor::files`, sharing the same concurrency window.
///
/// ```no_run
/// use open_arrow_access::{
///     Accessor, ArchiveAccessor, ArchiveFormat, ArchiveOptions, ByteAccessor,
///     ReadRequest, Result,
/// };
///
/// async fn read(source: impl ByteAccessor + 'static) -> Result<()> {
///     let options = ArchiveOptions::new(ArchiveFormat::Tar)
///         .with_entry_filter(|entry| Ok(entry.name.ends_with(b".tex")));
///     let archive = ArchiveAccessor::new(source, options);
///     // The .gz suffix selects gzip; TAR member selection remains explicit.
///     let mut reader = archive.open(ReadRequest::new("package.tar.gz")).await?;
///     while let Some(batch) = reader.next().await? { /* consume member rows */ }
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct ArchiveAccessor {
    source: Arc<dyn ByteAccessor>,
    options: Arc<ArchiveOptions>,
}

impl ArchiveAccessor {
    pub fn new<A: ByteAccessor + 'static>(source: A, options: ArchiveOptions) -> Self {
        Self {
            source: options.outer_compression.apply(source),
            options: Arc::new(options),
        }
    }
}

#[async_trait]
impl Accessor for ArchiveAccessor {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn metadata(&self, request: ReadRequest) -> Result<RecordMetadata> {
        Ok(RecordMetadata {
            schema: validation::projected_schema(&request.options)?,
            source: Some(request.object),
        })
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        // Reject invalid projections before accessing storage.
        let metadata = self.metadata(request.clone()).await?;
        let input = self
            .source
            .open_sequential(ByteReadRequest::from(&request))
            .await?;
        open_reader(input, metadata, request.options, self.options.clone())
    }
}

#[async_trait]
impl ScanPlanner for ArchiveAccessor {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let metadata = self.metadata(request.clone()).await?;
        let accessor = self.clone();
        let output = metadata.clone();
        let partitions = stream::try_unfold(None::<Box<dyn RecordReader>>, move |reader| {
            let accessor = accessor.clone();
            let request = request.clone();
            let output = output.clone();
            async move {
                let mut reader = match reader {
                    Some(reader) => reader,
                    None => accessor.open(request).await?,
                };
                let Some(batch) = reader.next().await? else {
                    return Ok(None);
                };
                let partition = scan_partition(async move {
                    Ok(StreamReader::records(
                        stream::iter([Ok(batch)]),
                        output,
                        OrderRequirement::PreserveGlobal,
                    ))
                });
                Ok(Some((partition, Some(reader))))
            }
        });
        Ok(ScanPlan::from_stream(metadata, partitions).with_streaming_discovery())
    }
}
