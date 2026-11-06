use std::num::NonZeroUsize;

use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, ReadRequest, RecordMetadata, RecordReader,
    Result, async_trait,
};

use super::{reader::WarcReader, warc_schema};

/// Default body-byte target per batch; one larger record may exceed it.
pub const DEFAULT_BATCH_BODY_BYTES: NonZeroUsize = NonZeroUsize::new(16 * 1024 * 1024).unwrap();

/// Read physical WARC records in input order, using the warc-rs async parser.
/// Every record type is included; continuation records remain separate rows.
#[derive(Clone, Debug)]
pub struct WarcAccessor<A> {
    inner: A,
    batch_body_bytes: NonZeroUsize,
}

impl<A> WarcAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner,
            batch_body_bytes: DEFAULT_BATCH_BODY_BYTES,
        }
    }

    /// End a batch after this many body bytes, independently of its row target.
    /// This is not a maximum record size or a total-memory limit.
    pub fn with_batch_body_bytes(mut self, bytes: NonZeroUsize) -> Self {
        self.batch_body_bytes = bytes;
        self
    }
}

#[async_trait]
impl<A> Accessor for WarcAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            list: false,
        }
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let full_schema = warc_schema();
        let (schema, projection) = request.options.resolve_projection(&full_schema)?;
        let indices = projection.unwrap_or_else(|| (0..full_schema.fields().len()).collect());
        let input = self
            .inner
            .open(ByteReadRequest::new(request.object.clone()).with_options(request.byte_options))
            .await?;
        Ok(Box::new(WarcReader::new(
            input,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            indices,
            request.options.batch_size.get(),
            self.batch_body_bytes.get(),
        )))
    }
}
