use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ReadRequest,
    RecordMetadata, RecordReader, Result, async_trait,
};

use super::{decode_error, reader::WarcReader, warc_schema};

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
            ..Capability::default()
        }
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let full_schema = warc_schema();
        let indices = match request.options.projection.as_ref() {
            None => (0..full_schema.fields().len()).collect(),
            Some(columns) => {
                let mut seen = HashSet::new();
                columns
                    .iter()
                    .map(|name| {
                        if !seen.insert(name) {
                            return Err(Error::new(
                                ErrorKind::InvalidInput,
                                "project",
                                format!("duplicate field {name:?}"),
                            ));
                        }
                        full_schema.index_of(name).map_err(|error| {
                            Error::new(
                                ErrorKind::InvalidInput,
                                "project",
                                format!("unknown field {name:?}"),
                            )
                            .with_source(error)
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };
        let schema = Arc::new(full_schema.project(&indices).map_err(decode_error)?);
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
