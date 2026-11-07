use atif_arrow::trajectory_schema;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, ReadRequest, RecordMetadata, RecordReader,
    Result, async_trait,
};

use super::reader::AtifReader;

/// The explicitly selected layout of ATIF documents; extensions are not inspected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtifFormat {
    /// One complete trajectory document, including pretty-printed JSON.
    Json,
    /// One complete trajectory per nonblank line. Arrays are not expanded.
    JsonLines,
}

/// Read ATIF v1.0–v1.8 with the fixed `atif-arrow` schema, one trajectory per row.
///
/// Consumes sequential bytes from any byte accessor. Whole documents are buffered;
/// batch size limits rows, not bytes. Projection selects top-level output columns
/// after validation and conversion of the entire trajectory.
#[derive(Clone, Debug)]
pub struct AtifAccessor<A> {
    inner: A,
    format: AtifFormat,
}

impl<A> AtifAccessor<A> {
    pub fn new(inner: A, format: AtifFormat) -> Self {
        Self { inner, format }
    }
}

#[async_trait]
impl<A> Accessor for AtifAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let (schema, projection) = request.options.resolve_projection(&trajectory_schema())?;
        let input = self
            .inner
            .open(ByteReadRequest::new(request.object.clone()).with_options(request.byte_options))
            .await?;
        Ok(Box::new(AtifReader::new(
            input,
            self.format,
            request.options.batch_size.get(),
            projection,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
        )))
    }
}
