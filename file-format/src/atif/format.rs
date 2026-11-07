use super::reader::AtifReader;
use crate::FileFormat;
use atif_arrow::trajectory_schema;
use open_arrow_access_core::{
    ByteReader, ReadOptions, ReadRequest, RecordMetadata, RecordReader, Result, SchemaRef,
    async_trait,
};

/// The explicitly selected layout of ATIF documents; extensions are not inspected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtifFormat {
    /// One complete trajectory document, including pretty-printed JSON.
    Json,
    /// One complete trajectory per nonblank line. Arrays are not expanded.
    JsonLines,
}

#[async_trait]
impl FileFormat for AtifFormat {
    type Prepared = (SchemaRef, Option<Vec<usize>>);

    fn prepare(&self, options: &ReadOptions) -> Result<Self::Prepared> {
        options.resolve_projection(&trajectory_schema())
    }

    async fn open(
        &self,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        (schema, projection): Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(AtifReader::new(
            input,
            *self,
            request.options.batch_size.get(),
            projection,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
        )))
    }
}
