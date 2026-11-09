use super::reader::json_reader;
use arrow_json::{ReaderBuilder, reader::Decoder};
use arrow_schema::ArrowError;
use open_arrow_access_core::{
    ByteReader, Error, ErrorKind, FactoryToken, ReadRequest, ReaderFactory, RecordMetadata,
    RecordReader, Result, SchemaRef, async_trait,
};

/// Read whitespace-separated JSON objects, including JSONL, with a fixed schema.
/// Missing nullable fields become null; unknown fields are ignored. Top-level
/// arrays and scalar records are rejected.
#[derive(Clone, Debug)]
pub struct JsonFormat {
    schema: SchemaRef,
}

impl JsonFormat {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl ReaderFactory for JsonFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = (SchemaRef, Decoder);

    fn prepare(&self, request: &ReadRequest) -> Result<Self::Prepared> {
        let options = &request.options;
        let (schema, _) = options.resolve_projection(&self.schema)?;
        let decoder = ReaderBuilder::new(schema.clone())
            .with_batch_size(options.batch_size.get())
            .build_decoder()
            .map_err(|error| {
                let kind = match &error {
                    ArrowError::NotYetImplemented(_) => ErrorKind::Unsupported,
                    _ => ErrorKind::InvalidInput,
                };
                Error::new(kind, "open_json", error.to_string()).with_source(error)
            })?;
        Ok((schema, decoder))
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        (schema, decoder): Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(json_reader(
            input,
            decoder,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            request.options.batch_size.get(),
        )))
    }
}
