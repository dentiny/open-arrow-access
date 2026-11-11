use crate::inference::SchemaInference;

use super::{
    inference::{self, JsonInferenceOptions},
    reader::json_reader,
};
use arrow_json::{ReaderBuilder, reader::Decoder};
use arrow_schema::ArrowError;
use open_arrow_access_core::{
    ByteReader, Capability, Error, ErrorKind, FactoryToken, OrderRequirement, ReadRequest,
    ReaderFactory, RecordCapability, RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
};

/// Read whitespace-separated JSON objects, including JSONL, with an explicit or inferred fixed schema.
/// Missing nullable fields become null; unknown fields are ignored. Top-level
/// arrays and scalar records are rejected. Bare `NaN`, `Infinity`, and `-Infinity`
/// values are accepted and decoded by Arrow as floating-point values.
#[derive(Clone, Debug)]
pub struct JsonFormat {
    schema: Option<SchemaRef>,
    inference: JsonInferenceOptions,
}

impl JsonFormat {
    /// Infer a schema before returning the reader, replaying the sample on the
    /// same input. Missing fields become nullable; types follow Arrow inference.
    pub fn infer() -> Self {
        Self {
            schema: None,
            inference: JsonInferenceOptions::default(),
        }
    }

    pub fn with_inference_options(mut self, options: JsonInferenceOptions) -> Self {
        self.inference = options;
        self
    }

    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema: Some(schema),
            inference: JsonInferenceOptions::default(),
        }
    }
}

#[async_trait]
impl SchemaInference for JsonFormat {
    async fn resolve_schema(
        &self,
        input: Box<dyn ByteReader>,
    ) -> Result<(Self, Box<dyn ByteReader>)> {
        if self.schema.is_some() {
            return Ok((self.clone(), input));
        }
        let (schema, input) = inference::infer(input, self.inference).await?;
        Ok((Self::new(schema), input))
    }
}

#[async_trait]
impl ReaderFactory for JsonFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = Option<(SchemaRef, Decoder)>;

    fn capability(&self, input: Capability) -> RecordCapability {
        Capability {
            read: input.read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    fn prepare(&self, request: &ReadRequest) -> Result<Self::Prepared> {
        let Some(input_schema) = &self.schema else {
            self.inference.validate()?;
            return Ok(None);
        };
        let options = &request.options;
        let (schema, _) = options.resolve_projection(input_schema)?;
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
        Ok(Some((schema, decoder)))
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        prepared: Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
        let (input, schema, decoder) = match prepared {
            Some((schema, decoder)) => (input, schema, decoder),
            None => {
                let (format, input) = self.resolve_schema(input).await?;
                let (schema, decoder) = format.prepare(&request)?.expect("resolved JSON schema");
                (input, schema, decoder)
            }
        };
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
