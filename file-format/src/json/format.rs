use crate::{inference::SchemaInference, preparation::PreparedProjection};

use super::{
    inference::{self, JsonInferenceOptions},
    reader::json_reader,
};
use arrow_json::{ReaderBuilder, reader::Decoder};
use open_arrow_access_core::{
    ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest, ReaderFactory,
    RecordCapability, RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
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

    fn prepare_projection(&self, request: &ReadRequest) -> Result<Option<PreparedProjection>> {
        let Some(input_schema) = &self.schema else {
            self.inference.validate()?;
            return Ok(None);
        };
        PreparedProjection::new(input_schema, &request.options).map(Some)
    }

    pub(super) fn prepare_scan(&self, request: &ReadRequest) -> Result<Option<PreparedProjection>> {
        let prepared = self.prepare_projection(request)?;
        if let Some(prepared) = &prepared {
            // Arrow exposes schema validation only through decoder construction.
            // Keep nested type/timezone errors before storage I/O and empty scans.
            self.build_decoder(prepared.schema.clone(), request)?;
        }
        Ok(prepared)
    }

    fn build_decoder(&self, schema: SchemaRef, request: &ReadRequest) -> Result<Decoder> {
        ReaderBuilder::new(schema)
            .with_batch_size(request.options.batch_size.get())
            .build_decoder()
            .map_err(super::error::open_error)
    }

    fn decoder(
        &self,
        prepared: PreparedProjection,
        request: &ReadRequest,
    ) -> Result<(SchemaRef, Decoder)> {
        let decoder = self.build_decoder(prepared.schema.clone(), request)?;
        Ok((prepared.schema, decoder))
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
        self.prepare_projection(request)?
            .map(|prepared| self.decoder(prepared, request))
            .transpose()
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
