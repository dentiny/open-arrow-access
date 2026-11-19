use crate::{
    inference::{ResolvedFormat, SchemaInference},
    preparation::{PreparedProjection, prepare_projection},
};

use super::{
    inference::{self, JsonInferenceOptions},
    reader::json_reader,
};
use arrow_json::{ReaderBuilder, reader::Decoder};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest,
    ReaderFactory, RecordCapability, RecordMetadata, RecordReader, ResolvedProjection, Result,
    SchemaRef, async_trait, into_stream, record_stream_reader,
};

/// Read whitespace-separated JSON objects, including JSONL, with an explicit or inferred fixed schema.
/// Missing nullable fields become null; unknown fields (including nested fields)
/// are rejected. The full input schema is decoded before output projection, so
/// unselected columns also undergo type and nullability checks. Top-level
/// arrays and scalar records are rejected. Bare `NaN`, `Infinity`, and `-Infinity`
/// values are accepted and decoded by Arrow as floating-point values.
/// String fields also accept numbers and booleans; numeric text is preserved.
#[derive(Clone, Debug)]
pub struct JsonFormat {
    schema: Option<SchemaRef>,
    inference: JsonInferenceOptions,
    coerce_primitive: bool,
}

/// Validated JSON decoder and its source schema for one open request.
pub struct PreparedJsonReader {
    pub schema: SchemaRef,
    pub decoder: Decoder,
}

impl JsonFormat {
    /// Infer a schema before returning the reader, replaying the sample on the
    /// same input. Missing fields become nullable; types follow Arrow inference.
    pub fn infer() -> Self {
        Self {
            schema: None,
            inference: JsonInferenceOptions::default(),
            coerce_primitive: true,
        }
    }

    pub fn with_inference_options(mut self, options: JsonInferenceOptions) -> Self {
        self.inference = options;
        self
    }

    /// Whether numeric and boolean values may be coerced into string fields.
    /// Defaults to true for compatibility. Fixed provider schemas can disable
    /// coercion to reject upstream type drift instead of changing its meaning.
    pub fn with_primitive_coercion(mut self, enabled: bool) -> Self {
        self.coerce_primitive = enabled;
        self
    }

    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema: Some(schema),
            inference: JsonInferenceOptions::default(),
            coerce_primitive: true,
        }
    }

    fn prepare_projection(&self, request: &ReadRequest) -> Result<Option<PreparedProjection>> {
        let Some(input_schema) = &self.schema else {
            self.inference.validate()?;
            return Ok(None);
        };
        prepare_projection(input_schema, &request.options).map(Some)
    }

    pub(super) fn prepare_scan(&self, request: &ReadRequest) -> Result<Option<PreparedProjection>> {
        let prepared = self.prepare_projection(request)?;
        if let Some(schema) = &self.schema {
            // Arrow exposes schema validation only through decoder construction.
            // Keep nested type/timezone errors before storage I/O and empty scans.
            self.build_decoder(schema.clone(), request)?;
        }
        Ok(prepared)
    }

    fn build_decoder(&self, schema: SchemaRef, request: &ReadRequest) -> Result<Decoder> {
        ReaderBuilder::new(schema)
            .with_strict_mode(true)
            .with_coerce_primitive(self.coerce_primitive)
            .with_batch_size(request.options.batch_size.get())
            .build_decoder()
            .map_err(super::error::open_error)
    }

    fn decoder(
        &self,
        prepared: PreparedProjection,
        request: &ReadRequest,
    ) -> Result<PreparedJsonReader> {
        // Strict decoding must recognize known fields omitted from the output.
        let schema = self.schema.clone().unwrap_or(prepared.schema);
        let decoder = self.build_decoder(schema.clone(), request)?;
        Ok(PreparedJsonReader { schema, decoder })
    }
}

#[async_trait]
impl SchemaInference for JsonFormat {
    async fn resolve_schema(
        &self,
        input: Box<dyn ByteReader>,
    ) -> Result<Option<ResolvedFormat<Self>>> {
        if self.schema.is_some() {
            return Ok(Some(ResolvedFormat {
                format: self.clone(),
                input,
            }));
        }
        let Some(inference::InferredJson { schema, input }) =
            inference::infer(input, self.inference).await?
        else {
            return Ok(None);
        };
        Ok(Some(ResolvedFormat {
            format: Self::new(schema).with_primitive_coercion(self.coerce_primitive),
            input,
        }))
    }
}

#[async_trait]
impl ReaderFactory for JsonFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = Option<PreparedJsonReader>;

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

    async fn metadata(
        &self,
        storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: Self::Request,
    ) -> Result<RecordMetadata> {
        Ok(self
            .inspect_metadata(storage, request.clone())
            .await?
            .unwrap_or_else(|| crate::inference::empty_plan(request).metadata))
    }

    async fn inspect_metadata(
        &self,
        storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: Self::Request,
    ) -> Result<Option<RecordMetadata>> {
        let prepared = self.prepare_projection(&request)?;
        let schema = if let Some(prepared) = prepared {
            prepared.schema
        } else {
            let input = storage.open(ByteReadRequest::from(&request)).await?;
            let Some(resolved) = self.resolve_schema(input).await? else {
                return Ok(None);
            };
            resolved
                .format
                .prepare_projection(&request)?
                .expect("resolved JSON schema")
                .schema
        };
        Ok(Some(RecordMetadata {
            schema,
            source: Some(request.object),
        }))
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        prepared: Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
        let (input, schema, decoder) = match prepared {
            Some(PreparedJsonReader { schema, decoder }) => (input, schema, decoder),
            None => {
                let Some(ResolvedFormat { format, input }) = self.resolve_schema(input).await?
                else {
                    return Ok(Box::new(
                        crate::inference::empty_plan(request).into_reader(),
                    ));
                };
                let PreparedJsonReader { schema, decoder } =
                    format.prepare(&request)?.expect("resolved JSON schema");
                (input, schema, decoder)
            }
        };
        let ResolvedProjection {
            schema: output_schema,
            indices: projection,
        } = request.options.resolve_projection(&schema)?;
        let metadata = RecordMetadata {
            schema,
            source: Some(request.object),
        };
        let reader = json_reader(
            input,
            decoder,
            metadata.clone(),
            request.options.batch_size.get(),
        );
        Ok(Box::new(record_stream_reader(
            into_stream(Box::new(reader)),
            RecordMetadata {
                schema: output_schema,
                ..metadata
            },
            projection,
            OrderRequirement::Reorderable,
        )))
    }
}
