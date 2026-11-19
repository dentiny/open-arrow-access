//! Typed S2ORC v2 records over an explicitly decompressed byte accessor.
mod decode;
mod schema;

use futures::{FutureExt, StreamExt, TryStreamExt};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ReadRequest, RecordCapability, RecordMetadata, RecordReader,
    ResolvedProjection, Result, ScanPlan, ScanPlanner, StreamReader, async_trait, into_stream,
};
use open_arrow_access_file_format::{
    DecodePartitionOptions, FileAccessor,
    json::{JsonAccessor, JsonFormat},
};
pub use schema::s2orc_v2_schema;
use std::sync::Arc;

/// Streaming typed reader. Input must already be decompressed JSONL.
/// All fields are decoded and validated before top-level projection. Native scan
/// planning retains the JSON framer's bounded chunks and parallel decode units.
pub struct S2orcV2Accessor<A> {
    input: JsonAccessor<Arc<A>>,
}

impl<A> S2orcV2Accessor<A> {
    pub fn new(bytes: A) -> Self {
        Self {
            input: FileAccessor::from_format(
                Arc::new(bytes),
                JsonFormat::new(schema::input_schema()).with_primitive_coercion(false),
            ),
        }
    }

    pub fn with_partition_options(mut self, options: DecodePartitionOptions) -> Self {
        self.input = self.input.with_partition_options(options);
        self
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for S2orcV2Accessor<A> {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.input.capability()
    }

    async fn metadata(&self, request: ReadRequest) -> Result<RecordMetadata> {
        Ok(RecordMetadata {
            schema: request
                .options
                .resolve_projection(&s2orc_v2_schema())?
                .schema,
            source: Some(request.object),
        })
    }

    async fn open(&self, mut request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let projection = request.options.resolve_projection(&s2orc_v2_schema())?;
        request.options.projection = None;
        let reader = self.input.open(request).await?;
        Ok(convert_reader(reader, projection))
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for S2orcV2Accessor<A> {
    async fn plan_scan(&self, mut request: ReadRequest) -> Result<ScanPlan> {
        let projection = request.options.resolve_projection(&s2orc_v2_schema())?;
        request.options.projection = None;
        let mut plan = self.input.plan_scan(request).await?;
        plan.metadata.schema = projection.schema.clone();
        plan.partitions = plan
            .partitions
            .map_ok(move |open| {
                let projection = projection.clone();
                async move { Ok(convert_reader(open.await?, projection)) }.boxed()
            })
            .boxed();
        Ok(plan)
    }
}

fn convert_reader(
    reader: Box<dyn RecordReader>,
    projection: ResolvedProjection,
) -> Box<dyn RecordReader> {
    let mut metadata = reader.metadata().clone();
    metadata.schema = projection.schema;
    let object = metadata.source.clone();
    let ordering = reader.capability().ordering;
    let batches = into_stream(reader).map(move |batch| {
        let batch = decode::batch(batch?).map_err(|error| error.with_object(object.as_ref()))?;
        match &projection.indices {
            Some(indices) => batch.project(indices).map_err(decode::arrow_error),
            None => Ok(batch),
        }
    });
    Box::new(StreamReader::records(batches, metadata, ordering))
}
