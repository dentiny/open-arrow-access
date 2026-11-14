use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::stream;
use open_arrow_access_core::{
    ByteReadRequest, ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest,
    ReaderFactory, RecordBatchBuilder, RecordCapability, RecordMetadata, RecordReader, Result,
    SchemaRef, StreamReader, async_trait,
};
use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};

use super::{WarcFormat, WarcHeader, WarcRecord};
use crate::FileAccessor;

/// Physical WARC records as Arrow rows. Bodies are unmodified LargeBinary;
/// dates default to millisecond timestamps without a timezone;
/// target URIs remain strings and are never fetched. No HTTP/entity decoding or
/// revisit resolution is performed. Use the native parser to inspect other headers.
#[derive(Clone, Copy, Debug, Default)]
pub struct WarcArrowFormat;

pub type WarcArrowAccessor<B> = FileAccessor<B, WarcArrowFormat>;

impl<B> FileAccessor<B, WarcArrowFormat> {
    pub fn new(inner: B) -> Self {
        Self::from_format(inner, WarcArrowFormat)
    }
}

fn raw_arrow_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("record_id", DataType::Utf8, false),
                Field::new("record_type", DataType::Utf8, false),
                Field::new("warc_version", DataType::Utf8, false),
                Field::new("date", DataType::Utf8, false),
                Field::new("source_uri", DataType::LargeUtf8, false),
                Field::new("target_uri", DataType::LargeUtf8, true),
                Field::new("content_type", DataType::Utf8, true),
                Field::new("body", DataType::LargeBinary, false),
            ]))
        })
        .clone()
}

/// Default WARC Arrow schema, including a non-null millisecond timestamp date.
pub fn arrow_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let fields = raw_arrow_schema()
                .fields()
                .iter()
                .map(|field| {
                    if field.name() == "date" {
                        Arc::new(field.as_ref().clone().with_data_type(date_type()))
                    } else {
                        field.clone()
                    }
                })
                .collect::<Vec<_>>();
            Arc::new(Schema::new(fields))
        })
        .clone()
}

fn date_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, /* timezone= */ None)
}

pub(super) fn evolve_reader(reader: Box<dyn RecordReader>) -> Result<Box<dyn RecordReader>> {
    if reader.metadata().schema.index_of("date").is_err() {
        return Ok(reader);
    }
    SchemaEvolutionLayer::try_new(vec![ColumnEvolution::change_data_type("date", date_type())])?
        .apply_to_reader(reader)
}

pub(super) fn raw_projection(output: &SchemaRef) -> Result<SchemaRef> {
    let names = output
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    open_arrow_access_core::ReadOptions {
        projection: Some(names),
        ..Default::default()
    }
    .resolve_projection(&raw_arrow_schema())
    .map(|(schema, _)| schema)
}

#[async_trait]
impl ReaderFactory for WarcArrowFormat {
    type Request = ReadRequest;
    type Prepared = SchemaRef;
    type Reader = dyn RecordReader;

    fn capability(&self, input: Capability) -> RecordCapability {
        Capability {
            read: input.read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    fn prepare(&self, request: &ReadRequest) -> Result<SchemaRef> {
        request
            .options
            .resolve_projection(&arrow_schema())
            .map(|(schema, _)| schema)
    }

    async fn create_reader(
        &self,
        token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        schema: SchemaRef,
    ) -> Result<Box<dyn RecordReader>> {
        let source = request.object.clone();
        let records = WarcFormat
            .create_reader(token, input, ByteReadRequest::from(&request), ())
            .await?;
        let batch_size = request.options.batch_size.get();
        let schema = raw_projection(&schema)?;
        let output = schema.clone();
        let source_uri = request.object.locator().to_owned();
        let batches = stream::try_unfold(records, move |mut records| {
            let output = output.clone();
            let source_uri = source_uri.clone();
            async move {
                let mut batch = new_batch(&output)?;
                while batch.len() < batch_size {
                    let Some(record) = records.next().await? else {
                        break;
                    };
                    append_record(&mut batch, &record, &source_uri)?;
                }
                if batch.is_empty() {
                    return Ok(None);
                }
                Ok(Some((batch.finish()?, records)))
            }
        });
        evolve_reader(Box::new(StreamReader::records(
            batches,
            RecordMetadata {
                schema,
                source: Some(source),
            },
            OrderRequirement::Reorderable,
        )))
    }
}

pub(super) fn new_batch(output: &SchemaRef) -> Result<RecordBatchBuilder> {
    RecordBatchBuilder::new(
        raw_arrow_schema(),
        Some(
            output
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
        ),
    )
}

pub(super) fn append_record(
    batch: &mut RecordBatchBuilder,
    record: &WarcRecord,
    source_uri: &str,
) -> Result<()> {
    batch.push(|row| {
        row.append("record_id", record.warc_id())?;
        row.append("record_type", record.warc_type().as_str())?;
        row.append("warc_version", record.warc_version())?;
        row.append("date", record.date().to_rfc3339().as_str())?;
        row.append("source_uri", source_uri)?;
        row.append(
            "target_uri",
            record.header(WarcHeader::TargetURI).as_deref(),
        )?;
        row.append(
            "content_type",
            record.header(WarcHeader::ContentType).as_deref(),
        )?;
        row.append("body", record.body())
    })
}
