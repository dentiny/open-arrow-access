use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest,
    ReaderFactory, RecordBatchBuilder, RecordCapability, RecordMetadata, RecordReader, Result,
    SchemaRef, StreamReader, async_trait,
};
use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};

use super::{WarcFormat, WarcHeader, WarcRecord};
use crate::FileAccessor;

/// Physical WARC records as Arrow rows. Bodies are unmodified LargeBinary;
/// dates default to millisecond timestamps without a timezone;
/// target URIs remain strings and are never fetched. No HTTP/entity decoding or
/// revisit resolution is performed. Standard WARC headers are exposed as strings;
/// use the native parser for custom headers.
#[derive(Clone, Copy, Debug, Default)]
pub struct WarcArrowFormat;

pub type WarcArrowAccessor<B> = FileAccessor<B, WarcArrowFormat>;

impl<B> FileAccessor<B, WarcArrowFormat> {
    pub fn new(inner: B) -> Self {
        Self::from_format(inner, WarcArrowFormat)
    }
}

// Header mapping shared by the physical schema and row population.
const HEADER_COLUMNS: &[(&str, WarcHeader, DataType, bool)] = &[
    (
        "target_uri",
        WarcHeader::TargetURI,
        DataType::LargeUtf8,
        true,
    ),
    (
        "content_type",
        WarcHeader::ContentType,
        DataType::Utf8,
        true,
    ),
    (
        "content_length",
        WarcHeader::ContentLength,
        DataType::Utf8,
        false,
    ),
    (
        "block_digest",
        WarcHeader::BlockDigest,
        DataType::Utf8,
        true,
    ),
    (
        "payload_digest",
        WarcHeader::PayloadDigest,
        DataType::Utf8,
        true,
    ),
    (
        "concurrent_to",
        WarcHeader::ConcurrentTo,
        DataType::Utf8,
        true,
    ),
    ("filename", WarcHeader::Filename, DataType::Utf8, true),
    (
        "identified_payload_type",
        WarcHeader::IdentifiedPayloadType,
        DataType::Utf8,
        true,
    ),
    ("ip_address", WarcHeader::IPAddress, DataType::Utf8, true),
    ("profile", WarcHeader::Profile, DataType::Utf8, true),
    ("refers_to", WarcHeader::RefersTo, DataType::Utf8, true),
    (
        "segment_number",
        WarcHeader::SegmentNumber,
        DataType::Utf8,
        true,
    ),
    (
        "segment_origin_id",
        WarcHeader::SegmentOriginID,
        DataType::Utf8,
        true,
    ),
    (
        "segment_total_length",
        WarcHeader::SegmentTotalLength,
        DataType::Utf8,
        true,
    ),
    ("truncated", WarcHeader::Truncated, DataType::Utf8, true),
    ("warcinfo_id", WarcHeader::WarcInfoID, DataType::Utf8, true),
];

fn raw_arrow_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let mut fields = vec![
                Field::new("record_id", DataType::Utf8, false),
                Field::new("record_type", DataType::Utf8, false),
                Field::new("warc_version", DataType::Utf8, false),
                Field::new("date", DataType::Utf8, false),
                Field::new("source_uri", DataType::LargeUtf8, false),
            ];
            fields.extend(HEADER_COLUMNS.iter().map(|(name, _, data_type, nullable)| {
                Field::new(*name, data_type.clone(), *nullable)
            }));
            // Keep the public schema's body position, before content_length.
            fields.insert(7, Field::new("body", DataType::LargeBinary, false));
            Arc::new(Schema::new(fields))
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

pub(super) fn date_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, /*timezone=*/ None)
}

pub(super) fn evolve_reader(reader: Box<dyn RecordReader>) -> Result<Box<dyn RecordReader>> {
    if reader.metadata().schema.index_of("date").is_err() {
        return Ok(reader);
    }
    SchemaEvolutionLayer::try_new(vec![ColumnEvolution::change_data_type("date", date_type())])?
        .wrap_reader(reader)
}

pub(super) fn raw_projection(output: &SchemaRef) -> Result<SchemaRef> {
    let names = output
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    open_arrow_access_core::ResolvedProjection::resolve(&raw_arrow_schema(), Some(&names))
        .map(|projection| projection.schema)
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
            .map(|projection| projection.schema)
    }

    async fn metadata(
        &self,
        _storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: Self::Request,
    ) -> Result<RecordMetadata> {
        Ok(RecordMetadata {
            schema: self.prepare(&request)?,
            source: Some(request.object),
        })
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
        for (name, header, _, _) in HEADER_COLUMNS {
            row.append(name, record.header(header.clone()).as_deref())?;
        }
        row.append("body", record.body())
    })
}
