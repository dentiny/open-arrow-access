use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest,
    ReaderFactory, RecordCapability, RecordMetadata, RecordReader, Result, SchemaRef, StreamReader,
    async_trait,
};

use super::WarcHeader;
use super::batch::WarcBatchBuilder;
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
pub(super) const HEADER_COLUMNS: &[(&str, WarcHeader, DataType, bool)] = &[
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
    ("warc_info_id", WarcHeader::WarcInfoID, DataType::Utf8, true),
];

fn base_arrow_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let mut fields = vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("type", DataType::Utf8, false),
                Field::new("warc_version", DataType::Utf8, false),
                Field::new("date", date_type(), false),
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
    base_arrow_schema()
}

pub(super) fn body_view_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let fields = arrow_schema()
                .fields()
                .iter()
                .map(|field| {
                    if field.name() == "body" {
                        Arc::new(field.as_ref().clone().with_data_type(DataType::BinaryView))
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
        let input = open_arrow_access_core::into_async_read(input, "read_warc");
        let records = warc::WarcReader::new(input).iter_raw_records();
        let batch_size = request.options.batch_size.get();
        let output = schema.clone();
        let source_uri = request.object.locator().to_owned();
        let batches = stream::try_unfold(records, move |mut records| {
            let output = output.clone();
            let source_uri = source_uri.clone();
            async move {
                let mut batch = WarcBatchBuilder::new(output, batch_size)?;
                while batch.len() < batch_size {
                    let Some(record) = records.next().await else {
                        break;
                    };
                    let (headers, body) = record.map_err(super::decode_error)?;
                    batch.append(headers, body, &source_uri)?;
                }
                if batch.is_empty() {
                    return Ok(None);
                }
                Ok(Some((batch.finish()?, records)))
            }
        });
        let _ = token;
        Ok(Box::new(StreamReader::records(
            batches,
            RecordMetadata {
                schema,
                source: Some(source),
            },
            OrderRequirement::Reorderable,
        )))
    }
}
