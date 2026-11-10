use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, FactoryToken, ReadRequest, ReaderFactory,
    RecordBatchBuilder, RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, SchemaRef,
    StreamReader, async_trait,
};

use super::{WarcFormat, WarcHeader};
use crate::FileAccessor;

/// Physical WARC records as Arrow rows. Bodies are unmodified LargeBinary;
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

pub fn arrow_schema() -> SchemaRef {
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

#[async_trait]
impl ReaderFactory for WarcArrowFormat {
    type Request = ReadRequest;
    type Prepared = SchemaRef;
    type Reader = dyn RecordReader;

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
        let output = schema.clone();
        let source_uri = request.object.locator().to_owned();
        let batches = stream::try_unfold(records, move |mut records| {
            let output = output.clone();
            let source_uri = source_uri.clone();
            async move {
                let mut batch = RecordBatchBuilder::new(
                    arrow_schema(),
                    Some(
                        output
                            .fields()
                            .iter()
                            .map(|field| field.name().clone())
                            .collect(),
                    ),
                )?;
                while batch.len() < batch_size {
                    let Some(record) = records.next().await? else {
                        break;
                    };
                    batch.push(|row| {
                        row.append("record_id", record.warc_id())?;
                        row.append("record_type", record.warc_type().as_str())?;
                        row.append("warc_version", record.warc_version())?;
                        row.append("date", record.date().to_rfc3339().as_str())?;
                        row.append("source_uri", source_uri.as_str())?;
                        row.append(
                            "target_uri",
                            record.header(WarcHeader::TargetURI).as_deref(),
                        )?;
                        row.append(
                            "content_type",
                            record.header(WarcHeader::ContentType).as_deref(),
                        )?;
                        row.append("body", record.body())
                    })?;
                }
                if batch.is_empty() {
                    return Ok(None);
                }
                Ok(Some((batch.finish()?, records)))
            }
        });
        Ok(Box::new(StreamReader::new(
            batches,
            RecordMetadata {
                schema,
                source: Some(source),
            },
        )))
    }
}

/// A physical WARC stream is indivisible without a verified record/compression index.
/// FileSet/Directory can schedule independent WARC files concurrently.
#[async_trait]
impl<B> ScanPlanner for WarcArrowAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        Ok(ScanPlan::whole_input(self.open(request).await?))
    }
}
