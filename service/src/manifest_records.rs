use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, OrderRequirement, ReadRequest, RecordBatchBuilder, RecordCapability, RecordMetadata,
    RecordReader, Result, ScanPlan, ScanPlanner, SchemaRef, StreamReader, async_trait,
};

/// An owned, ordered snapshot of a resolved file namespace as Arrow records.
///
/// Obtain it with [`crate::ResolvedFileSet::manifest_records`]. Opening and reading only
/// access this snapshot: neither file bodies nor stat/HEAD metadata are fetched.
/// `ReadRequest::object` labels the output source; it does not select attachments.
/// Projection and batch size use the ordinary record-reader options.
#[derive(Clone, Debug)]
pub struct ManifestRecords {
    entries: Arc<[(String, Option<u64>)]>,
}

impl ManifestRecords {
    pub(crate) fn new(entries: Vec<(String, Option<u64>)>) -> Self {
        Self {
            entries: entries.into(),
        }
    }

    /// The full manifest schema, including when the namespace is empty.
    pub fn schema(&self) -> SchemaRef {
        manifest_schema()
    }
}

fn manifest_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("file_name", DataType::Utf8, false),
                Field::new("size", DataType::UInt64, true),
            ]))
        })
        .clone()
}

#[async_trait]
impl Accessor for ManifestRecords {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let (schema, _) = request.options.resolve_projection(&manifest_schema())?;
        let entries = self.entries.clone();
        let batches = stream::try_unfold(0, move |start| {
            let entries = entries.clone();
            let options = request.options.clone();
            async move {
                if start == entries.len() {
                    return Ok(None);
                }
                let end = start
                    .saturating_add(options.batch_size.get())
                    .min(entries.len());
                let mut builder = RecordBatchBuilder::new(manifest_schema(), options.projection)?;
                for (name, size) in &entries[start..end] {
                    builder.push(|row| {
                        row.append("file_name", name.as_str())?;
                        row.append("size", *size)
                    })?;
                }
                Ok(Some((builder.finish()?, end)))
            }
        });
        Ok(Box::new(StreamReader::records(
            batches,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            OrderRequirement::PreserveGlobal,
        )))
    }
}

#[async_trait]
impl ScanPlanner for ManifestRecords {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        Ok(ScanPlan::whole_input(self.open(request).await?))
    }
}
