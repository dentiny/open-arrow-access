use super::{manifest, reader::InventoryTransform};
use arrow_schema::Schema;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, ObjectRef, OrderRequirement, ReadRequest,
    RecordCapability, RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner,
    async_trait,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Read the gzip CSV shards listed in an inventory manifest.
pub struct PmcInventoryAccessor<A> {
    inner: Arc<A>,
}
impl<A> Clone for PmcInventoryAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<A> PmcInventoryAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}
#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for PmcInventoryAccessor<A> {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let (transform, files, request) = self.prepare_scan(request).await?;
        Ok(Box::new(transform.create_reader(
            self.inner.clone(),
            files,
            request,
        )))
    }
}

impl<A: ByteAccessor + 'static> PmcInventoryAccessor<A> {
    async fn prepare_scan(
        &self,
        mut request: ReadRequest,
    ) -> Result<(InventoryTransform, Vec<ObjectRef>, ReadRequest)> {
        let manifest = manifest::read(
            self.inner.as_ref(),
            ByteReadRequest::new(request.object.clone()).with_options(request.byte_options),
        )
        .await?;
        let csv_schema = manifest.file_schema;
        let metadata = HashMap::from([
            (
                "pmc.inventory.manifest_uri".into(),
                request.object.locator().to_owned(),
            ),
            (
                "pmc.inventory.creation_timestamp".into(),
                manifest.creation_timestamp.to_string(),
            ),
            ("pmc.inventory.version".into(), manifest.version),
        ]);
        let full_schema = Arc::new(Schema::new_with_metadata(
            csv_schema.fields().clone(),
            metadata,
        ));
        let (output_schema, _) = request.options.resolve_projection(&full_schema)?;
        let output_projection = request.options.projection.as_mut().map(|csv_columns| {
            let output_indices = (0..csv_columns.len()).collect();
            // Output columns come first; validation-only columns are appended.
            let mut names: HashSet<_> = csv_columns.iter().cloned().collect();
            for name in ["Bucket", "Key"] {
                if names.insert(name.to_owned()) {
                    csv_columns.push(name.into());
                }
            }
            output_indices
        });
        let transform = InventoryTransform {
            csv_schema,
            output_projection,
            output_schema,
        };
        Ok((
            transform,
            manifest
                .files
                .into_iter()
                .map(|file| file.key.into())
                .collect(),
            request,
        ))
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for PmcInventoryAccessor<A> {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let (transform, files, request) = self.prepare_scan(request).await?;
        let metadata = RecordMetadata {
            schema: transform.output_schema.clone(),
            source: Some(request.object.clone()),
        };
        let source = self.inner.clone();
        let partitions = files.into_iter().map(move |file| {
            let transform = transform.clone();
            let source = source.clone();
            let request = request.clone();
            Box::pin(async move {
                Ok(
                    Box::new(transform.create_reader(source, vec![file], request))
                        as Box<dyn RecordReader>,
                )
            }) as ScanPartition
        });
        Ok(
            ScanPlan::from_stream(metadata, stream::iter(partitions.map(Ok)))
                .with_ordering(self.capability().ordering),
        )
    }
}
