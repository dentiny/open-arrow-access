use super::{manifest, reader::InventoryTransform, validation};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, ReadRequest, RecordReader, Result,
    async_trait,
};
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

/// Read the gzip CSV shards listed in an inventory manifest.
pub struct PmcInventoryAccessor<A> {
    inner: Arc<A>,
    manifest_limit: NonZeroUsize,
}
impl<A> Clone for PmcInventoryAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            manifest_limit: self.manifest_limit,
        }
    }
}
impl<A> PmcInventoryAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
            manifest_limit: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
        }
    }
    pub fn with_manifest_limit(mut self, limit: NonZeroUsize) -> Self {
        self.manifest_limit = limit;
        self
    }
}
#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for PmcInventoryAccessor<A> {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        validation::validate_manifest_locator(request.object.locator())?;
        let (manifest, raw) = manifest::read(
            self.inner.as_ref(),
            ByteReadRequest::new(request.object.clone()).with_options(request.byte_options),
            self.manifest_limit.get(),
        )
        .await?;
        let names = validation::validate_fields(&manifest.file_schema)?;
        let csv_schema = Arc::new(Schema::new(
            names
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let mut fields: Vec<_> = csv_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("metadata_key", DataType::Utf8, false)));
        let metadata = HashMap::from([
            (
                "pmc.inventory.manifest_uri".into(),
                request.object.locator().to_owned(),
            ),
            (
                "pmc.inventory.creation_timestamp".into(),
                manifest.creation_timestamp,
            ),
            ("pmc.inventory.version".into(), manifest.version),
            ("pmc.inventory.raw_manifest_json".into(), raw),
        ]);
        let full_schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        let (output_schema, projection) = request.options.resolve_projection(&full_schema)?;
        let transform = InventoryTransform {
            csv_schema,
            projection: projection.unwrap_or_else(|| (0..full_schema.fields().len()).collect()),
            schema: output_schema,
        };
        Ok(Box::new(
            transform.create_reader(
                self.inner.clone(),
                manifest
                    .files
                    .into_iter()
                    .map(|file| file.key.into())
                    .collect(),
                request,
            ),
        ))
    }
}
