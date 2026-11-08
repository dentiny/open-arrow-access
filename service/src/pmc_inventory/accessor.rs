use super::{manifest, reader::InventoryTransform, validation};
use arrow_schema::Schema;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, ReadRequest, RecordReader, Result,
    async_trait,
};
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

const DEFAULT_MANIFEST_BYTES: NonZeroUsize = NonZeroUsize::new(4 * 1024 * 1024).unwrap();

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
            manifest_limit: DEFAULT_MANIFEST_BYTES,
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
        let manifest = manifest::read(
            self.inner.as_ref(),
            ByteReadRequest::new(request.object.clone()).with_options(request.byte_options),
            self.manifest_limit.get(),
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
        let (output_schema, projection) = request.options.resolve_projection(&full_schema)?;
        let transform = InventoryTransform {
            csv_schema,
            projection,
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
