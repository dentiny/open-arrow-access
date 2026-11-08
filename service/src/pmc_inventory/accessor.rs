use super::{manifest, reader::InventoryTransform};
use arrow_schema::Schema;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, ReadRequest, RecordReader, Result,
    async_trait,
};
use std::{collections::HashMap, sync::Arc};

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
    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, mut request: ReadRequest) -> Result<Box<dyn RecordReader>> {
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
            for name in ["Bucket", "Key"] {
                if !csv_columns.iter().any(|column| column == name) {
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
