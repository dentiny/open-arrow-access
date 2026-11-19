//! Select one inventory shard and read the article metadata it references.
use super::{PmcAccessor, dataset::article_plan, schema};
use crate::pmc_inventory::{PmcInventoryAccessor, validate_shard_selection};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ObjectRef, ReadOptions, ReadOptionsProvider,
    ReadRequest, RecordCapability, RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner,
    async_trait,
};
use std::sync::Arc;

/// One exact shard from a fixed inventory manifest. Locators are resolved by storage.
#[derive(Clone, Debug)]
pub struct PmcInventoryShardRequest {
    manifest: ObjectRef,
    shard: ObjectRef,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}
impl PmcInventoryShardRequest {
    pub fn new(manifest: impl Into<ObjectRef>, shard: impl Into<ObjectRef>) -> Self {
        Self {
            manifest: manifest.into(),
            shard: shard.into(),
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        }
    }
    pub fn manifest(&self) -> &ObjectRef {
        &self.manifest
    }
    pub fn shard(&self) -> &ObjectRef {
        &self.shard
    }
    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }
    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.byte_options = options;
        self
    }
}
impl ReadOptionsProvider for PmcInventoryShardRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }

    fn byte_read_options_mut(&mut self) -> Option<&mut ByteReadOptions> {
        Some(&mut self.byte_options)
    }
}

/// Yield the ordinary typed PMC article schema, not inventory rows.
/// Reads only the selected CSV shard, then lazily opens its metadata JSON objects.
/// PDF/XML/media URLs remain values for downstream blob ingestion.
pub struct PmcInventoryShardAccessor<A> {
    storage: Arc<A>,
}
impl<A> PmcInventoryShardAccessor<A> {
    pub fn new(storage: A) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }
}
#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for PmcInventoryShardAccessor<A> {
    type Request = PmcInventoryShardRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        PmcAccessor::with_default_decoder(self.storage.clone()).capability()
    }
    async fn metadata(&self, request: Self::Request) -> Result<RecordMetadata> {
        validate_shard_selection(&request.shard)?;
        Ok(RecordMetadata {
            schema: request.options.resolve_projection(&schema())?.schema,
            source: None,
        })
    }
    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}
#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for PmcInventoryShardAccessor<A> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        // Validate the article projection before any manifest/shard I/O.
        self.metadata(request.clone()).await?;
        let inventory =
            PmcInventoryAccessor::new(self.storage.clone()).with_shard(request.shard.clone());
        let paths = inventory
            .metadata_paths(
                ReadRequest::new(request.manifest)
                    .with_options(request.options.clone())
                    .with_byte_options(request.byte_options),
            )
            .await?;
        article_plan(
            self.storage.clone(),
            ReadRequest::new(request.shard)
                .with_options(request.options)
                .with_byte_options(request.byte_options),
            paths,
            super::PmcFormat::default(),
        )
    }
}
