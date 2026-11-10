use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, ListRequest, ObjectRef, RecordReader, Result, ScanPlan,
    ScanPlanner, async_trait, list_files,
};
use open_arrow_access_dataset::file_set::{FileSetAccessor, FileSetRequest};
use open_arrow_access_file_format::parquet::ParquetAccessor;

use super::{
    HuggingFaceAccessor, HuggingFaceDatasetRequest, HuggingFaceRepository, HuggingFaceService,
    validation::validate_dataset_request,
};

impl HuggingFaceService {
    /// Open one split as ordered Arrow batches from HF's published Parquet shards.
    /// Pin the export before listing so membership and reads use the same commit.
    /// Read shards lazily in path order, validating schemas across files.
    pub async fn dataset(
        &self,
        request: HuggingFaceDatasetRequest,
    ) -> Result<Box<dyn RecordReader>> {
        let (files, request) = self.resolve_dataset(request).await?;
        files.open(request).await
    }

    async fn resolve_dataset(
        &self,
        request: HuggingFaceDatasetRequest,
    ) -> Result<(
        FileSetAccessor<ParquetAccessor<HuggingFaceAccessor>>,
        FileSetRequest,
    )> {
        let repository =
            HuggingFaceRepository::new(&request.repository).with_revision(&request.revision);
        validate_dataset_request(&request, &repository)?;
        let repository = self.pin_revision(repository).await?;
        let accessor = self.repository(repository).await?;
        let prefix = format!("{}/{}", request.config, request.split);
        let objects = match accessor.list_parquet_files(&prefix).await {
            Ok(objects) => objects,
            Err(error)
                if error.kind() == ErrorKind::NotFound
                    && request.allow_partial
                    && !request.split.starts_with("partial-") =>
            {
                accessor
                    .list_parquet_files(&format!("{}/partial-{}", request.config, request.split))
                    .await?
            }
            Err(error) => return Err(error),
        };
        Ok((
            FileSetAccessor::new(ParquetAccessor::new(accessor)).with_file_planning(),
            FileSetRequest::new(objects)
                .with_options(request.options)
                .with_byte_options(request.byte_options),
        ))
    }
}

#[async_trait]
impl Accessor for HuggingFaceService {
    type Request = HuggingFaceDatasetRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        self.dataset(request).await
    }
}

impl HuggingFaceAccessor {
    async fn list_parquet_files(&self, prefix: &str) -> Result<Vec<ObjectRef>> {
        let objects = list_files(
            self,
            ListRequest::new(prefix).with_recursive(false),
            |relative| Ok(!relative.contains('/') && relative.ends_with(".parquet")),
        )
        .await?;
        if objects.is_empty() {
            return Err(Error::new(
                ErrorKind::NotFound,
                "resolve_huggingface",
                "selected split contains no Parquet shards",
            ));
        }
        Ok(objects)
    }
}

#[async_trait]
impl ScanPlanner for HuggingFaceService {
    async fn plan_scan(&self, request: HuggingFaceDatasetRequest) -> Result<ScanPlan> {
        let (files, request) = self.resolve_dataset(request).await?;
        files.plan_scan(request).await
    }
}
