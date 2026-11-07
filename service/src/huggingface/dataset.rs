use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, Error, ErrorKind, ListRequest, ObjectRef, RecordReader,
    Result, async_trait,
};
use open_arrow_access_dataset::{FileSetAccessor, FileSetRequest};
use open_arrow_access_file_format::ParquetAccessor;

use super::{
    HuggingFaceAccessor, HuggingFaceDatasetRequest, HuggingFaceRepository, HuggingFaceService,
    validation::validate_path,
};

impl HuggingFaceService {
    /// Open one split as ordered Arrow batches from HF's published Parquet shards.
    /// Pin the export before listing so membership and reads use the same commit.
    /// Shards are sorted by path; record decoding and multi-file schema validation
    /// use the existing Parquet and file-set accessors, with lazy sequential reads.
    pub async fn dataset(
        &self,
        request: HuggingFaceDatasetRequest,
    ) -> Result<Box<dyn RecordReader>> {
        let repository =
            HuggingFaceRepository::new(&request.repository).with_revision(&request.revision);
        repository.validate()?;
        for component in [&request.config, &request.split] {
            validate_path(component, false)?;
            if component.contains('/') {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "open_huggingface_dataset",
                    "config and split must be individual path components",
                ));
            }
        }
        if request.split.starts_with("partial-") && !request.allow_partial {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "open_huggingface_dataset",
                "partial exports require explicit opt-in",
            ));
        }
        let repository = self.pin_revision(repository).await?;
        let accessor = self.repository(repository).await?;
        let prefix = format!("{}/{}", request.config, request.split);
        let objects = match parquet_files(&accessor, &prefix).await {
            Ok(objects) => objects,
            Err(error)
                if error.kind() == ErrorKind::NotFound
                    && request.allow_partial
                    && !request.split.starts_with("partial-") =>
            {
                parquet_files(
                    &accessor,
                    &format!("{}/partial-{}", request.config, request.split),
                )
                .await?
            }
            Err(error) => return Err(error),
        };
        FileSetAccessor::new(ParquetAccessor::new(accessor))
            .open(
                FileSetRequest::new(objects)
                    .with_options(request.options)
                    .with_byte_options(request.byte_options),
            )
            .await
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

async fn parquet_files(accessor: &HuggingFaceAccessor, prefix: &str) -> Result<Vec<ObjectRef>> {
    let mut listing = accessor
        .list(ListRequest::new(prefix).with_recursive(false))
        .await?;
    let prefix = format!("{prefix}/");
    let mut objects = Vec::new();
    while let Some(object) = listing.try_next().await? {
        let relative = object.locator().strip_prefix(&prefix).ok_or_else(|| {
            Error::new(
                ErrorKind::Decode,
                "resolve_huggingface",
                "listing returned a file outside the selected split",
            )
        })?;
        validate_path(relative, false)?;
        if !relative.contains('/') && relative.ends_with(".parquet") {
            objects.push(object);
        }
    }
    objects.sort_unstable_by(|a, b| a.locator().cmp(b.locator()));
    objects.dedup();
    if objects.is_empty() {
        return Err(Error::new(
            ErrorKind::NotFound,
            "resolve_huggingface",
            "selected split contains no Parquet shards",
        ));
    }
    Ok(objects)
}
