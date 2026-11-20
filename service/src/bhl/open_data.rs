//! Select independent objects from BHL's public AWS dataset or a matching mirror.
use std::sync::Arc;

use open_arrow_access_core::{ByteAccessor, ListRequest, ObjectRef, Result, list_files};
use opendal::{Operator, services::S3};

use super::{error, validation};
use crate::{ResolvedFileSet, file_manifest::FileSpec};

pub const OPEN_DATA_BUCKET_URL: &str = "s3://bhl-open-data/";

/// Anonymous bucket-root storage in us-east-2, without ambient AWS credentials.
/// No I/O occurs here; callers can apply OpenDAL retry/timeout/metrics layers.
pub fn open_data_bucket_operator() -> Result<Operator> {
    opendal::install_default();
    Operator::new(
        S3::default()
            .bucket("bhl-open-data")
            .region("us-east-2")
            .disable_config_load()
            .skip_signature(),
    )
    .map_err(error::configure_open_data)
}

/// Caller-bound bucket-root storage with explicit, independent file membership.
///
/// Discovery and partition selection are separate: persist discovered keys before
/// distributing jobs. Neither method makes the live bucket an immutable snapshot.
/// A mirror must retain bucket-relative ocr/images/scandata/data keys.
pub struct BhlOpenData<A> {
    storage: Arc<A>,
}

impl<A> Clone for BhlOpenData<A> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}

impl<A> BhlOpenData<A> {
    pub fn new(storage: A) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }

    /// Bind exactly these unique bucket-relative object keys, in caller order.
    ///
    /// No listing, metadata request or body download occurs. The returned file
    /// metadata is the selected key vector; listing its empty root is cached.
    /// Unknown logical names cannot escape this selection. Byte reads remain lazy.
    /// Bind a format and the shared file-set planner to obtain one native scan
    /// partition per selected object; this does not implement a BHL metadata join.
    pub fn partition(
        &self,
        objects: impl IntoIterator<Item = impl Into<ObjectRef>>,
    ) -> Result<ResolvedFileSet<Arc<A>, Vec<ObjectRef>>> {
        let objects = objects.into_iter().map(Into::into).collect::<Vec<_>>();
        validation::validate_objectds(&objects, false)?;
        let files = objects.iter().cloned().map(|object| FileSpec {
            name: object.locator().to_owned(),
            object,
            content_length: None,
        });
        ResolvedFileSet::from_files(self.storage.clone(), files, objects.clone())
    }
}

impl<A: ByteAccessor> BhlOpenData<A> {
    /// Discover sorted, unique objects beneath a directory prefix, without bodies.
    ///
    /// Prefixes start with ocr/, images/, scandata/ or data/. Recursion is forwarded
    /// unchanged. Memory is proportional to selected key count; prefer item/part
    /// prefixes for bounded discovery. Use partition() with saved keys on workers
    /// to avoid repeated listing and unrelated object reads.
    pub async fn discover(&self, request: ListRequest) -> Result<Vec<ObjectRef>> {
        validation::validate_open_data_prefix(request.prefix.locator())?;
        let objects = list_files(
            self.storage.as_ref(),
            request,
            validation::select_open_data_file,
        )
        .await?;
        validation::validate_objectds(&objects, true)?;
        Ok(objects)
    }
}
