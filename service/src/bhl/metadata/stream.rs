use super::{BhlMetadata, BhlPageMetadata};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{Error, ObjectRef, Result};
use std::sync::Arc;

pub(super) fn pages(
    metadata: Arc<BhlMetadata>,
    keys: Vec<ObjectRef>,
) -> BoxStream<'static, Result<BhlPageMetadata>> {
    // Run the synchronous query on first read. V1 buffers this request's joined
    // metadata in memory before starting its independent OCR partitions.
    stream::once(async move {
        let mut rows = Vec::new();
        metadata.for_each(&keys, |page| {
            rows.push(page);
            Ok(())
        })?;
        Ok::<_, Error>(stream::iter(rows.into_iter().map(Ok)))
    })
    .try_flatten()
    .boxed()
}
