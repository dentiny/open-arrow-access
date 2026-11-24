//! Keep inline text/XML/HTML and expose document URLs for sink-side downloads.
use super::{
    CourtListenerDatePartitionAccessor,
    error::{arrow_error, records_error, schema_error},
    schema::document_fields,
    validation::{http_url, validate_media_root, validate_relative_document_path},
};
use crate::arrow_utils::{struct_field, struct_string_value};
use arrow_array::{ArrayRef, RecordBatch, StringArray, StructArray};
use arrow_schema::SchemaRef;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, OrderRequirement, ReadRequest, RecordCapability, RecordMetadata,
    RecordReader, Result, ScanPlan, ScanPlanner, async_trait, record_stream_reader,
};
use std::sync::Arc;
use url::Url;

/// A joined date stream with all original fields plus document URLs.
/// This accessor never opens document URLs; a sink owns downloads and blob writes.
pub struct CourtListenerDocumentAccessor<A> {
    source: CourtListenerDatePartitionAccessor<A>,
    media_root: Url,
}
impl<A> Clone for CourtListenerDocumentAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            media_root: self.media_root.clone(),
        }
    }
}
impl<A> CourtListenerDatePartitionAccessor<A> {
    /// Resolve exported media-relative paths beneath an explicit HTTP(S) root.
    /// Existing absolute HTTP(S) URLs are preserved verbatim. No document I/O.
    pub fn with_document_urls(
        self,
        media_root: impl AsRef<str>,
    ) -> Result<CourtListenerDocumentAccessor<A>> {
        let mut media_root = Url::parse(media_root.as_ref())
            .map_err(|_| schema_error("media root must be an absolute HTTP(S) URL"))?;
        validate_media_root(&media_root)?;
        if !media_root.path().ends_with('/') {
            media_root.set_path(&format!("{}/", media_root.path()));
        }
        Ok(CourtListenerDocumentAccessor {
            source: self,
            media_root,
        })
    }
}
impl<A> CourtListenerDocumentAccessor<A> {
    pub fn schema(&self) -> SchemaRef {
        self.source.schemas().schema_with_document_urls()
    }
}
fn resolve(root: &Url, value: Option<&str>) -> Result<Option<String>> {
    let Some(value) = value.filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    if value.trim() != value || value.chars().any(char::is_control) {
        return Err(records_error("invalid document reference whitespace"));
    }
    if let Ok(url) = Url::parse(value) {
        if http_url(&url) && value.contains("://") {
            return Ok(Some(value.to_owned()));
        }
        return Err(records_error("unsupported absolute document URL"));
    }
    // FileField values are raw keys, not URL-encoded paths.
    validate_relative_document_path(value)?;
    let mut url = root.clone();
    let mut segments = url
        .path_segments_mut()
        .map_err(|_| schema_error("media root cannot contain path segments"))?;
    segments.pop_if_empty();
    for part in value.split('/') {
        segments.push(part);
    }
    drop(segments);
    Ok(Some(url.into()))
}
fn attach(batch: RecordBatch, schema: SchemaRef, root: &Url) -> Result<RecordBatch> {
    let entity = |name| {
        batch
            .column_by_name(name)
            .and_then(|a| a.as_any().downcast_ref::<StructArray>())
            .ok_or_else(|| schema_error("missing document entity struct"))
    };
    let opinion = entity("opinion")?;
    let cluster = entity("cluster")?;
    let local = struct_field::<StringArray>(opinion, "local_path")
        .ok_or_else(|| schema_error("missing document path field"))?;
    let download = struct_field::<StringArray>(opinion, "download_url")
        .ok_or_else(|| schema_error("missing document path field"))?;
    let harvard = struct_field::<StringArray>(cluster, "filepath_pdf_harvard")
        .ok_or_else(|| schema_error("missing document path field"))?;
    let mut opinion_urls = Vec::with_capacity(batch.num_rows());
    let mut cluster_urls = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        opinion_urls.push(resolve(
            root,
            struct_string_value(opinion, local, row)
                .filter(|v| !v.is_empty())
                .or_else(|| struct_string_value(opinion, download, row).filter(|v| !v.is_empty())),
        )?);
        cluster_urls.push(resolve(
            root,
            struct_string_value(cluster, harvard, row).filter(|v| !v.is_empty()),
        )?);
    }
    let documents: ArrayRef = Arc::new(StructArray::new(
        document_fields(),
        vec![
            Arc::new(StringArray::from(opinion_urls)),
            Arc::new(StringArray::from(cluster_urls)),
        ],
        None,
    ));
    let mut columns = batch.columns().to_vec();
    columns.push(documents);
    RecordBatch::try_new(schema, columns).map_err(arrow_error)
}
#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for CourtListenerDocumentAccessor<A> {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }
    async fn metadata(&self, mut request: ReadRequest) -> Result<RecordMetadata> {
        let schema = request.options.resolve_projection(&self.schema())?.schema;
        request.options.projection = None;
        let mut metadata = self.source.metadata(request).await?;
        metadata.schema = schema;
        Ok(metadata)
    }
    async fn open(&self, mut request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let metadata = self.metadata(request.clone()).await?;
        let schema = self.schema();
        let projection = request.options.resolve_projection(&schema)?;
        request.options.projection = None;
        let reader = self.source.open(request).await?;
        let root = self.media_root.clone();
        let batches = stream::try_unfold(reader, move |mut reader| {
            let schema = schema.clone();
            let root = root.clone();
            async move {
                let Some(batch) = reader.next().await? else {
                    return Ok(None);
                };
                Ok(Some((attach(batch, schema, &root)?, reader)))
            }
        });
        Ok(Box::new(record_stream_reader(
            batches,
            metadata,
            projection.indices,
            OrderRequirement::PreserveGlobal,
        )))
    }
}
#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for CourtListenerDocumentAccessor<A> {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let metadata = self.metadata(request.clone()).await?;
        let source = self.clone();
        Ok(ScanPlan::new(
            metadata,
            vec![Box::pin(async move { source.open(request).await })],
        )
        .with_ordering(OrderRequirement::PreserveGlobal)
        .with_file_streams())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_urls_preserve_absolute_urls_and_encode_raw_storage_keys() {
        let root = Url::parse("https://media.example/base/").unwrap();
        assert_eq!(
            resolve(&root, Some("harvard_pdf/a b#%?.pdf"))
                .unwrap()
                .unwrap(),
            "https://media.example/base/harvard_pdf/a%20b%23%25%3F.pdf"
        );
        let signed = "https://files.example/a.pdf?token=x%2Fy&v=1";
        assert_eq!(
            resolve(&root, Some(signed)).unwrap().as_deref(),
            Some(signed)
        );
        assert_eq!(resolve(&root, Some("")).unwrap(), None);
        assert_eq!(resolve(&root, None).unwrap(), None);
        for path in [
            "../a.pdf",
            "/a.pdf",
            "//evil.example/a.pdf",
            "a/../b.pdf",
            "a/./b.pdf",
            "a//b.pdf",
            "file:///a.pdf",
            "https://u:p@example/a.pdf",
            "https:example.com/a.pdf",
            " https://example.com/a.pdf",
            "https://example.com/a\npdf",
        ] {
            assert!(resolve(&root, Some(path)).is_err(), "{path}");
        }
    }
}
