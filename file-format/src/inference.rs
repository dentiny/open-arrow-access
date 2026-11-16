use open_arrow_access_core::{ByteReader, Result, async_trait};

pub(crate) struct ResolvedFormat<F> {
    pub format: F,
    pub input: Box<dyn ByteReader>,
}

/// Resolve a format's schema before decoding or partitioning an opened input.
///
/// An explicit schema leaves the input unread. Otherwise, implementations infer a
/// fixed schema from a bounded sample and return a configured format together
/// with input that replays the sample before continuing the same source. Replay
/// may use a canonical representation; the returned format must match those
/// bytes and the reader metadata must describe that representation.
///
/// Returns `None` when inference reaches EOF without records.
/// Format-specific state, such as CSV header handling, belongs in the returned
/// format. Callers validate configuration before opening storage and resolve
/// projection against the resulting schema after this operation.
#[async_trait]
pub(crate) trait SchemaInference: Sized + Send + Sync {
    async fn resolve_schema(
        &self,
        input: Box<dyn ByteReader>,
    ) -> Result<Option<ResolvedFormat<Self>>>;
}

/// Empty schema inference is a normal zero-column, zero-row scan.
pub(crate) fn empty_plan(
    request: open_arrow_access_core::ReadRequest,
) -> open_arrow_access_core::ScanPlan {
    use open_arrow_access_core::{OrderRequirement, RecordMetadata, ScanPlan, Schema};
    ScanPlan::new(
        RecordMetadata {
            schema: std::sync::Arc::new(Schema::empty()),
            source: Some(request.object),
        },
        vec![],
    )
    .with_ordering(OrderRequirement::Reorderable)
}
