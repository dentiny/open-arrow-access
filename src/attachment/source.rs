use super::{AttachmentAccessor, AttachmentOptions};
use crate::{BoundSource, ByteAccessor, ReadRequest, Result};

/// Bind one logical attachment name and complete read request to an Arrow source.
///
/// The supplied byte accessor owns file membership, authentication and storage.
/// No provider URLs are resolved here. Configuration is checked before opening
/// bytes; binding retains the first reader and subsequent readers reopen independently.
/// Binary mode buffers the whole object on first consumption, without decompression.
/// More specialized schemas/dialects can use the existing format APIs directly.
///
/// ```no_run
/// use open_arrow_access::{
///     AttachmentFormat, AttachmentOptions, BoundSource, ByteAccessor, ReadRequest,
///     Result, open_attachment_source,
/// };
///
/// // Enable format-json. `files` can be any provider's resolved byte accessor.
/// async fn selected_json<A: ByteAccessor + 'static>(files: A) -> Result<BoundSource> {
///     open_attachment_source(
///         files,
///         ReadRequest::new("events.jsonl.gz"),
///         AttachmentOptions::new(AttachmentFormat::Jsonl),
///     ).await
/// }
/// ```
pub async fn open_attachment_source<A>(
    accessor: A,
    request: ReadRequest,
    options: AttachmentOptions,
) -> Result<BoundSource>
where
    A: ByteAccessor + 'static,
{
    BoundSource::bind(AttachmentAccessor::new(accessor, options)?, request).await
}
