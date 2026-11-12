use futures::TryStreamExt;

use crate::{ByteAccessor, ListRequest, ObjectRef, Result};

/// Collect sorted, unique objects beneath a directory prefix.
/// Return [`crate::ErrorKind::Unsupported`] before I/O if listing is unavailable.
///
/// `select` filters each locator beneath the prefix. Empty results belong to
/// the caller. The request is forwarded unchanged, including
/// recursion; a trailing prefix slash is optional and an empty prefix selects root.
/// Listing and callback errors pass through unchanged. Objects outside the prefix
/// return [`crate::ErrorKind::InvalidInput`]. Memory grows with the selected file count.
pub async fn list_files<A: ByteAccessor + ?Sized>(
    accessor: &A,
    request: ListRequest,
    mut select: impl FnMut(&str) -> Result<bool> + Send,
) -> Result<Vec<ObjectRef>> {
    crate::validation::validate_listing_capability(accessor.capability().list)?;
    let mut prefix = request.prefix.locator().to_owned();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    let mut listing = accessor.list(request).await?;
    let mut objects = Vec::new();
    while let Some(object) = listing.try_next().await? {
        let relative = relative_listing_path(object.locator(), &prefix)?;
        if select(relative)? {
            objects.push(object);
        }
    }
    objects.sort_unstable_by(|a, b| a.locator().cmp(b.locator()));
    objects.dedup();
    Ok(objects)
}

pub(crate) fn relative_listing_path<'a>(path: &'a str, prefix: &str) -> Result<&'a str> {
    path.strip_prefix(prefix)
        .ok_or_else(crate::error::listing_outside_prefix)
}
