use futures::TryStreamExt;

use crate::{ByteAccessor, Error, ErrorKind, ListRequest, ObjectRef, Result};

/// Collect sorted, unique objects beneath a directory prefix.
///
/// `select` validates and filters each relative locator. Path rules and empty
/// results belong to the caller. The request is forwarded unchanged, including
/// recursion; a trailing prefix slash is optional and an empty prefix selects root.
/// Listing and callback errors pass through unchanged. Objects outside the prefix
/// return [`ErrorKind::InvalidInput`]. Memory grows with the selected file count.
pub async fn list_files<A: ByteAccessor + ?Sized>(
    accessor: &A,
    request: ListRequest,
    mut select: impl FnMut(&str) -> Result<bool> + Send,
) -> Result<Vec<ObjectRef>> {
    let mut prefix = request.prefix.locator().to_owned();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    let mut listing = accessor.list(request).await?;
    let mut objects = Vec::new();
    while let Some(object) = listing.try_next().await? {
        let relative = object.locator().strip_prefix(&prefix).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "list_files",
                "listing returned an object outside its directory prefix",
            )
        })?;
        if select(relative)? {
            objects.push(object);
        }
    }
    objects.sort_unstable_by(|a, b| a.locator().cmp(b.locator()));
    objects.dedup();
    Ok(objects)
}
