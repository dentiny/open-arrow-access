use crate::{ByteReader, Bytes, Error, Result};

/// Pull one byte chunk, rejecting an empty chunk with the supplied error.
///
/// This does not skip empty chunks or read ahead. EOF and upstream errors pass
/// through unchanged; `empty` is called only for `Some` empty bytes. As with
/// [`ByteReader`], stop consuming after EOF or an error. This helper does not add
/// terminal state or source context to the reader.
pub async fn next_nonempty<R: ByteReader + ?Sized>(
    reader: &mut R,
    empty: impl FnOnce() -> Error,
) -> Result<Option<Bytes>> {
    match reader.next().await? {
        Some(bytes) if bytes.is_empty() => Err(empty()),
        result => Ok(result),
    }
}
