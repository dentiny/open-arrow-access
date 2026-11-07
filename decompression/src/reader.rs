use std::num::NonZeroUsize;

use futures::TryStreamExt;
use open_arrow_access_core::{
    ByteReader, Error, ErrorKind, StreamReader, into_async_read, read_chunks,
};

use crate::Codec;

/// Wrap decoded chunks with shared metadata and terminal-state handling.
pub(crate) fn create_reader<C: Codec>(
    inner: Box<dyn ByteReader>,
    codec: &C,
    chunk_size: NonZeroUsize,
) -> impl ByteReader + 'static {
    let mut metadata = inner.metadata().clone();
    metadata.content_length = None;
    let input = into_async_read(inner, "decompress_input");
    let decoder = codec.decoder(Box::pin(input));
    let name = codec.name();
    let stream = read_chunks(decoder, chunk_size).map_err(move |error| {
        Error::new(
            ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode),
            "decompress",
            format!("{name} decoding failed"),
        )
        .with_source(error)
    });
    StreamReader::new(stream, metadata)
}
