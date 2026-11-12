use std::num::NonZeroUsize;

use futures::{TryStreamExt, io::AsyncRead};
use open_arrow_access_core::{ByteMetadata, ByteReader, StreamReader, read_chunks};

/// Wrap an owned decoder as a chunked, sequential byte reader.
///
/// Construction performs no I/O. Retains supplied metadata, preserves upstream
/// error categories, and classifies native decoder errors as Decode. EOF and
/// errors terminate the reader through the shared StreamReader contract.
pub fn decoded_reader<D>(
    decoder: D,
    metadata: ByteMetadata,
    chunk_size: NonZeroUsize,
    operation: &'static str,
    codec_name: &'static str,
) -> Box<dyn ByteReader>
where
    D: AsyncRead + Unpin + Send + 'static,
{
    let stream = read_chunks(decoder, chunk_size)
        .map_err(move |error| crate::error::decoder_error(error, operation, codec_name));
    Box::new(StreamReader::new(stream, metadata))
}
