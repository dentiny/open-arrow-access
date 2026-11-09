use super::error::{decode, io};
use super::{Connection, download_url};
use futures::stream;
use open_arrow_access_core::{ByteMetadata, ByteReader, Bytes, Result, StreamReader};
use opendal::raw::oio::ReadStream;

impl Connection {
    /// Open an absolute URL, preserving encoded paths and signatures byte-for-byte.
    pub async fn open(
        &self,
        url: &str,
        options: open_arrow_access_core::ByteReadOptions,
    ) -> Result<Box<dyn ByteReader>> {
        let response = self.response(download_url(url)?, "*/*").await?;
        let length = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .map(str::parse::<u64>)
            .transpose()
            .map_err(|_| decode("invalid Content-Length"))?;
        let version = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let chunk_size = options.io_chunk_size.map_or(64 * 1024, |s| s.get());
        let state = (response.into_body(), Bytes::new(), 0u64);
        let chunks =
            stream::try_unfold(state, move |(mut body, mut pending, mut read)| async move {
                if pending.is_empty() {
                    let buffer = body.read().await.map_err(io)?;
                    if buffer.is_empty() {
                        if length.is_some_and(|len| read != len) {
                            return Err(decode("truncated HTTP body"));
                        }
                        return Ok(None);
                    }
                    pending = buffer.to_bytes();
                    read = read
                        .checked_add(pending.len() as u64)
                        .ok_or_else(|| decode("body length overflow"))?;
                    if length.is_some_and(|len| read > len) {
                        return Err(decode("HTTP body exceeds Content-Length"));
                    }
                }
                let chunk = pending.split_to(pending.len().min(chunk_size));
                Ok(Some((chunk, (body, pending, read))))
            });
        Ok(Box::new(StreamReader::new(
            chunks,
            ByteMetadata {
                source: None,
                content_length: length,
                version,
            },
        )))
    }
}
