use super::error::io;
use super::validation::BodyLength;
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
        let length = BodyLength::new(response.headers())?;
        let content_length = length.expected;
        let version = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let chunk_size = options.io_chunk_size.map_or(64 * 1024, |s| s.get());
        let state = (response.into_body(), Bytes::new(), length);
        let chunks = stream::try_unfold(
            state,
            move |(mut body, mut pending, mut length)| async move {
                if pending.is_empty() {
                    let buffer = body.read().await.map_err(io)?;
                    if buffer.is_empty() {
                        length.finish()?;
                        return Ok(None);
                    }
                    pending = buffer.to_bytes();
                    length.push(pending.len())?;
                }
                let chunk = pending.split_to(pending.len().min(chunk_size));
                Ok(Some((chunk, (body, pending, length))))
            },
        );
        Ok(Box::new(StreamReader::new(
            chunks,
            ByteMetadata {
                source: None,
                content_length,
                version,
            },
        )))
    }
}
