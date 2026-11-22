use futures::{TryStreamExt, stream};
use http::{HeaderValue, header};
use open_arrow_access_core::{ByteMetadata, ByteReadOptions, ByteReader, Result, StreamReader};
use opendal::Buffer;
use url::Url;

use super::{Connection, error};
use crate::http_body::BodyReader;

const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;

pub(super) async fn open(
    mut connection: Connection,
    url: Url,
    options: ByteReadOptions,
) -> Result<Box<dyn ByteReader>> {
    // Download headers take precedence over connection defaults.
    for name in [
        header::RANGE,
        header::IF_MATCH,
        header::IF_RANGE,
        header::ACCEPT_ENCODING,
    ] {
        connection.headers.remove(name);
    }
    let mut request = connection.request(&url, "*/*")?;
    request.headers_mut().insert(
        header::ACCEPT_ENCODING,
        HeaderValue::from_static("identity"),
    );
    let response = connection
        .http_client()
        .response(request, error::OP)
        .await?;
    let version = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let chunk_size = options
        .io_chunk_size
        .map_or(DEFAULT_CHUNK_SIZE, |s| s.get());
    let body = BodyReader::new(response, error::OP)?;
    let metadata = ByteMetadata {
        source: None,
        content_length: body.content_length(),
        version,
    };
    let chunks = stream::try_unfold(body, |mut body| async move {
        Ok(body.next().await?.map(|buffer| (buffer, body)))
    })
    .map_ok(move |buffer| {
        stream::iter(buffer.flat_map(move |part| {
            Buffer::from(part)
                .chunks(chunk_size)
                .map(|chunk| Ok(chunk.to_bytes()))
        }))
    })
    .try_flatten();
    Ok(Box::new(StreamReader::new(chunks, metadata)))
}
