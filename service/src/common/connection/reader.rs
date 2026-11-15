use std::ops::Range;

use futures::stream;
use http::{HeaderValue, Request, StatusCode, header};
use open_arrow_access_core::{
    ByteMetadata, ByteReadOptions, ByteReader, Bytes, Capability, Error, ErrorKind, Reader, Result,
    StreamReader, async_trait, validate_byte_range,
};
use opendal::Buffer;
use url::Url;

use super::{Connection, error, transport::AuthScope};
use crate::http_body::BodyReader;

mod validation;

const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;

// Per-download protocol headers take precedence over scoped connection settings.
// Authentication remains attached by the existing redirect transport on every request.
fn request(connection: &Connection, url: &Url) -> Result<Request<Buffer>> {
    let mut request = connection.request(url, "*/*")?;
    let scope = request
        .extensions_mut()
        .get_mut::<AuthScope>()
        .expect("auth scope");
    for name in [
        header::RANGE,
        header::IF_MATCH,
        header::IF_RANGE,
        header::ACCEPT_ENCODING,
    ] {
        scope.headers.remove(name);
    }
    request.headers_mut().insert(
        header::ACCEPT_ENCODING,
        HeaderValue::from_static("identity"),
    );
    Ok(request)
}

pub(super) async fn open(
    connection: Connection,
    url: Url,
    options: ByteReadOptions,
) -> Result<Box<dyn ByteReader>> {
    let response = connection
        .client
        .response(request(&connection, &url)?, error::OP)
        .await?;
    let headers = response.headers();
    let etag = validation::strong_etag(headers);
    let version = headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let ranges = validation::supports_ranges(headers);
    let chunk_size = options
        .io_chunk_size
        .map_or(DEFAULT_CHUNK_SIZE, |s| s.get());
    let body = BodyReader::new(response, error::OP)?;
    let metadata = ByteMetadata {
        source: None,
        content_length: body.content_length(),
        version,
    };
    let ranges = ranges && metadata.content_length.is_some();
    let state = (body, Buffer::new(), Bytes::new());
    let chunks = stream::try_unfold(
        state,
        move |(mut body, mut parts, mut pending)| async move {
            while pending.is_empty() {
                if let Some(part) = parts.next() {
                    pending = part;
                    continue;
                }
                let Some(buffer) = body.next().await? else {
                    return Ok(None);
                };
                parts = buffer;
            }
            let chunk = pending.split_to(pending.len().min(chunk_size));
            Ok(Some((chunk, (body, parts, pending))))
        },
    );
    Ok(Box::new(HttpReader {
        sequential: Box::new(StreamReader::new(chunks, metadata)),
        connection,
        url,
        etag,
        ranges,
    }))
}

struct HttpReader {
    sequential: Box<dyn ByteReader>,
    connection: Connection,
    url: Url,
    etag: Option<HeaderValue>,
    ranges: bool,
}

#[async_trait]
impl Reader for HttpReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;

    fn capability(&self) -> Capability {
        Capability {
            read_with_range: self.ranges,
            ..self.sequential.capability()
        }
    }
    fn metadata(&self) -> &ByteMetadata {
        self.sequential.metadata()
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.sequential.next().await
    }
}

#[async_trait]
impl ByteReader for HttpReader {
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        if !self.ranges {
            return Err(Error::new(
                ErrorKind::Unsupported,
                error::OP,
                "HTTP input does not advertise byte ranges with a known identity length",
            ));
        }
        let length = self.metadata().content_length.expect("ranged input length");
        validate_byte_range(&range, length, error::OP)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let mut request = request(&self.connection, &self.url)?;
        request.headers_mut().insert(
            header::RANGE,
            HeaderValue::from_str(&format!("bytes={}-{}", range.start, range.end - 1))
                .expect("valid range header"),
        );
        if let Some(etag) = &self.etag {
            request.headers_mut().insert(header::IF_MATCH, etag.clone());
        }
        let response = self.connection.client.fetch(request, error::OP).await?;
        match response.status() {
            StatusCode::PARTIAL_CONTENT => {}
            StatusCode::OK => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    error::OP,
                    "HTTP server ignored the byte range request",
                ));
            }
            status => {
                return Err(crate::http_client::http_status_error(
                    error::OP,
                    status,
                    response.headers(),
                ));
            }
        }
        validation::validate_response(response.headers(), &range, length, self.etag.as_ref())?;
        let expected = range.end - range.start;
        let mut body = BodyReader::new(response, error::OP)?;
        if body
            .content_length()
            .is_some_and(|length| length != expected)
        {
            return Err(error::decode(
                "HTTP range Content-Length does not match the requested range",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(buffer) = body.next().await? {
            if buffer.len() as u64 > expected - bytes.len() as u64 {
                return Err(error::decode("HTTP range body exceeds the requested range"));
            }
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        if bytes.len() as u64 != expected {
            return Err(error::decode(
                "HTTP range body length does not match the requested range",
            ));
        }
        Ok(bytes.into())
    }
}
