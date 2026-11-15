use std::ops::Range;

use futures::{TryStreamExt, stream};
use http::{HeaderValue, Request, StatusCode, header};
use open_arrow_access_core::{
    ByteMetadata, ByteReadOptions, ByteReader, Bytes, Capability, Error, ErrorKind, Reader, Result,
    StreamReader, async_trait, validate_byte_range,
};
use opendal::{Buffer, BytesRange};
use url::Url;

use super::{Connection, error};
use crate::{http_body::BodyReader, http_client::HttpClient};

mod validation;

const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;

pub(super) async fn open(
    mut connection: Connection,
    url: Url,
    options: ByteReadOptions,
) -> Result<Box<dyn ByteReader>> {
    // Prepare once; cloned range requests retain redirect-safe authentication.
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
        .client
        .response(request.clone(), error::OP)
        .await?;
    let headers = response.headers();
    let version = headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let ranges = validation::supports_ranges(headers);
    if let Some(etag) = validation::strong_etag(headers) {
        request.headers_mut().insert(header::IF_MATCH, etag);
    }
    let chunk_size = options
        .io_chunk_size
        .map_or(DEFAULT_CHUNK_SIZE, |s| s.get());
    let body = BodyReader::new(response, error::OP)?;
    let metadata = ByteMetadata {
        source: None,
        content_length: body.content_length(),
        version,
    };
    let range_length = metadata.content_length.filter(|_| ranges);
    let chunks = stream::try_unfold(body, |mut body| async move {
        Ok::<_, Error>(body.next().await?.map(|buffer| (buffer, body)))
    })
    .map_ok(move |buffer| {
        stream::iter(buffer.flat_map(move |part| {
            Buffer::from(part)
                .chunks(chunk_size)
                .map(|chunk| Ok(chunk.to_bytes()))
        }))
    })
    .try_flatten();
    Ok(Box::new(HttpReader {
        sequential: Box::new(StreamReader::new(chunks, metadata)),
        client: connection.client,
        request,
        range_length,
    }))
}

struct HttpReader {
    sequential: Box<dyn ByteReader>,
    client: HttpClient,
    request: Request<Buffer>,
    range_length: Option<u64>,
}

#[async_trait]
impl Reader for HttpReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;

    fn capability(&self) -> Capability {
        Capability {
            read_with_range: self.range_length.is_some(),
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
        let Some(length) = self.range_length else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                error::OP,
                "HTTP input does not advertise byte ranges with a known identity length",
            ));
        };
        validate_byte_range(&range, length, error::OP)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let mut request = self.request.clone();
        request.headers_mut().insert(
            header::RANGE,
            HeaderValue::from_str(&BytesRange::from(range.clone()).to_header())
                .expect("valid range header"),
        );
        let response = self.client.fetch(request, error::OP).await?;
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
        let etag = self.request.headers().get(header::IF_MATCH);
        validation::validate_response(response.headers(), &range, length, etag)?;
        BodyReader::new(response, error::OP)?
            .with_expected_length(range.end - range.start)?
            .collect()
            .await
    }
}
