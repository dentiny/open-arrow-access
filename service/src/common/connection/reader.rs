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
    let client = connection.http_client();
    let response = client.response(request.clone(), error::OP).await?;
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
    let chunks = sequential_chunks(
        body,
        &client,
        &request,
        range_length.filter(|_| connection.http_concurrency_limit.is_some()),
        chunk_size,
    );
    let chunks = chunks
        .map_ok(move |buffer| {
            stream::iter(buffer.flat_map(move |part| {
                Buffer::from(part)
                    .chunks(chunk_size)
                    .map(|chunk| Ok(chunk.to_bytes()))
            }))
        })
        .try_flatten();
    Ok(Box::new(HttpReader {
        sequential: StreamReader::new(chunks, metadata),
        client,
        request,
        range_length,
    }))
}

fn sequential_chunks(
    body: BodyReader,
    client: &HttpClient,
    request: &Request<Buffer>,
    chunked_range_length: Option<u64>,
    chunk_size: usize,
) -> futures::stream::BoxStream<'static, Result<Buffer>> {
    if let Some(length) = chunked_range_length {
        // Do not reserve the shared budget while the reader is idle: each
        // bounded range finishes its response before handing bytes to the caller.
        drop(body);
        let client = client.clone();
        let request = request.clone();
        Box::pin(stream::try_unfold(0, move |offset| {
            let client = client.clone();
            let request = request.clone();
            async move {
                if offset == length {
                    return Ok(None);
                }
                let end = offset + (length - offset).min(chunk_size as u64);
                let bytes = read_http_range(&client, &request, Some(length), offset..end).await?;
                Ok(Some((Buffer::from(bytes), end)))
            }
        }))
    } else {
        Box::pin(stream::try_unfold(body, |mut body| async move {
            Ok::<_, Error>(body.next().await?.map(|buffer| (buffer, body)))
        }))
    }
}

struct HttpReader<R> {
    sequential: R,
    client: HttpClient,
    request: Request<Buffer>,
    range_length: Option<u64>,
}

#[async_trait]
impl<R: ByteReader> Reader for HttpReader<R> {
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
impl<R: ByteReader> ByteReader for HttpReader<R> {
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        read_http_range(&self.client, &self.request, self.range_length, range).await
    }
}

async fn read_http_range(
    client: &HttpClient,
    template: &Request<Buffer>,
    length: Option<u64>,
    range: Range<u64>,
) -> Result<Bytes> {
    let Some(length) = length else {
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
    let mut request = template.clone();
    request.headers_mut().insert(
        header::RANGE,
        HeaderValue::from_str(&BytesRange::from(range.clone()).to_header())
            .expect("valid range header"),
    );
    let response = client.fetch(request, error::OP).await?;
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
    let etag = template.headers().get(header::IF_MATCH);
    validation::validate_response(response.headers(), &range, length, etag)?;
    let body =
        BodyReader::new(response, error::OP)?.with_expected_length(range.end - range.start)?;
    crate::http_body::collect(body).await
}
