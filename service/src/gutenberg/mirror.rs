//! Explicit routing from advertised Gutenberg URLs to a caller-owned mirror.

use std::ops::Range;

use super::{error, validation};

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteMetadata, BytePosition, ByteReadRequest, ByteReader, Bytes,
    Capability, ErrorKind, Reader, Result, async_trait,
};

/// Read advertised Gutenberg file URLs from a caller-configured mirror.
///
/// `prefix` is relative to the wrapped accessor's root. The mirror must expose
/// the main digit tree and generated files beneath `cache/epub/`. URI aliases
/// are resolved without changing the original RDF or Arrow metadata fields.
/// UTF-8 text aliases try `pg<ID>.txt`, then `pg<ID>.txt.utf8` only on
/// NotFound before any bytes are delivered. Other missing files and unsupported
/// URLs remain errors; advertised formats are never substituted.
/// Discovery is not supported. Read options and the backend's sequential-open
/// and metadata paths are preserved.
pub struct GutenbergMirror<A> {
    inner: A,
    prefix: String,
}

impl<A> GutenbergMirror<A> {
    pub fn new(inner: A, prefix: impl Into<String>) -> Self {
        Self {
            inner,
            prefix: prefix.into().trim_end_matches('/').to_owned(),
        }
    }

    fn map_request(
        &self,
        mut request: ByteReadRequest,
    ) -> Result<(ByteReadRequest, Option<ByteReadRequest>)> {
        let path = mirror_path(request.object.locator())?;
        // Only this advertised alias has two known generated mirror filenames.
        // Exact file URLs must never fall back to another representation.
        let url = url::Url::parse(request.object.locator()).expect("validated mirror URL");
        let decoded = percent_encoding::percent_decode_str(url.path())
            .decode_utf8()
            .expect("validated mirror path");
        let utf8_alias = decoded.starts_with("/ebooks/") && decoded.ends_with(".txt.utf-8");
        request.object = if self.prefix.is_empty() {
            path
        } else {
            format!("{}/{path}", self.prefix)
        }
        .into();
        let alternate = utf8_alias.then(|| {
            let mut alternate = request.clone();
            alternate.object = format!("{}.utf8", request.object.locator()).into();
            alternate
        });
        Ok((request, alternate))
    }
}

#[async_trait]
impl<A> Accessor for GutenbergMirror<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability();
        capability.list = false;
        capability
    }

    async fn metadata(&self, request: ByteReadRequest) -> Result<ByteMetadata> {
        let (request, alternate) = self.map_request(request)?;
        match self.inner.metadata(request).await {
            Err(error) if error.kind() == ErrorKind::NotFound && alternate.is_some() => {
                self.inner.metadata(alternate.unwrap()).await
            }
            result => result,
        }
    }

    async fn inspect_metadata(&self, request: ByteReadRequest) -> Result<Option<ByteMetadata>> {
        let (request, alternate) = self.map_request(request)?;
        match self.inner.inspect_metadata(request).await {
            Err(error) if error.kind() == ErrorKind::NotFound && alternate.is_some() => {
                self.inner.inspect_metadata(alternate.unwrap()).await
            }
            result => result,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.open_mirror(request, false).await
    }

    async fn open_sequential(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.open_mirror(request, true).await
    }
}

#[async_trait]
impl<A> ByteAccessor for GutenbergMirror<A> where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>
{
}

impl<A> GutenbergMirror<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn open_inner(
        &self,
        request: ByteReadRequest,
        sequential: bool,
    ) -> Result<Box<dyn ByteReader>> {
        if sequential {
            self.inner.open_sequential(request).await
        } else {
            self.inner.open(request).await
        }
    }

    async fn open_mirror(
        &self,
        request: ByteReadRequest,
        sequential: bool,
    ) -> Result<Box<dyn ByteReader>> {
        let (request, alternate) = self.map_request(request)?;
        let Some(alternate) = alternate else {
            return self.open_inner(request, sequential).await;
        };
        let result = match self.open_inner(request, sequential).await {
            Ok(mut reader) => {
                // Some backends defer the GET until next(). Probe once so a
                // missing alias is resolved before publishing reader metadata.
                let position = reader.position();
                match reader.next().await {
                    Ok(first) => {
                        return Ok(Box::new(Prefetched {
                            reader,
                            first: Some(first),
                            position,
                        }));
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        };
        match result {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                self.open_inner(alternate, sequential).await
            }
            result => result,
        }
    }
}

/// Retain the probed first chunk without advancing the caller's cursor.
struct Prefetched {
    reader: Box<dyn ByteReader>,
    first: Option<Option<Bytes>>,
    position: Option<BytePosition>,
}

#[async_trait]
impl Reader for Prefetched {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;

    fn capability(&self) -> Capability {
        self.reader.capability()
    }
    fn metadata(&self) -> &ByteMetadata {
        self.reader.metadata()
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        match self.first.take() {
            Some(first) => Ok(first),
            None => self.reader.next().await,
        }
    }
}

#[async_trait]
impl ByteReader for Prefetched {
    fn position(&self) -> Option<BytePosition> {
        if self.first.is_some() {
            self.position
        } else {
            self.reader.position()
        }
    }
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.reader.read_range(range).await
    }
    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        self.reader.read_ranges(ranges).await
    }
}

fn mirror_path(uri: &str) -> Result<String> {
    let invalid = || error::invalid_mirror_uri(uri);
    let url = url::Url::parse(uri).map_err(|_| invalid())?;
    let decoded = percent_encoding::percent_decode_str(url.path())
        .decode_utf8()
        .map_err(|_| invalid())?;
    let path = decoded.trim_start_matches('/');
    validation::mirror_uri(&url, path, uri)?;
    if let Some(rest) = path.strip_prefix("files/") {
        let (id, member) = rest.split_once('/').ok_or_else(invalid)?;
        validation::mirror_ebook_id(id, uri)?;
        let mut digits = String::new();
        if id.len() == 1 {
            digits.push_str("0/");
        } else {
            for digit in id[..id.len() - 1].chars() {
                digits.push(digit);
                digits.push('/');
            }
        }
        return Ok(format!("{digits}{id}/{member}"));
    }
    if let Some(rest) = path.strip_prefix("ebooks/") {
        let (id, format) = rest.split_once('.').ok_or_else(invalid)?;
        validation::mirror_ebook_id(id, uri)?;
        let suffix = match format {
            "html.images" => "-images.html",
            "html.noimages" => ".html",
            "epub3.images" => "-images-3.epub",
            "epub3.noimages" => "-3.epub",
            "epub.images" => "-images.epub",
            "epub.noimages" => ".epub",
            "kf8.images" => "-images-kf8.mobi",
            "kf8.noimages" => "-kf8.mobi",
            "kindle.images" => "-images.mobi",
            "kindle.noimages" => ".mobi",
            "txt.utf-8" => ".txt",
            "rdf" => ".rdf",
            _ => {
                return Err(invalid());
            }
        };
        return Ok(format!("cache/epub/{id}/pg{id}{suffix}"));
    }
    if let Some(rest) = path.strip_prefix("cache/generated/") {
        return Ok(format!("cache/epub/{rest}"));
    }
    if path.starts_with("cache/epub/") {
        return Ok(path.to_owned());
    }
    Err(invalid())
}

#[cfg(test)]
mod tests {
    #[test]
    fn maps_gutenberg_aliases_to_mirror_paths() {
        for (uri, expected) in [
            ("https://www.gutenberg.org/files/1/1-0.txt", "0/1/1-0.txt"),
            (
                "https://www.gutenberg.org/files/12345/a%20b.txt",
                "1/2/3/4/12345/a b.txt",
            ),
            (
                "https://www.gutenberg.org/ebooks/42.epub.images",
                "cache/epub/42/pg42-images.epub",
            ),
            (
                "https://www.gutenberg.org/ebooks/42.html.noimages",
                "cache/epub/42/pg42.html",
            ),
            (
                "http://www.gutenberg.org/cache/generated/42/pg42.txt",
                "cache/epub/42/pg42.txt",
            ),
        ] {
            assert_eq!(super::mirror_path(uri).unwrap(), expected);
        }
        for uri in [
            "https://example.org/files/1/1.txt",
            "https://www.gutenberg.org/files/1/a%5Cb",
            "https://www.gutenberg.org/files/1/a?token=x",
            "https://u:p@www.gutenberg.org/files/1/a",
            "https://www.gutenberg.org:8443/files/1/a",
            "https://www.gutenberg.org/files/1/../../private",
            "https://www.gutenberg.org/ebooks/42.unknown",
        ] {
            assert!(super::mirror_path(uri).is_err());
        }
    }
}
