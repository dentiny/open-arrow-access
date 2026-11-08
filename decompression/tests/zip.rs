use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{Arc, Mutex},
};

use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes, Capability, Error,
    ErrorKind, Reader, Result, async_trait,
};
use open_arrow_access_decompression::{ZipAccessor, ZipLimits};
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes, fixture};

#[derive(Clone)]
struct Source {
    inner: MemoryAccessor,
    ranges: Arc<Mutex<Vec<Range<u64>>>>,
    failure: Option<ErrorKind>,
    fail_after: usize,
    unknown_length: bool,
    short_read: bool,
}

impl Source {
    fn new(data: &[u8]) -> Self {
        Self {
            inner: MemoryAccessor::new(data, 1)
                .with_ranges()
                .with_version("snapshot"),
            ranges: Arc::default(),
            failure: None,
            fail_after: 0,
            unknown_length: false,
            short_read: false,
        }
    }
}

#[async_trait]
impl Accessor for Source {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        self.inner.capability()
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let inner = self.inner.open(request).await?;
        let mut metadata = inner.metadata().clone();
        if self.unknown_length {
            metadata.content_length = None;
        }
        Ok(Box::new(Input {
            source: self.clone(),
            inner,
            metadata,
        }))
    }
}

struct Input {
    source: Source,
    inner: Box<dyn ByteReader>,
    metadata: ByteMetadata,
}
#[async_trait]
impl Reader for Input {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    fn capability(&self) -> Capability {
        self.inner.capability()
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.inner.next().await
    }
}
#[async_trait]
impl ByteReader for Input {
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.source.ranges.lock().unwrap().push(range.clone());
        if let Some(kind) = self
            .source
            .failure
            .filter(|_| self.source.ranges.lock().unwrap().len() > self.source.fail_after)
        {
            return Err(Error::new(kind, "zip_fixture", "range failure"));
        }
        let mut bytes = self.inner.read_range(range).await?;
        if self.source.short_read {
            bytes.truncate(bytes.len().saturating_sub(1));
        }
        Ok(bytes)
    }
}

#[tokio::test]
async fn enumerate_then_read_selected_members_with_bounded_io() {
    let data = fixture!("zip", "mixed.zip").await;
    let source = Source::new(&data);
    let zip = ZipAccessor::new(source.clone(), 1).with_chunk_size(NonZeroUsize::new(3).unwrap());
    let entries = zip
        .entries(ByteReadRequest::new("papers.zip"))
        .await
        .unwrap();
    assert_eq!(entries.len(), 8);
    assert_eq!(entries[1].name, b"paper.xml");
    assert_eq!(entries[5].name, entries[1].name);
    assert_eq!(entries[6].name, b"../original.txt");
    assert_eq!(entries[4].name, b"media/");
    assert_eq!(entries[1].uncompressed_size, 24);
    source.ranges.lock().unwrap().clear();

    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        io_chunk_size: NonZeroUsize::new(4096),
    };
    let request = ByteReadRequest::new("papers.zip").with_options(options);
    let mut reader = zip.open(request.clone()).await.unwrap();
    assert_eq!(reader.metadata().source, Some(request.object));
    assert_eq!(reader.metadata().version.as_deref(), Some("snapshot"));
    assert_eq!(reader.metadata().content_length, Some(24));
    assert!(!reader.capability().read_with_range);
    assert!(!reader.capability().list);
    assert!(!reader.capability().read_parallel);
    assert_eq!(
        reader.read_range(0..1).await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        assert!(!chunk.is_empty() && chunk.len() <= 3);
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes, b"<article>hello</article>");
    let reads = source.ranges.lock().unwrap().len();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(reads, source.ranges.lock().unwrap().len());
    let ranges = source.ranges.lock().unwrap();
    assert!(ranges.iter().all(|range| range.end - range.start <= 65_536));
    assert!(
        ranges
            .iter()
            .map(|range| range.end - range.start)
            .sum::<u64>()
            < data.len() as u64
    );
    assert_eq!(
        source
            .inner
            .observations
            .requests
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .options,
        options
    );
}

#[tokio::test]
async fn stored_empty_duplicate_and_directory_entries_are_independent() {
    let data = fixture!("zip", "mixed.zip").await;
    for (index, expected) in [
        (2, b"%PDF-\x00\xff".as_slice()),
        (3, b""),
        (4, b""),
        (5, b"second version"),
    ] {
        let zip = ZipAccessor::new(Source::new(&data), index);
        let mut reader = zip.open(ByteReadRequest::new("archive")).await.unwrap();
        assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn stored_and_deflated_data_descriptors_and_zip64() {
    for (data, index, expected) in [
        (
            fixture!("zip", "descriptors.zip").await,
            0,
            b"stored descriptor".as_slice(),
        ),
        (
            fixture!("zip", "descriptors.zip").await,
            1,
            b"deflated descriptor".as_slice(),
        ),
        (fixture!("zip", "zip64.zip").await, 0, b"ZIP64".as_slice()),
    ] {
        let zip = ZipAccessor::new(Source::new(&data), index);
        assert_eq!(
            zip.entries(ByteReadRequest::new("archive")).await.unwrap()[index].index,
            index
        );
        assert_eq!(
            collect_bytes(
                zip.open(ByteReadRequest::new("archive"))
                    .await
                    .unwrap()
                    .as_mut()
            )
            .await
            .unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn malformed_missing_unsupported_and_sequential_inputs() {
    let empty = fixture!("zip", "empty.zip").await;
    let zip = ZipAccessor::new(Source::new(&empty), 0);
    assert!(
        zip.entries(ByteReadRequest::new("empty"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        zip.open(ByteReadRequest::new("empty"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::NotFound
    );
    let data = fixture!("zip", "mixed.zip").await;
    let bad_offset = fixture!("zip", "bad-offset.zip").await;
    for bytes in [
        b"not a ZIP".as_slice(),
        &data[..data.len() - 10],
        &bad_offset,
    ] {
        assert_eq!(
            ZipAccessor::new(Source::new(bytes), 0)
                .open(ByteReadRequest::new("bad"))
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Decode
        );
    }
    let sequential = ZipAccessor::new(MemoryAccessor::new(&data, 1), 0);
    assert!(!sequential.capability().read);
    assert_eq!(
        sequential
            .open(ByteReadRequest::new("archive"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    let mut source = Source::new(&data);
    source.unknown_length = true;
    assert_eq!(
        ZipAccessor::new(source, 0)
            .open(ByteReadRequest::new("archive"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    let unsupported = fixture!("zip", "unsupported.zip").await;
    assert_eq!(
        ZipAccessor::new(Source::new(&unsupported), 0)
            .open(ByteReadRequest::new("archive"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );

    // Even an empty entry must reject encryption before constructing a decoder.
    let mut encrypted = data.clone();
    let local = encrypted
        .windows(4)
        .enumerate()
        .find_map(|(offset, bytes)| {
            (bytes == b"PK\x03\x04" && encrypted[offset + 30..].starts_with(b"empty.txt"))
                .then_some(offset)
        })
        .unwrap();
    encrypted[local + 6] |= 1;
    assert_eq!(
        ZipAccessor::new(Source::new(&encrypted), 3)
            .open(ByteReadRequest::new("encrypted"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
}

#[tokio::test]
async fn crc_failure_is_terminal() {
    let mut data = fixture!("zip", "mixed.zip").await;
    let offset = data.windows(5).position(|v| v == b"%PDF-").unwrap();
    data[offset] ^= 1;
    let source = Source::new(&data);
    let mut reader = ZipAccessor::new(source.clone(), 2)
        .open(ByteReadRequest::new("corrupt"))
        .await
        .unwrap();
    loop {
        match reader.next().await {
            Ok(Some(_)) => (),
            Err(error) => {
                assert_eq!(error.kind(), ErrorKind::Decode);
                break;
            }
            Ok(None) => panic!("corrupt entry accepted"),
        }
    }
    let reads = source.ranges.lock().unwrap().len();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(reads, source.ranges.lock().unwrap().len());
}

#[tokio::test]
async fn metadata_and_actual_expansion_limits_are_enforced() {
    let data = fixture!("zip", "mixed.zip").await;
    for limits in [
        ZipLimits {
            max_entries: 1,
            ..ZipLimits::default()
        },
        ZipLimits {
            max_directory_bytes: 1,
            ..ZipLimits::default()
        },
        ZipLimits {
            max_entry_bytes: 1,
            ..ZipLimits::default()
        },
    ] {
        assert_eq!(
            ZipAccessor::new(Source::new(&data), 1)
                .with_limits(limits)
                .open(ByteReadRequest::new("archive"))
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Decode
        );
    }
    // Both headers lie about decoded size; bound the actual expansion as well.
    let mut data = data;
    let signatures: Vec<_> = data
        .windows(4)
        .enumerate()
        .filter_map(|(i, bytes)| {
            if bytes == b"PK\x03\x04" {
                Some((i, 22, 30))
            } else if bytes == b"PK\x01\x02" {
                Some((i, 24, 46))
            } else {
                None
            }
        })
        .collect();
    for (offset, size_offset, name_offset) in signatures {
        if data[offset + name_offset..].starts_with(b"expanded.txt") {
            data[offset + size_offset..offset + size_offset + 4]
                .copy_from_slice(&1u32.to_le_bytes());
        }
    }
    let zip = ZipAccessor::new(Source::new(&data), 7).with_limits(ZipLimits {
        max_entry_bytes: 64,
        ..ZipLimits::default()
    });
    let mut reader = zip.open(ByteReadRequest::new("bomb")).await.unwrap();
    assert_eq!(
        collect_bytes(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::Decode
    );
}

#[tokio::test]
async fn range_errors_keep_categories_and_short_reads_fail() {
    let data = fixture!("zip", "mixed.zip").await;
    for kind in [
        ErrorKind::Io,
        ErrorKind::PermissionDenied,
        ErrorKind::NotFound,
    ] {
        let mut source = Source::new(&data);
        source.failure = Some(kind);
        assert_eq!(
            ZipAccessor::new(source, 0)
                .open(ByteReadRequest::new("archive"))
                .await
                .err()
                .unwrap()
                .kind(),
            kind
        );
    }
    let mut source = Source::new(&data);
    source.short_read = true;
    assert_eq!(
        ZipAccessor::new(source, 0)
            .open(ByteReadRequest::new("archive"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Io
    );

    let source = Source::new(&data);
    let zip = ZipAccessor::new(source.clone(), 0);
    drop(zip.open(ByteReadRequest::new("archive")).await.unwrap());
    let opens = source.ranges.lock().unwrap().len();
    let mut source = Source::new(&data);
    source.failure = Some(ErrorKind::PermissionDenied);
    source.fail_after = opens;
    let mut reader = ZipAccessor::new(source.clone(), 0)
        .open(ByteReadRequest::new("archive"))
        .await
        .unwrap();
    assert_eq!(
        collect_bytes(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    let reads = source.ranges.lock().unwrap().len();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(reads, source.ranges.lock().unwrap().len());
}
