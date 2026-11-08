use std::{num::NonZeroUsize, sync::atomic::Ordering};

use open_arrow_access_core::{Accessor, ByteReadOptions, ByteReadRequest, ErrorKind};
use open_arrow_access_decompression::zip::{ZipAccessor, ZipLimits};
use open_arrow_access_test_support::{MemoryAccessor, byte_layer, collect_bytes, fixture};

fn memory(data: &[u8]) -> MemoryAccessor {
    MemoryAccessor::new(data, 1)
        .with_ranges()
        .with_version("snapshot")
}

#[tokio::test]
async fn archive_session_reuses_storage_and_interleaves_independent_entries() {
    let data = fixture!("zip", "mixed.zip").await;
    let source = memory(&data);
    let observed = source.observations.clone();
    let archive = ZipAccessor::new(source, 0)
        .archive(ByteReadRequest::new("papers.zip"))
        .await
        .unwrap();
    assert_eq!(archive.entries().len(), 8);
    let chunk = NonZeroUsize::new(3).unwrap();
    fn owned_send<T: Send + 'static>(value: T) -> T {
        value
    }
    let mut xml = owned_send(archive.open_entry(1, chunk)).await.unwrap();
    let mut pdf = owned_send(archive.open_entry(2, chunk)).await.unwrap();
    assert_eq!(xml.next().await.unwrap().unwrap().as_ref(), b"<ar");
    assert_eq!(pdf.next().await.unwrap().unwrap().as_ref(), b"%PD");
    let mut duplicate = archive.clone().open_entry(5, chunk).await.unwrap();
    drop(archive);
    byte_layer::assert_output(xml.as_mut(), b"ticle>hello</article>", 3)
        .await
        .unwrap();
    byte_layer::assert_output(pdf.as_mut(), b"F-\x00\xff", 3)
        .await
        .unwrap();
    byte_layer::assert_output(duplicate.as_mut(), b"second version", 3)
        .await
        .unwrap();
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn enumerate_then_read_selected_members_with_bounded_io() {
    let data = fixture!("zip", "mixed.zip").await;
    let source = memory(&data);
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
    source.observations.ranges.lock().unwrap().clear();

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
    byte_layer::assert_output(reader.as_mut(), b"<article>hello</article>", 3)
        .await
        .unwrap();
    let reads = source.observations.ranges.lock().unwrap().len();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(reads, source.observations.ranges.lock().unwrap().len());
    assert!(source.observations.range_reads.load(Ordering::SeqCst) > 0);
    let ranges = source.observations.ranges.lock().unwrap();
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
        let zip = ZipAccessor::new(memory(&data), index);
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
        let zip = ZipAccessor::new(memory(&data), index);
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
    let zip = ZipAccessor::new(memory(&empty), 0);
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
    for name in ["bad_magic.zip", "bad_truncated.zip"] {
        let bytes = fixture!("zip", name).await;
        assert_eq!(
            ZipAccessor::new(memory(&bytes), 0)
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
    let source = memory(&data);
    let source = source.without_content_length();
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
        ZipAccessor::new(memory(&unsupported), 0)
            .open(ByteReadRequest::new("archive"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );

    // Even an empty entry must reject encryption before constructing a decoder.
    let encrypted = fixture!("zip", "bad_encrypted.zip").await;
    assert_eq!(
        ZipAccessor::new(memory(&encrypted), 3)
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
    let data = fixture!("zip", "bad_crc.zip").await;
    let source = memory(&data);
    let mut reader = ZipAccessor::new(source.clone(), 2)
        .open(ByteReadRequest::new("corrupt"))
        .await
        .unwrap();
    byte_layer::assert_terminal_error(
        reader.as_mut(),
        &source.observations.range_reads,
        ErrorKind::Decode,
        "decompress_zip",
    )
    .await;
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
            ZipAccessor::new(memory(&data), 1)
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
    let data = fixture!("zip", "bad_expansion_size.zip").await;
    let zip = ZipAccessor::new(memory(&data), 7).with_limits(ZipLimits {
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
        let source = memory(&data);
        let source = source.with_range_failure(0, kind);
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
    let source = memory(&data);
    let source = source.with_short_ranges();
    assert_eq!(
        ZipAccessor::new(source, 0)
            .open(ByteReadRequest::new("archive"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Io
    );

    let source = memory(&data);
    let zip = ZipAccessor::new(source.clone(), 0);
    drop(zip.open(ByteReadRequest::new("archive")).await.unwrap());
    let opens = source.observations.ranges.lock().unwrap().len();
    let source = memory(&data);
    let source = source.with_range_failure(opens, ErrorKind::PermissionDenied);
    let mut reader = ZipAccessor::new(source.clone(), 0)
        .open(ByteReadRequest::new("archive"))
        .await
        .unwrap();
    byte_layer::assert_terminal_error(
        reader.as_mut(),
        &source.observations.range_reads,
        ErrorKind::PermissionDenied,
        "decompress_zip",
    )
    .await;
}

#[tokio::test]
#[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
async fn malformed_zip64_offset_uses_upstream_panic() {
    let data = fixture!("zip", "bad-offset.zip").await;
    let _ = ZipAccessor::new(memory(&data), 0)
        .open(ByteReadRequest::new("bad-offset.zip"))
        .await;
}
