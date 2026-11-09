use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::{error::Error as _, num::NonZeroUsize};

use futures::TryStreamExt;
use more_asserts::{assert_gt, assert_le};
use open_arrow_access_core::{Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, ErrorKind};
use opendal::{
    Execute, Executor, OperationContext, Operator,
    raw::{BoxedStaticFuture, Layer, Servicer},
    services::Fs,
};

use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{byte_accessor, fixture, fixture_path};

#[tokio::test]
async fn filesystem_read_contract() {
    let dir = tempfile::tempdir().unwrap();
    for contents in [b"".as_slice(), b"independent readers"] {
        tokio::fs::write(dir.path().join("data.bin"), contents)
            .await
            .unwrap();
        let accessor =
            OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
        let end = contents.len() as u64;
        byte_accessor::assert_ranged_read_contract(
            accessor.clone(),
            "data.bin".into(),
            contents,
            &[0..end, end..end, 0..end],
        )
        .await
        .unwrap();
        let mut reader = accessor.open("data.bin".into()).await.unwrap();
        for range in [end..end + 1, std::ops::Range { start: 2, end: 1 }] {
            assert_eq!(
                reader.read_range(range).await.unwrap_err().kind(),
                ErrorKind::InvalidInput
            );
        }
    }
}

#[tokio::test]
async fn filesystem_rejects_surrounding_whitespace_without_aliasing_files() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(
        dir.path().join("data.bin"),
        &fixture!("bytes", "other.bin").await,
    )
    .await
    .unwrap();
    let locators = [
        " data.bin",
        "data.bin ",
        " data.bin ",
        "\tdata.bin\n",
        "\u{a0}data.bin\u{a0}",
    ];
    #[cfg(unix)]
    for locator in locators {
        tokio::fs::write(
            dir.path().join(locator),
            &fixture!("bytes", "requested.bin").await,
        )
        .await
        .unwrap();
    }
    tokio::fs::write(
        dir.path().join("data file.bin"),
        &fixture!("bytes", "internal_space.bin").await,
    )
    .await
    .unwrap();
    let accessor = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));

    for locator in locators {
        let error = accessor.open(locator.into()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{locator:?}");
        assert_eq!(error.operation(), "open");
    }
    let mut reader = accessor.open("data.bin".into()).await.unwrap();
    assert_eq!(
        reader.next().await.unwrap().unwrap().as_ref(),
        b"other file"
    );
    let mut reader = accessor.open("data file.bin".into()).await.unwrap();
    assert_eq!(
        reader.next().await.unwrap().unwrap().as_ref(),
        b"internal space"
    );
}

#[tokio::test]
async fn sequential_reads_report_short_input_and_terminate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short.bin");
    tokio::fs::copy(fixture_path!("bytes", "short.bin"), &path)
        .await
        .unwrap();
    let accessor = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let mut reader = accessor.open("short.bin".into()).await.unwrap();
    tokio::fs::copy(fixture_path!("bytes", "short_truncated.bin"), &path)
        .await
        .unwrap();
    let error = loop {
        match reader.next().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("short input was accepted as complete"),
            Err(error) => break error,
        }
    };
    assert_eq!(error.kind(), ErrorKind::Io);
    assert!(reader.next().await.unwrap().is_none());
}

#[derive(Debug, Default)]
struct TaskCounts {
    started: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}

#[tokio::test]
async fn sequential_output_chunks_reuse_storage_requests_when_ranges_are_interleaved() {
    let dir = tempfile::tempdir().unwrap();
    let contents = fixture!("bytes", "sequence.bin").await;
    tokio::fs::write(dir.path().join("data.bin"), &contents)
        .await
        .unwrap();
    let counts = Arc::new(TaskCounts::default());
    let operator = Operator::new(Fs::default().root(dir.path().to_str().unwrap()))
        .unwrap()
        .layer(CountTasksLayer(counts.clone()));
    let accessor = OpenDalAccessor::new(operator).with_chunk_size(std::num::NonZeroUsize::MIN);
    let mut reader = accessor
        .open(
            ByteReadRequest::new("data.bin").with_options(ByteReadOptions {
                io_chunk_size: std::num::NonZeroUsize::new(4),
                io_concurrency: std::num::NonZeroUsize::new(4).unwrap(),
            }),
        )
        .await
        .unwrap();
    let mut output = reader.next().await.unwrap().unwrap().to_vec();
    assert_eq!(reader.read_range(12..14).await.unwrap().as_ref(), &[12, 13]);
    while let Some(chunk) = reader.next().await.unwrap() {
        assert_eq!(chunk.len(), 1);
        output.extend_from_slice(&chunk);
    }
    assert_eq!(output, contents);
    assert_eq!(
        counts.started.load(Ordering::SeqCst),
        17,
        "sixteen sequential I/O chunks plus one independent range"
    );
    assert_le!(counts.peak.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn large_single_and_bulk_ranges_preserve_values_and_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let mib = 1024 * 1024;
    let contents = fixture!("bytes", "large.bin").await;
    tokio::fs::write(dir.path().join("large.bin"), &contents)
        .await
        .unwrap();
    let accessor = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    for (concurrency, chunk_size) in [
        (1, None),
        (4, None),
        (1, std::num::NonZeroUsize::new(256 * 1024)),
        (4, std::num::NonZeroUsize::new(256 * 1024)),
        (4, std::num::NonZeroUsize::new(4 * mib)),
    ] {
        let mut reader = accessor
            .open(
                ByteReadRequest::new("large.bin").with_options(ByteReadOptions {
                    io_concurrency: std::num::NonZeroUsize::new(concurrency).unwrap(),
                    io_chunk_size: chunk_size,
                }),
            )
            .await
            .unwrap();
        let large = 17..(4 * mib + 3) as u64;
        let ranges = vec![
            0..mib as u64,
            mib as u64..(2 * mib + 1) as u64,
            large.clone(),
            large,
            contents.len() as u64..contents.len() as u64,
            5..23,
        ];
        byte_accessor::assert_ranges(reader.as_mut(), &contents, &ranges)
            .await
            .unwrap();
        assert_eq!(
            reader.next().await.unwrap().unwrap().as_ref(),
            &contents[..64 * 1024]
        );
    }
}

#[tokio::test]
async fn large_range_reports_short_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.bin");
    tokio::fs::copy(fixture_path!("bytes", "large.bin"), &path)
        .await
        .unwrap();
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .await
        .unwrap();
    let accessor = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let mut reader = accessor.open("large.bin".into()).await.unwrap();
    file.set_len(1024 * 1024).await.unwrap();
    let error = reader.read_range(0..3 * 1024 * 1024).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
}

struct ActiveTask(Arc<TaskCounts>);

impl Drop for ActiveTask {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct CountingExecutor {
    inner: Arc<dyn Execute>,
    counts: Arc<TaskCounts>,
}

impl Execute for CountingExecutor {
    fn execute(&self, future: BoxedStaticFuture<()>) {
        self.counts.started.fetch_add(1, Ordering::SeqCst);
        let counts = self.counts.clone();
        self.inner.execute(Box::pin(async move {
            let active = counts.active.fetch_add(1, Ordering::SeqCst) + 1;
            counts.peak.fetch_max(active, Ordering::SeqCst);
            let _guard = ActiveTask(counts);
            // Force overlap without relying on filesystem speed or a timed sleep.
            tokio::task::yield_now().await;
            future.await;
        }));
    }
}

#[derive(Debug)]
struct CountTasksLayer(Arc<TaskCounts>);

impl Layer for CountTasksLayer {
    fn apply_context(&self, _: Servicer, inner: OperationContext) -> OperationContext {
        inner.with_executor(Executor::with(CountingExecutor {
            inner: inner.executor().clone().into_inner(),
            counts: self.0.clone(),
        }))
    }
}

#[tokio::test]
async fn filesystem_bulk_reads_do_not_merge_across_unrequested_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let middle = 512 * 1024;
    let end = 1024 * 1024;
    let contents = fixture!("bytes", "gaps.bin").await;
    tokio::fs::write(dir.path().join("data.bin"), &contents)
        .await
        .unwrap();
    let counts = Arc::new(TaskCounts::default());
    let operator = Operator::new(Fs::default().root(dir.path().to_str().unwrap()))
        .unwrap()
        .layer(CountTasksLayer(counts.clone()));
    let accessor = OpenDalAccessor::new(operator);
    let mut reader = accessor
        .open(
            ByteReadRequest::new("data.bin").with_options(ByteReadOptions {
                io_concurrency: std::num::NonZeroUsize::new(4).unwrap(),
                ..ByteReadOptions::default()
            }),
        )
        .await
        .unwrap();
    let buffers = reader
        .read_ranges(vec![
            middle as u64..middle as u64 + 2,
            0..1,
            end as u64..end as u64 + 2,
            1..2,
            0..2,
            0..0,
        ])
        .await
        .unwrap();
    assert_eq!(
        buffers
            .iter()
            .map(|buffer| buffer.as_ref())
            .collect::<Vec<_>>(),
        vec![
            &b"cd"[..],
            &b"a"[..],
            &b"ef"[..],
            &b"b"[..],
            &b"ab"[..],
            &b""[..]
        ]
    );
    // With no I/O splitting, each disjoint range uses one backend task. Nearby
    // ranges must stay separate while adjacent/overlapping ranges still merge.
    assert_eq!(counts.started.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn io_options_enable_bounded_backend_concurrency_for_single_and_bulk_reads() {
    let dir = tempfile::tempdir().unwrap();
    let contents = fixture!("bytes", "sequence.bin").await;
    tokio::fs::write(dir.path().join("data.bin"), &contents)
        .await
        .unwrap();
    for concurrency in [1, 4] {
        let counts = Arc::new(TaskCounts::default());
        let operator = Operator::new(Fs::default().root(dir.path().to_str().unwrap()))
            .unwrap()
            .layer(CountTasksLayer(counts.clone()));
        let accessor = OpenDalAccessor::new(operator);
        let mut reader = accessor
            .open(
                ByteReadRequest::new("data.bin").with_options(ByteReadOptions {
                    io_concurrency: std::num::NonZeroUsize::new(concurrency).unwrap(),
                    io_chunk_size: std::num::NonZeroUsize::new(8),
                }),
            )
            .await
            .unwrap();
        assert_eq!(reader.read_range(0..64).await.unwrap().as_ref(), contents);
        let ranges = reader.read_ranges(vec![32..64, 0..16]).await.unwrap();
        assert_eq!(ranges[0].as_ref(), &contents[32..64]);
        assert_eq!(ranges[1].as_ref(), &contents[..16]);
        let peak = counts.peak.load(Ordering::SeqCst);
        assert_le!(peak, concurrency, "peak {peak} exceeds limit {concurrency}");
        if concurrency > 1 {
            assert_gt!(peak, 1, "configured reads did not execute concurrently");
        }
    }
}

#[tokio::test]
async fn filesystem_rejects_missing_files_directories_and_invalid_locators() {
    let dir = tempfile::tempdir().unwrap();
    let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let error = fs.open("missing.bin".into()).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(error.source().is_some());
    for locator in ["", "../outside", "/absolute", "."] {
        let error = fs.open(locator.into()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    tokio::fs::create_dir(dir.path().join("subdir"))
        .await
        .unwrap();
    assert_eq!(
        fs.open("subdir/".into()).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert!(fs.capability().list);
    assert!(
        fs.list("".into())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn byte_io_options_preserve_range_order_empty_ranges_and_sequential_cursor() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::copy(
        open_arrow_access_test_support::fixture_path!("bytes", "alphabet.bin"),
        dir.path().join("data.bin"),
    )
    .await
    .unwrap();
    let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()))
        .with_chunk_size(NonZeroUsize::new(3).unwrap());
    for concurrency in [1, 4] {
        let mut reader = fs
            .open(
                ByteReadRequest::new("data.bin").with_options(ByteReadOptions {
                    io_concurrency: NonZeroUsize::new(concurrency).unwrap(),
                    io_chunk_size: NonZeroUsize::new(2),
                }),
            )
            .await
            .unwrap();
        assert_eq!(reader.metadata().content_length, Some(10));
        assert!(reader.capability().read_with_range);
        assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"abc");
        let buffers = reader
            .read_ranges(vec![7..10, 0..3, 5..5, 2..6, 7..10])
            .await
            .unwrap();
        let actual = buffers
            .iter()
            .map(|buffer| buffer.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                &b"hij"[..],
                &b"abc"[..],
                &b""[..],
                &b"cdef"[..],
                &b"hij"[..]
            ]
        );
        assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"def");
        assert!(reader.read_ranges(vec![]).await.unwrap().is_empty());
        assert_eq!(
            reader
                .read_ranges(vec![0..2, 0..11])
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(reader.read_range(4..8).await.unwrap().as_ref(), b"efgh");
    }
}
