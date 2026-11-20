use std::{num::NonZeroUsize, sync::atomic::Ordering};

use arrow_array::cast::AsArray;
use open_arrow_access::{
    Accessor, ArchiveAccessor, ArchiveFormat, ArchiveOptions, ByteReadOptions, ErrorKind,
    ReadOptions, ReadRequest, Reader, ScanPlanner, archive_schema,
};
use open_arrow_access_test_support::{MemoryAccessor, fixture};

#[tokio::test]
async fn filters_before_materialization_and_preserves_projection_and_provenance() {
    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 64);
    let observed = source.observations.clone();
    let mut options = ArchiveOptions::new(ArchiveFormat::Tar)
        .with_entry_filter(|entry| Ok(entry.name.ends_with(b".pdf")));
    options.target_bytes = NonZeroUsize::MIN;
    let accessor = ArchiveAccessor::new(source, options);
    let io = ByteReadOptions {
        io_chunk_size: NonZeroUsize::new(64),
        ..Default::default()
    };
    let request = ReadRequest::new("archive.tar")
        .with_batch_size(NonZeroUsize::new(10).unwrap())
        .with_byte_options(io);
    let metadata = accessor.metadata(request.clone()).await.unwrap();
    let plan = accessor.plan_scan(request.clone()).await.unwrap();
    assert_eq!(metadata.schema, archive_schema());
    assert_eq!(metadata, plan.metadata);
    assert!(observed.requests.lock().unwrap().is_empty());
    let mut reader = plan.into_reader();
    let first = reader.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(
        first["entry_index"]
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0),
        1
    );
    assert_eq!(
        first["entry_name"].as_binary::<i64>().value(0),
        b"papers/paper.pdf"
    );
    assert_eq!(
        first["data"].as_binary::<i64>().value(0),
        b"%PDF-first-paper"
    );
    assert_eq!(
        first["source_uri"].as_string::<i64>().value(0),
        "archive.tar"
    );
    assert_eq!(observed.requests.lock().unwrap()[0].options, io);
    let second = reader.next().await.unwrap().unwrap();
    assert_eq!(
        second["entry_index"]
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0),
        2
    );
    assert!(reader.next().await.unwrap().is_none());

    // Zero-column projection keeps the selected row count and validates EOF.
    let mut projected = accessor
        .open(request.with_projection(Vec::<String>::new()))
        .await
        .unwrap();
    assert_eq!(projected.metadata().schema.fields().len(), 0);
    let mut rows = 0;
    while let Some(batch) = projected.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 0);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
    assert_eq!(observed.range_reads.load(Ordering::SeqCst), 0);

    let crawl = open_arrow_access::crawl::CrawlAccessor::files(
        accessor,
        open_arrow_access::crawl::CrawlOptions::default()
            .with_emission(open_arrow_access::EmissionPolicy::Ordered),
    );
    let mut reader = crawl
        .open(open_arrow_access::dataset::file_set::FileSetRequest::new([
            "archive.tar",
        ]))
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn incremental_output_does_not_hide_late_errors_and_drop_stops_consumption() {
    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 512).with_failure(4096, ErrorKind::PermissionDenied);
    let observed = source.observations.clone();
    let accessor = ArchiveAccessor::new(source, ArchiveOptions::new(ArchiveFormat::Tar));
    let request = ReadRequest::new("archive.tar").with_batch_size(NonZeroUsize::MIN);
    let mut reader = accessor.open(request.clone()).await.unwrap();
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    assert!(reader.next().await.unwrap().is_some());
    let reads = observed.reads.load(Ordering::SeqCst);
    drop(reader);
    assert_eq!(observed.reads.load(Ordering::SeqCst), reads);

    let mut reader = accessor.open(request).await.unwrap();
    let mut rows = 0;
    let error = loop {
        match reader.next().await {
            Ok(Some(batch)) => rows += batch.num_rows(),
            Ok(None) => panic!("late source error was hidden by TAR end markers"),
            Err(error) => break error,
        }
    };
    assert_eq!(rows, 3);
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn decodes_gzip_single_files_and_validates_before_storage_io() {
    let bytes = fixture!("arxiv-articles", "main.tex.gz").await;
    let source = MemoryAccessor::new(&bytes, 7);
    let observed = source.observations.clone();
    let accessor = ArchiveAccessor::new(
        source,
        ArchiveOptions::new(ArchiveFormat::Single {
            name: b"source.tex".to_vec(),
        }),
    );
    assert!(
        accessor
            .open(ReadRequest::new("source.gz").with_projection(["missing"]))
            .await
            .is_err()
    );
    assert!(observed.requests.lock().unwrap().is_empty());
    let mut reader = accessor.open(ReadRequest::new("source.gz")).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(
        batch["data"].as_binary::<i64>().value(0),
        fixture!("arxiv-articles", "main.tex").await
    );
    assert_eq!(
        batch["entry_name"].as_binary::<i64>().value(0),
        b"source.tex"
    );
    assert!(reader.next().await.unwrap().is_none());

    let corrupt = fixture!("arxiv-articles", "bad-crc.tar.gz").await;
    let archive = ArchiveAccessor::new(
        MemoryAccessor::new(&corrupt, 7),
        ArchiveOptions::new(ArchiveFormat::Tar),
    );
    let mut reader = archive
        .open(ReadRequest::new("package.tar.gz"))
        .await
        .unwrap();
    let error = loop {
        match reader.next().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("corrupt gzip checksum was ignored"),
            Err(error) => break error,
        }
    };
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());

    let mut options = ArchiveOptions::new(ArchiveFormat::Single {
        name: b"source.tex".to_vec(),
    });
    options.limits.max_entry_bytes = 1;
    let accessor = ArchiveAccessor::new(MemoryAccessor::new(&bytes, 7), options);
    let mut reader = accessor
        .open(
            ReadRequest::new("source.gz")
                .with_options(ReadOptions::default().with_projection(["entry_name"])),
        )
        .await
        .unwrap();
    assert_eq!(
        reader.next().await.unwrap_err().kind(),
        ErrorKind::ResourceExhausted
    );
    assert!(reader.next().await.unwrap().is_none());
}
