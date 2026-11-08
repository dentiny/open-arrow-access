use std::{num::NonZeroUsize, sync::Mutex};

use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability, Error,
    ErrorKind, ListRequest, ObjectStream, Result, async_trait,
};
use open_arrow_access_service::courtlistener::{CourtListenerBulkAccessor, CourtListenerSnapshot};
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes, fixture};

struct Listing {
    keys: Vec<&'static str>,
    requests: Mutex<Vec<ListRequest>>,
    fail: bool,
}

#[async_trait]
impl Accessor for Listing {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: true,
        }
    }

    async fn open(&self, _: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        panic!("discovery must not open table contents")
    }
}

#[async_trait]
impl ByteAccessor for Listing {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        assert_eq!(request.prefix.locator(), "bulk-data/");
        assert!(!request.recursive);
        self.requests.lock().unwrap().push(request);
        let mut entries: Vec<_> = self.keys.iter().map(|&key| Ok(key.into())).collect();
        if self.fail {
            entries.push(Err(Error::new(
                ErrorKind::PermissionDenied,
                "list_fixture",
                "denied",
            )));
        }
        Ok(Box::pin(stream::iter(entries)))
    }
}

#[tokio::test]
async fn discovers_only_exact_table_snapshots_without_reading_contents() -> Result<()> {
    let accessor = CourtListenerBulkAccessor::new(Listing {
        keys: vec![
            "bulk-data/opinions-2026-09-30.csv.bz2",
            "bulk-data/opinions-2026-03-31.csv.bz2",
            "bulk-data/opinions-2026-09-30.csv.bz2",
            "bulk-data/opinion-clusters-2026-09-30.csv.bz2",
            "bulk-data/opinions-cited-2026-09-30.csv.bz2",
            "bulk-data/nested/opinions-2026-09-30.csv.bz2",
            "bulk-data/opinions-2026-02-30.csv.bz2",
            "bulk-data/opinions-2026-09-30.csv.bz2.tmp",
            "bulk-data/schema-2026-09-30.sql",
        ],
        requests: Mutex::default(),
        fail: false,
    });
    let snapshots = accessor.snapshots("opinions").await?;
    assert_eq!(
        snapshots.iter().map(|s| s.date()).collect::<Vec<_>>(),
        ["2026-03-31", "2026-09-30"]
    );
    assert_eq!(
        snapshots[1].schema_object().locator(),
        "bulk-data/schema-2026-09-30.sql"
    );
    assert!(accessor.snapshots("courts").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn preserves_original_csv_dialect_bytes_metadata_and_io_options() -> Result<()> {
    let input = fixture!("courtlistener", "table.csv.bz2").await;
    let expected = fixture!("courtlistener", "table.csv").await;
    let source = MemoryAccessor::new(&input, 1)
        .with_ranges()
        .with_version("fixture-v1");
    let observations = source.observations.clone();
    let accessor = CourtListenerBulkAccessor::new(source);
    let snapshot = CourtListenerSnapshot::new("opinion-clusters", "2026-09-30")?;
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(7),
    };
    let mut reader = accessor
        .open(ByteReadRequest::new(snapshot.object()).with_options(options))
        .await?;
    assert_eq!(reader.metadata().source, Some(snapshot.object()));
    assert_eq!(reader.metadata().version.as_deref(), Some("fixture-v1"));
    assert_eq!(reader.metadata().content_length, None);
    assert!(!accessor.capability().list);
    assert!(!accessor.capability().read_with_range);
    assert!(!reader.capability().read_with_range);
    assert_eq!(collect_bytes(reader.as_mut()).await?, expected);
    assert!(reader.next().await?.is_none());
    let requests = observations.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].object, snapshot.object());
    assert_eq!(requests[0].options, options);
    Ok(())
}

#[tokio::test]
async fn explicit_reads_need_no_listing_and_discovery_errors_are_not_empty_results() -> Result<()> {
    let accessor = CourtListenerBulkAccessor::new(MemoryAccessor::new(b"bad bzip2", 1));
    assert_eq!(
        accessor.snapshots("opinions").await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    let mut reader = accessor
        .open(
            CourtListenerSnapshot::new("opinions", "2026-09-30")?
                .object()
                .into(),
        )
        .await?;
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await?.is_none());
    let failed = CourtListenerBulkAccessor::new(Listing {
        keys: vec![],
        requests: Mutex::default(),
        fail: true,
    });
    assert_eq!(
        failed.snapshots("courts").await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    let outside = CourtListenerBulkAccessor::new(Listing {
        keys: vec!["other/courts-2026-09-30.csv.bz2"],
        requests: Mutex::default(),
        fail: false,
    });
    assert_eq!(
        outside.snapshots("courts").await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    Ok(())
}

#[tokio::test]
async fn invalid_selections_fail_before_storage_io() {
    let source = MemoryAccessor::new(b"", 1);
    let observations = source.observations.clone();
    let accessor = CourtListenerBulkAccessor::new(source);
    for table in [
        "",
        "../opinions",
        "https://example.com",
        "Opinions",
        "opinions?token=x",
    ] {
        assert_eq!(
            CourtListenerSnapshot::new(table, "2026-09-30")
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            accessor.snapshots(table).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    for date in [
        "0000-01-01",
        "2026-02-29",
        "1900-02-29",
        "2026-04-31",
        "2026-13-01",
        "2026-01-00",
        "2026-1-01",
        "../../bad",
        "2026-01-01/",
    ] {
        assert!(CourtListenerSnapshot::new("courts", date).is_err());
    }
    assert!(CourtListenerSnapshot::new("people_db_race", "2000-02-29").is_ok());
    for object in [
        "bulk-data/../opinions-2026-09-30.csv.bz2",
        "bulk-data/opinions-2026-02-30.csv.bz2",
        "opinions-2026-09-30.csv.bz2",
        "bulk-data/schema-2026-09-30.sql",
        "bulk-data/💥-2026-09-30.csv.bz2",
    ] {
        assert_eq!(
            accessor.open(object.into()).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert!(observations.requests.lock().unwrap().is_empty());
}
