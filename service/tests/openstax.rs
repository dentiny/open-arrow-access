mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::openstax::OpenStaxService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn book_snapshot_pins_commit_and_defers_archive_download() {
    let api = HttpFixture::new();
    let archives = HttpFixture::new();
    api.respond(
        "/repos/openstax/osbooks-physics/commits/main",
        StatusCode::OK,
        fixture!("openstax", "commit.json").await,
    );
    archives.file(
        "/openstax/osbooks-physics/zip/0123456789012345678901234567890123456789",
        fixture!("openstax", "book.zip").await,
    );
    let service = OpenStaxService::with_connections(
        providers::connection(&api),
        providers::connection(&archives),
    );
    let files = service.book("osbooks-physics", "main").await.unwrap();
    assert_eq!(
        files.metadata().commit_sha,
        "0123456789012345678901234567890123456789"
    );
    assert!(archives.requests().is_empty());
    let expected = fixture!("openstax", "book.zip").await;
    byte_accessor::assert_read_contract(files.clone(), "osbooks-physics.zip".into(), &expected)
        .await
        .unwrap();
    use open_arrow_access_core::{Accessor, read_all};
    use open_arrow_access_decompression::zip::{ZipArchive, ZipLimits};
    use open_arrow_access_test_support::MemoryAccessor;
    // Provider downloads are sequential; materialize once before ZIP range access.
    let input = files.open("osbooks-physics.zip".into()).await.unwrap();
    let bytes = read_all(input, "read_openstax").await.unwrap();
    let input = MemoryAccessor::new(&bytes, 7)
        .with_ranges()
        .open("book.zip".into())
        .await
        .unwrap();
    let archive = ZipArchive::open(input, ZipLimits::default()).await.unwrap();
    assert!(
        archive.entries()[0]
            .name
            .ends_with(b"/modules/m00001/index.cnxml")
    );
    let reader = archive
        .open_entry(0, std::num::NonZeroUsize::new(7).unwrap())
        .await
        .unwrap();
    assert_eq!(
        read_all(reader, "read_cnxml").await.unwrap(),
        b"<document xmlns=\"http://cnx.rice.edu/cnxml\"><title>Physics</title></document>\n"
    );
}

#[tokio::test]
async fn rejects_unsupported_repositories_and_unpinned_responses() {
    let f = HttpFixture::new();
    let s = OpenStaxService::with_connections(providers::connection(&f), providers::connection(&f));
    assert!(s.book("../repo", "main").await.is_err());
    assert!(f.requests().is_empty());
    f.respond(
        "/repos/openstax/osbooks-physics/commits/main",
        StatusCode::OK,
        Bytes::from_static(b"{\"sha\":\"main\"}"),
    );
    assert_eq!(
        s.book("osbooks-physics", "main").await.unwrap_err().kind(),
        ErrorKind::Decode
    );
    assert!(s.repositories(0, 10).await.is_err());
}
