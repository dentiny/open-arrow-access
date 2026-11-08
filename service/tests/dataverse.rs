use std::num::NonZeroUsize;

use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, ErrorKind, ListRequest,
};
use open_arrow_access_service::dataverse::DataverseService;
use open_arrow_access_test_support::{collect_bytes, fixture, http::HttpFixture};

const PID: &str = "doi:10.7910/DVN/A&B+";
const METADATA: &str = "/api/v1/datasets/:persistentId/versions/1.0?persistentId=doi%3A10.7910%2FDVN%2FA%26B%2B&excludeFiles=false&excludeMetadataBlocks=true";

#[tokio::test]
async fn pins_membership_preserves_logical_names_and_reads_original_bytes() {
    let http = HttpFixture::new();
    http.respond(
        METADATA,
        StatusCode::OK,
        fixture!("dataverse", "dataset.json").await,
    );
    http.file(
        "/api/access/datafile/101?format=original",
        fixture!("dataverse", "paper.bin").await,
    );
    http.file(
        "/api/access/datafile/102?format=original",
        fixture!("dataverse", "original.bin").await,
    );
    // Dataverse can redirect HEAD to a URL signed for GET, which rejects HEAD.
    http.respond_for(
        http::Method::HEAD,
        "/api/access/datafile/101?format=original",
        StatusCode::FORBIDDEN,
        Vec::new(),
    );
    http.respond_for(
        http::Method::HEAD,
        "/api/access/datafile/102?format=original",
        StatusCode::FORBIDDEN,
        Vec::new(),
    );
    let dataset = DataverseService::new(http.endpoint())
        .unwrap()
        .dataset(PID, "1.0")
        .await
        .unwrap();
    assert_eq!(http.requests().len(), 1);
    assert_eq!(dataset.persistent_id(), PID);
    assert_eq!(dataset.version(), "1.0");
    assert_eq!(dataset.files()[0].id, 101);
    assert_eq!(dataset.files()[0].size, 8);
    assert!(!dataset.files()[0].restricted);
    let listed: Vec<_> = dataset
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        listed.iter().map(|f| f.locator()).collect::<Vec<_>>(),
        vec!["papers/2026/paper #1%.pdf", "table.tab"]
    );
    assert_eq!(
        dataset.open("missing".into()).await.err().unwrap().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(http.requests().len(), 1);
    let options = ByteReadOptions {
        io_chunk_size: Some(NonZeroUsize::new(2).unwrap()),
        ..Default::default()
    };
    let mut reader = dataset
        .open(ByteReadRequest::new("papers/2026/paper #1%.pdf").with_options(options))
        .await
        .unwrap();
    assert_eq!(reader.read_range(1..4).await.unwrap().as_ref(), b"PDF");
    let mut tabular = dataset.open("table.tab".into()).await.unwrap();
    drop(dataset);
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"%PDFdata");
    assert_eq!(collect_bytes(tabular.as_mut()).await.unwrap(), b"ORIGINAL");
    assert!(
        http.requests()
            .iter()
            .all(|request| request.method != http::Method::HEAD)
    );
    assert_eq!(
        http.requests()
            .iter()
            .filter(|r| r.path == METADATA)
            .count(),
        1
    );
}

#[tokio::test]
async fn rejects_invalid_responses_and_reads_chunked_metadata() {
    for file in [
        "wrong-pid",
        "wrong-version",
        "draft",
        "missing-files",
        "bad-label",
        "bad-directory",
        "zero-id",
        "duplicate-name",
        "malformed",
    ] {
        let http = HttpFixture::new();
        http.respond(
            METADATA,
            StatusCode::OK,
            fixture!("dataverse", &format!("{file}.json")).await,
        );
        assert_eq!(
            DataverseService::new(http.endpoint())
                .unwrap()
                .dataset(PID, "1.0")
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Decode,
            "{file}"
        );
        assert_eq!(http.requests().len(), 1);
    }
    let http = HttpFixture::new();
    http.respond_chunked(
        METADATA,
        StatusCode::OK,
        fixture!("dataverse", "dataset.json").await,
    );
    assert!(
        DataverseService::new(http.endpoint())
            .unwrap()
            .dataset(PID, "1.0")
            .await
            .is_ok()
    );
    http.respond(
        METADATA,
        StatusCode::OK,
        fixture!("dataverse", "missing-original-size.json").await,
    );
    assert_eq!(
        DataverseService::new(http.endpoint())
            .unwrap()
            .dataset(PID, "1.0")
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    http.respond(
        METADATA,
        StatusCode::OK,
        fixture!("dataverse", "empty.json").await,
    );
    assert!(
        DataverseService::new(http.endpoint())
            .unwrap()
            .dataset(PID, "1.0")
            .await
            .unwrap()
            .files()
            .is_empty()
    );
}

#[tokio::test]
async fn validates_selection_before_network() {
    let http = HttpFixture::new();
    let service = DataverseService::new(http.endpoint()).unwrap();
    for (pid, version) in [
        ("", "1.0"),
        ("doi:", "1.0"),
        ("doi:abc", "latest"),
        (PID, ":draft"),
        (PID, "0.1"),
        (PID, "1/2.0"),
        (PID, "1.-1"),
    ] {
        assert_eq!(
            service.dataset(pid, version).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert!(http.requests().is_empty());
}

#[tokio::test]
async fn accepts_equivalent_doi_casing() {
    let http = HttpFixture::new();
    let path = METADATA.replace("DVN%2FA%26B", "dvn%2Fa%26b");
    http.respond(
        &path,
        StatusCode::OK,
        fixture!("dataverse", "dataset.json").await,
    );
    let dataset = DataverseService::new(http.endpoint())
        .unwrap()
        .dataset(&PID.to_ascii_lowercase(), "1.0")
        .await
        .unwrap();
    assert_eq!(dataset.files().len(), 2);
    assert_eq!(http.requests().len(), 1);
}
