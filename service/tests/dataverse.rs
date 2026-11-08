use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{Accessor, ByteAccessor, ByteReadRequest, ErrorKind, ListRequest};
use open_arrow_access_service::dataverse::DataverseService;
use open_arrow_access_test_support::{byte_accessor, collect_bytes, fixture, http::HttpFixture};

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
    for (id, file) in [(101, "paper.bin"), (102, "original.bin")] {
        let path = format!("/api/access/datafile/{id}?format=original");
        http.file(&path, fixture!("dataverse", file).await);
        // Dataverse can redirect HEAD to a URL signed for GET, which rejects HEAD.
        http.respond_for(http::Method::HEAD, &path, StatusCode::FORBIDDEN, Vec::new());
    }
    let dataset = DataverseService::new(http.endpoint())
        .unwrap()
        .dataset(PID, "1.0")
        .await
        .unwrap();
    assert_eq!(http.requests().len(), 1);
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
    let mut reader = dataset
        .open(ByteReadRequest::new("papers/2026/paper #1%.pdf"))
        .await
        .unwrap();
    assert_eq!(reader.read_range(1..4).await.unwrap().as_ref(), b"PDF");
    let mut tabular = dataset.open("table.tab".into()).await.unwrap();
    byte_accessor::assert_read_contract(
        dataset,
        ByteReadRequest::new("papers/2026/paper #1%.pdf"),
        b"%PDFdata",
    )
    .await
    .unwrap();
    assert_eq!(collect_bytes(tabular.as_mut()).await.unwrap(), b"ORIGINAL");
    assert!(
        http.requests()
            .iter()
            .all(|request| request.method != http::Method::HEAD)
    );
}

#[tokio::test]
async fn rejects_mismatched_versions_and_unsupported_files() {
    for (file, kind) in [
        ("wrong-pid.json", ErrorKind::Decode),
        ("wrong-version.json", ErrorKind::Decode),
        ("draft.json", ErrorKind::Decode),
        ("missing-original-size.json", ErrorKind::Unsupported),
        ("restricted.json", ErrorKind::Unsupported),
    ] {
        let http = HttpFixture::new();
        http.respond(METADATA, StatusCode::OK, fixture!("dataverse", file).await);
        assert_eq!(
            DataverseService::new(http.endpoint())
                .unwrap()
                .dataset(PID, "1.0")
                .await
                .err()
                .unwrap()
                .kind(),
            kind,
            "{file}"
        );
        assert_eq!(http.requests().len(), 1);
    }
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
