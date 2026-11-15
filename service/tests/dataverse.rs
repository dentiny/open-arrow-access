use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{Accessor, ByteAccessor, ByteReadRequest, ErrorKind, ListRequest};
use open_arrow_access_service::dataverse::DataverseService;
use open_arrow_access_test_support::{
    byte_accessor, collect_bytes, fixture,
    http::{HttpFixture, assert_resolution_cases},
};

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
    assert_eq!(dataset.persistent_id(), PID);
    assert_eq!(dataset.version(), "1.0");
    assert_eq!(
        dataset
            .files()
            .iter()
            .map(|file| (file.id, file.size))
            .collect::<Vec<_>>(),
        vec![(101, 8), (102, 8)]
    );
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
    let mut tabular = dataset.open("table.tab".into()).await.unwrap();
    byte_accessor::assert_ranged_read_contract(
        dataset,
        ByteReadRequest::new("papers/2026/paper #1%.pdf"),
        b"%PDFdata",
        &[1..4, 0..0, 1..4],
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
async fn rejects_invalid_metadata_and_unsupported_files() {
    assert_resolution_cases(
        env!("CARGO_MANIFEST_DIR"),
        "dataverse",
        METADATA,
        &[
            ("wrong-pid.json", StatusCode::OK, ErrorKind::Decode),
            ("wrong-version.json", StatusCode::OK, ErrorKind::Decode),
            ("draft.json", StatusCode::OK, ErrorKind::Decode),
            ("duplicate-name.json", StatusCode::OK, ErrorKind::Decode),
            ("malformed.json", StatusCode::OK, ErrorKind::Decode),
            ("missing-files.json", StatusCode::OK, ErrorKind::Decode),
            ("zero-id.json", StatusCode::OK, ErrorKind::Decode),
            (
                "missing-original-size.json",
                StatusCode::OK,
                ErrorKind::Unsupported,
            ),
            ("restricted.json", StatusCode::OK, ErrorKind::Unsupported),
        ],
        |endpoint| async move { DataverseService::new(&endpoint)?.dataset(PID, "1.0").await },
    )
    .await;
    let http = HttpFixture::new();
    http.respond(
        METADATA,
        StatusCode::OK,
        fixture!("dataverse", "empty.json").await,
    );
    let dataset = DataverseService::new(http.endpoint())
        .unwrap()
        .dataset(PID, "1.0")
        .await
        .unwrap();
    assert!(dataset.files().is_empty());
    assert_eq!(http.requests().len(), 1);
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

#[tokio::test]
async fn preserves_logical_names_without_path_validation() {
    for name in ["bad-directory.json", "bad-label.json"] {
        let http = HttpFixture::new();
        http.respond(METADATA, StatusCode::OK, fixture!("dataverse", name).await);
        let dataset = DataverseService::new(http.endpoint())
            .unwrap()
            .dataset(PID, "1.0")
            .await
            .unwrap();
        assert_eq!(dataset.files().len(), 2);
        assert_eq!(http.requests().len(), 1);
    }
}

fn authenticated(http: &HttpFixture, bearer: bool) -> DataverseService {
    let service = DataverseService::new(http.endpoint())
        .unwrap()
        .with_context(opendal::OperationContext::new());
    if bearer {
        service.with_bearer_token("test-secret").unwrap()
    } else {
        service.with_token("test-secret").unwrap()
    }
}

#[tokio::test]
async fn authenticates_metadata_and_restricted_original_reads() {
    for (bearer, header, value) in [
        (false, "x-dataverse-key", "test-secret"),
        (true, "authorization", "Bearer test-secret"),
    ] {
        let http = HttpFixture::new();
        http.respond(
            METADATA,
            StatusCode::OK,
            fixture!("dataverse", "restricted.json").await,
        );
        http.file(
            "/api/access/datafile/102?format=original",
            b"ORIGINAL".to_vec(),
        );
        let service = authenticated(&http, bearer);
        assert!(!format!("{service:?}").contains("test-secret"));
        let dataset = service.dataset(PID, "1.0").await.unwrap();
        let mut reader = dataset.open("table.tab".into()).await.unwrap();
        assert_eq!(reader.read_range(1..4).await.unwrap().as_ref(), b"RIG");
        assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"ORIGINAL");
        for request in http.requests() {
            assert_eq!(request.headers[header], value);
            assert!(!request.path.contains("test-secret"));
        }
    }
}

#[tokio::test]
async fn credentials_follow_same_origin_redirects_but_never_return_after_leaving() {
    use open_arrow_access_test_support::http::{file_response, response};
    let http = HttpFixture::new();
    let other = HttpFixture::new();
    let destination = other.endpoint().to_owned();
    http.respond(
        METADATA,
        StatusCode::OK,
        fixture!("dataverse", "restricted.json").await,
    );
    http.handle(move |request| {
        if request.uri().path() == "/returned" {
            return Ok(file_response(&request, b"ORIGINAL".to_vec()));
        }
        let location = if request.uri().path() == "/api/access/datafile/102" {
            "/same-origin".to_owned()
        } else {
            format!("{destination}/signed?signature=a%2Bb")
        };
        let mut redirect = response(StatusCode::TEMPORARY_REDIRECT, Vec::new());
        redirect
            .headers_mut()
            .insert("location", location.parse().unwrap());
        Ok(redirect)
    });
    let origin = http.endpoint().to_owned();
    other.handle(move |_| {
        let mut redirect = response(StatusCode::FOUND, Vec::new());
        redirect
            .headers_mut()
            .insert("location", format!("{origin}/returned").parse().unwrap());
        Ok(redirect)
    });
    let dataset = authenticated(&http, false)
        .dataset(PID, "1.0")
        .await
        .unwrap();
    let mut reader = dataset.open("table.tab".into()).await.unwrap();
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"ORIGINAL");
    let requests = http.requests();
    let same_origin = requests.iter().find(|r| r.path == "/same-origin").unwrap();
    assert_eq!(same_origin.headers["x-dataverse-key"], "test-secret");
    let returned = requests.iter().find(|r| r.path == "/returned").unwrap();
    assert!(!returned.headers.contains_key("x-dataverse-key"));
    let external = other.requests();
    assert_eq!(external.len(), 1);
    assert_eq!(external[0].path, "/signed?signature=a%2Bb");
    assert!(!external[0].headers.contains_key("x-dataverse-key"));
}
