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
    let service = DataverseService::new(http.endpoint()).unwrap();
    let service = if bearer {
        service.with_bearer_token("test-secret").unwrap()
    } else {
        service.with_token("test-secret").unwrap()
    };
    service.with_context(opendal::OperationContext::new())
}

#[tokio::test]
async fn authenticates_metadata_and_restricted_original_reads() {
    for bearer in [false, true] {
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
        byte_accessor::assert_ranged_read_contract(
            dataset,
            ByteReadRequest::new("table.tab"),
            b"ORIGINAL",
            &[1..4, 0..0, 1..4],
        )
        .await
        .unwrap();
        let (header, value, absent) = if bearer {
            ("authorization", "Bearer test-secret", "x-dataverse-key")
        } else {
            ("x-dataverse-key", "test-secret", "authorization")
        };
        let requests = http.requests();
        assert!(requests.len() > 1);
        for request in requests {
            assert_eq!(request.headers.get(header).unwrap(), value);
            assert!(request.headers.get(header).unwrap().is_sensitive());
            assert!(!request.headers.contains_key(absent));
            assert!(!request.path.contains("test-secret"));
        }
    }
}

#[tokio::test]
async fn credentials_follow_same_origin_redirects_but_never_return_after_leaving() {
    use open_arrow_access_test_support::http::{file_response, response};
    for bearer in [false, true] {
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
                return Ok(file_response(&request, b"%PDFdata".to_vec()));
            }
            let location = if request.uri().path() == "/api/access/datafile/101" {
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
        let dataset = authenticated(&http, bearer)
            .dataset(PID, "1.0")
            .await
            .unwrap();
        let mut reader = dataset
            .open("papers/2026/paper #1%.pdf".into())
            .await
            .unwrap();
        assert_eq!(reader.read_range(1..4).await.unwrap().as_ref(), b"PDF");
        assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"%PDFdata");
        for request in other.requests().into_iter().chain(
            http.requests()
                .into_iter()
                .filter(|r| r.path == "/returned"),
        ) {
            assert!(!request.headers.contains_key("x-dataverse-key"));
            assert!(!request.headers.contains_key("authorization"));
        }
        let header = if bearer {
            "authorization"
        } else {
            "x-dataverse-key"
        };
        assert!(
            http.requests()
                .iter()
                .filter(|r| r.path == "/same-origin")
                .all(|r| r.headers.contains_key(header))
        );
        assert!(
            other
                .requests()
                .iter()
                .all(|r| r.path == "/signed?signature=a%2Bb")
        );
    }
}

#[tokio::test]
async fn rejects_invalid_tokens_and_preserves_permission_errors() {
    let http = HttpFixture::new();
    for token in ["", " ", "secret\r\ninjected: true"] {
        assert_eq!(
            DataverseService::new(http.endpoint())
                .unwrap()
                .with_token(token)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            DataverseService::new(http.endpoint())
                .unwrap()
                .with_bearer_token(token)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
    assert!(http.requests().is_empty());
    for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
        http.respond(METADATA, status, Vec::new());
        let error = authenticated(&http, false)
            .dataset(PID, "1.0")
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::from_http_status(status.as_u16()));
        assert!(!error.to_string().contains("test-secret"));
    }
}

#[tokio::test]
async fn last_authentication_scheme_wins_and_custom_context_survives_setters() {
    for bearer in [false, true] {
        let http = HttpFixture::new();
        http.respond(
            METADATA,
            StatusCode::OK,
            fixture!("dataverse", "empty.json").await,
        );
        let service = DataverseService::new(http.endpoint())
            .unwrap()
            .with_context(opendal::OperationContext::new());
        let service = if bearer {
            service
                .with_token("old-secret")
                .unwrap()
                .with_bearer_token("new-secret")
                .unwrap()
        } else {
            service
                .with_bearer_token("old-secret")
                .unwrap()
                .with_token("new-secret")
                .unwrap()
        };
        service.dataset(PID, "1.0").await.unwrap();
        let requests = http.requests();
        let headers = &requests[0].headers;
        if bearer {
            assert_eq!(headers["authorization"], "Bearer new-secret");
            assert!(!headers.contains_key("x-dataverse-key"));
        } else {
            assert_eq!(headers["x-dataverse-key"], "new-secret");
            assert!(!headers.contains_key("authorization"));
        }
    }
}

#[tokio::test]
async fn token_does_not_bypass_server_file_permissions() {
    let http = HttpFixture::new();
    http.respond(
        METADATA,
        StatusCode::OK,
        fixture!("dataverse", "restricted.json").await,
    );
    http.respond(
        "/api/access/datafile/102?format=original",
        StatusCode::FORBIDDEN,
        Vec::new(),
    );
    let dataset = authenticated(&http, false)
        .dataset(PID, "1.0")
        .await
        .unwrap();
    let mut reader = dataset.open("table.tab".into()).await.unwrap();
    let error = collect_bytes(reader.as_mut()).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
}
