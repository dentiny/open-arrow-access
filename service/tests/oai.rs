mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::oai::OaiService;
use open_arrow_access_test_support::{collect_bytes, fixture, http::HttpFixture};
use providers::connection;

#[tokio::test]
async fn raw_records_tokens_and_deleted_headers() {
    let f = HttpFixture::new();
    let bytes = Bytes::from(fixture!("oai", "page.xml").await);
    f.respond(
        "/oai?verb=ListRecords&metadataPrefix=arXivRaw&from=2026-01-01",
        StatusCode::OK,
        bytes.clone(),
    );
    f.respond(
        "/oai?verb=ListRecords&resumptionToken=next%2Btoken%26x",
        StatusCode::OK,
        Bytes::from_static(b"<OAI-PMH><ListRecords><resumptionToken/></ListRecords></OAI-PMH>"),
    );
    let s = OaiService::new(connection(&f), "oai", "arXivRaw").unwrap();
    let page = s.records(Some("2026-01-01"), None, None).await.unwrap();
    assert_eq!(page.record_count, 2);
    assert_eq!(page.resumption_token.as_deref(), Some("next+token&x"));
    assert_eq!(page.bytes, bytes);
    assert_eq!(collect_bytes(page.reader().as_mut()).await.unwrap(), bytes);
    assert!(
        s.resume(page.resumption_token.as_ref().unwrap())
            .await
            .unwrap()
            .resumption_token
            .is_none()
    );
    assert_eq!(f.requests().len(), 2);
}

#[tokio::test]
async fn protocol_errors_and_truncated_xml() {
    let f = HttpFixture::new();
    let s = OaiService::new(connection(&f), "oai", "oai_dc").unwrap();
    for name in ["bad-token.xml", "truncated.xml"] {
        f.respond(
            "/oai?verb=ListRecords&metadataPrefix=oai_dc",
            StatusCode::OK,
            fixture!("oai", name).await,
        );
        assert_eq!(
            s.records(None, None, None).await.unwrap_err().kind(),
            ErrorKind::Decode
        );
    }
    f.respond(
        "/oai?verb=ListRecords&metadataPrefix=oai_dc",
        StatusCode::OK,
        fixture!("oai", "empty.xml").await,
    );
    assert_eq!(s.records(None, None, None).await.unwrap().record_count, 0);
    assert!(s.resume("").await.is_err());
}
