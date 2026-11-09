mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::{
    bhl::{BhlOperation, BhlService},
    figshare::FigshareService,
};
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn api_v3_keeps_rights_and_exposes_lazy_content() {
    let f = HttpFixture::new();
    let raw = String::from_utf8(fixture!("bhl", "item.json").await)
        .unwrap()
        .replace("ORIGIN", f.endpoint());
    f.respond("/api3?op=GetItemMetadata&format=json&id=125582&idtype=bhl&pages=f&ocr=f&parts=f&apikey=test",StatusCode::OK,raw.into_bytes());
    f.file(
        "/itemtext/125582?apikey=test",
        Bytes::from_static(b"original OCR"),
    );
    let s = BhlService::with_services(
        providers::connection(&f).with_query("apikey", "test"),
        FigshareService::with_connection(providers::connection(&f)),
    );
    let files = s
        .item(std::num::NonZeroU64::new(125582).unwrap())
        .await
        .unwrap();
    assert_eq!(files.metadata().value()["Result"][0]["Rights"], "retained");
    assert_eq!(f.requests().len(), 1);
    byte_accessor::assert_read_contract(files, "text".into(), b"original OCR")
        .await
        .unwrap();
}

#[tokio::test]
async fn bulk_export_uses_shared_figshare_and_api_denials_are_not_empty_data() {
    let f = HttpFixture::new();
    let raw = String::from_utf8(fixture!("figshare", "article.json").await)
        .unwrap()
        .replace("ORIGIN", f.endpoint());
    f.respond(
        "/v2/articles/21422193/versions/4",
        StatusCode::OK,
        raw.into_bytes(),
    );
    f.respond(
        "/api3?op=GetStats&format=json",
        StatusCode::OK,
        Bytes::from_static(
            b"{\"Status\":\"unauthorized\",\"ErrorMessage\":\"key rejected\",\"Result\":[]}",
        ),
    );
    let s = BhlService::with_services(
        providers::connection(&f),
        FigshareService::with_connection(providers::connection(&f)),
    );
    assert_eq!(
        s.bulk_export(std::num::NonZeroU32::new(4))
            .await
            .unwrap()
            .metadata()
            .value()["version"],
        4
    );
    assert_eq!(
        s.get(BhlOperation::Stats, &[]).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(
        s.get(BhlOperation::Stats, &[("format", "xml")])
            .await
            .is_err()
    );
}
