use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{ByteAccessor, ListRequest};
use open_arrow_access_service::{self as svc, common::Connection};
use open_arrow_access_test_support::{fixture, http::HttpFixture};
use std::{
    hint::black_box,
    num::{NonZeroU32, NonZeroU64},
    time::Instant,
};

async fn list(files: impl ByteAccessor) -> usize {
    files
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_fold(0, |n, object| async move {
            black_box(object);
            Ok(n + 1)
        })
        .await
        .unwrap()
}

pub async fn run(name: &str, iterations: usize) {
    let f = HttpFixture::new();
    let (dir, file, path) = match name {
        "biorxiv" => ("", "", "/details/biorxiv/2026-11-01/2026-11-02/0/json"),
        "zenodo" => ("zenodo", "record.json", "/api/records/3551612"),
        "dataverse" => (
            "dataverse",
            "dataset.json",
            "/api/v1/datasets/:persistentId/versions/1.0?persistentId=doi%3A10.7910%2FDVN%2FA%26B%2B&excludeFiles=false&excludeMetadataBlocks=true",
        ),
        "internet-archive" => ("internet_archive", "item.json", "/metadata/sample-item"),
        "figshare" | "bhl" => (
            "figshare",
            "article.json",
            "/v2/articles/21422193/versions/4",
        ),
        "hathifiles" => (
            "hathifiles",
            "listing.json",
            "/files/hathifiles/hathi_file_list.json",
        ),
        "federal-register" => (
            "federal-register",
            "document.json",
            "/api/v1/documents/2026-22789.json",
        ),
        "govinfo" => ("govinfo", "bulk.json", "/bulkdata/json/FR/2026"),
        "library-of-congress" => (
            "library-of-congress",
            "resource.json",
            "/resource/r1/?fo=json",
        ),
        "openstax" => (
            "openstax",
            "commit.json",
            "/repos/openstax/osbooks-physics/commits/main",
        ),
        "semantic-scholar" => (
            "semantic-scholar",
            "dataset.json",
            "/datasets/v1/release/2026-10-27/dataset/s2orc",
        ),
        "regulations" => (
            "regulations",
            "record.json",
            "/v4/documents/DOC-1?include=attachments",
        ),
        "uspto" => (
            "uspto",
            "product.json",
            "/api/v1/datasets/products/PTGRXML?includeFiles=true&offset=0&limit=100",
        ),
        "doab" | "oapen" => (
            "oai",
            "page.xml",
            "/oai/request?verb=ListRecords&metadataPrefix=xoai",
        ),
        "arxiv" => (
            "oai",
            "page.xml",
            "/oai?verb=ListRecords&metadataPrefix=arXivRaw",
        ),
        _ => panic!("unknown provider {name}"),
    };
    let raw = if name == "biorxiv" {
        format!(
            r#"{{"messages":[{{"status":"ok","cursor":"0","total":"1"}}],"collection":[{{"doi":"a","version":"1","jatsxml":"{}/paper.xml"}}]}}"#,
            f.endpoint()
        )
    } else {
        String::from_utf8(fixture!(dir, file).await)
            .unwrap()
            .replace("ORIGIN", f.endpoint())
    };
    let bytes = raw.len();
    f.respond(path, StatusCode::OK, raw.into_bytes());
    if name == "semantic-scholar" {
        f.respond(
            "/datasets/v1/release/2026-10-27",
            StatusCode::OK,
            fixture!("semantic-scholar", "release.json").await,
        );
    }
    let c = Connection::new(f.endpoint())
        .unwrap()
        .with_context(opendal::OperationContext::new());
    let id = NonZeroU64::new(21422193).unwrap();
    let version = NonZeroU32::new(4);
    let start = Instant::now();
    let mut units = 0;
    for _ in 0..iterations {
        let count = match name {
            "biorxiv" => {
                let s = svc::biorxiv::BiorxivService::with_connection(c.clone());
                let page = s
                    .details(svc::biorxiv::Server::Biorxiv, "2026-11-01", "2026-11-02", 0)
                    .await
                    .unwrap();
                list(s.jats(page).unwrap()).await
            }
            "zenodo" => {
                list(
                    svc::zenodo::ZenodoService::with_endpoint(f.endpoint())
                        .unwrap()
                        .record(NonZeroU64::new(3551612).unwrap())
                        .await
                        .unwrap(),
                )
                .await
            }
            "dataverse" => {
                list(
                    svc::dataverse::DataverseService::new(f.endpoint())
                        .unwrap()
                        .dataset("doi:10.7910/DVN/A&B+", "1.0")
                        .await
                        .unwrap(),
                )
                .await
            }
            "internet-archive" => {
                list(
                    svc::internet_archive::InternetArchiveService::with_endpoint(f.endpoint())
                        .unwrap()
                        .item("sample-item")
                        .await
                        .unwrap(),
                )
                .await
            }
            "figshare" => {
                list(
                    svc::figshare::FigshareService::with_connection(c.clone())
                        .article(id, version)
                        .await
                        .unwrap(),
                )
                .await
            }
            "bhl" => {
                list(
                    svc::bhl::BhlService::with_services(
                        c.clone(),
                        svc::figshare::FigshareService::with_connection(c.clone()),
                    )
                    .bulk_export(version)
                    .await
                    .unwrap(),
                )
                .await
            }
            "hathifiles" => {
                list(
                    svc::hathifiles::HathifilesService::with_connection(c.clone())
                        .catalog()
                        .await
                        .unwrap(),
                )
                .await
            }
            "federal-register" => {
                list(
                    svc::federal_register::FederalRegisterService::with_connection(c.clone())
                        .document("2026-22789")
                        .await
                        .unwrap(),
                )
                .await
            }
            "govinfo" => {
                list(
                    svc::govinfo::GovInfoService::with_connections(c.clone(), c.clone())
                        .bulk_directory("FR/2026")
                        .await
                        .unwrap(),
                )
                .await
            }
            "library-of-congress" => {
                list(
                    svc::library_of_congress::LibraryOfCongressService::with_connections(
                        c.clone(),
                        c.clone(),
                    )
                    .resource("r1")
                    .await
                    .unwrap(),
                )
                .await
            }
            "openstax" => {
                list(
                    svc::openstax::OpenStaxService::with_connections(c.clone(), c.clone())
                        .book("osbooks-physics", "main")
                        .await
                        .unwrap(),
                )
                .await
            }
            "semantic-scholar" => {
                list(
                    svc::semantic_scholar::SemanticScholarService::with_connection(c.clone())
                        .dataset("2026-10-27", "s2orc")
                        .await
                        .unwrap(),
                )
                .await
            }
            "regulations" => {
                list(
                    svc::regulations::RegulationsService::with_connection(c.clone())
                        .record(svc::regulations::RegulationsResource::Documents, "DOC-1")
                        .await
                        .unwrap(),
                )
                .await
            }
            "uspto" => {
                list(
                    svc::uspto::UsptoService::with_connection(c.clone())
                        .product("PTGRXML", 0, 100)
                        .await
                        .unwrap(),
                )
                .await
            }
            "doab" => {
                svc::doab::DoabService::with_connection(c.clone())
                    .unwrap()
                    .records(None, None, None)
                    .await
                    .unwrap()
                    .record_count
            }
            "oapen" => {
                svc::oapen::OapenService::with_connection(c.clone())
                    .unwrap()
                    .records(None, None, None)
                    .await
                    .unwrap()
                    .record_count
            }
            "arxiv" => {
                svc::arxiv_metadata::ArxivMetadataService::with_connection(c.clone())
                    .unwrap()
                    .records(None, None, None)
                    .await
                    .unwrap()
                    .record_count
            }
            _ => unreachable!(),
        };
        assert!(count > 0);
        units += count;
        f.clear_requests();
    }
    println!(
        "{}",
        serde_json::json!({"case":format!("service-{name}"),"iterations":iterations,"input_bytes":bytes,"units":units,"seconds":start.elapsed().as_secs_f64(),"transport":"in-process deterministic HTTP; no network latency"})
    );
}
