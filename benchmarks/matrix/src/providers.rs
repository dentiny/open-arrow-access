use open_arrow_access_core::{Accessor, ByteReadRequest, ReadRequest};
use open_arrow_access_service as s;
use open_arrow_access_test_support::{MemoryAccessor, fixture_path, http::HttpFixture};
use opendal::OperationContext;
use serde_json::json;
use std::{
    hint::black_box,
    num::{NonZeroU32, NonZeroU64},
    time::{Duration, Instant},
};

pub fn list() {
    println!("provider-figshare");
    println!("provider-federal-register");
    println!("provider-govinfo");
    println!("provider-hathifiles");
    println!("provider-library-of-congress");
    println!("provider-openstax");
    println!("provider-regulations");
    println!("provider-uspto");
    println!("provider-zenodo");
    println!("provider-dataverse");
    println!("provider-internet-archive");
    println!("provider-semantic-scholar");
    println!("provider-bhl");
    println!("provider-doab");
    println!("provider-oapen");
    println!("provider-arxiv-metadata");
    println!("provider-biorxiv");
    println!("provider-arxiv");
    println!("provider-pmc");
    println!("provider-gutenberg");
    println!("provider-courtlistener");
    println!("provider-meca");
    println!("provider-warc");
    println!("provider-huggingface");
}
pub async fn run(case: &str, duration: Duration) {
    let f = HttpFixture::new();
    let c = s::common::Connection::new(f.endpoint())
        .unwrap()
        .with_context(OperationContext::new());
    let (format, file, path) = match case {
        "provider-figshare" => (
            "figshare",
            "article.json",
            "/v2/articles/21422193/versions/4",
        ),
        "provider-federal-register" => (
            "federal-register",
            "document.json",
            "/api/v1/documents/2026-22789.json",
        ),
        "provider-govinfo" => ("govinfo", "bulk.json", "/bulkdata/json/FR/2026"),
        "provider-hathifiles" => (
            "hathifiles",
            "listing.json",
            "/files/hathifiles/hathi_file_list.json",
        ),
        "provider-library-of-congress" => (
            "library-of-congress",
            "resource.json",
            "/resource/r1/?fo=json",
        ),
        "provider-openstax" => (
            "openstax",
            "commit.json",
            "/repos/openstax/osbooks-physics/commits/main",
        ),
        "provider-regulations" => (
            "regulations",
            "record.json",
            "/v4/documents/DOC-1?include=attachments",
        ),
        "provider-uspto" => (
            "uspto",
            "product.json",
            "/api/v1/datasets/products/PTGRXML?includeFiles=true&offset=0&limit=100",
        ),
        "provider-zenodo" => ("zenodo", "record.json", "/api/records/3551612"),
        "provider-dataverse" => (
            "dataverse",
            "dataset.json",
            "/api/v1/datasets/:persistentId/versions/1.0?persistentId=doi%3A10.7910%2FDVN%2FA%26B%2B&excludeFiles=false&excludeMetadataBlocks=true",
        ),
        "provider-internet-archive" => ("internet_archive", "item.json", "/metadata/sample-item"),
        "provider-semantic-scholar" => (
            "semantic-scholar",
            "dataset.json",
            "/datasets/v1/release/2026-10-27/dataset/s2orc",
        ),
        "provider-bhl" => (
            "bhl",
            "item.json",
            "/api3?op=GetItemMetadata&format=json&id=125582&idtype=bhl&pages=f&ocr=f&parts=f",
        ),
        "provider-doab" => (
            "oai",
            "page.xml",
            "/oai/request?verb=ListRecords&metadataPrefix=xoai",
        ),
        "provider-oapen" => (
            "oai",
            "page.xml",
            "/oai/request?verb=ListRecords&metadataPrefix=xoai",
        ),
        "provider-arxiv-metadata" => (
            "oai",
            "page.xml",
            "/oai?verb=ListRecords&metadataPrefix=arXivRaw",
        ),
        "provider-biorxiv" => ("", "", "/details/biorxiv/2026-11-01/2026-11-02/0/json"),
        "provider-arxiv" => ("arxiv", "pdf/arXiv_pdf_manifest.xml", ""),
        "provider-pmc" => ("pmc", "metadata.json", ""),
        "provider-gutenberg" => ("gutenberg", "book.rdf", ""),
        "provider-courtlistener" => ("courtlistener", "courts-2026-09-30.csv.bz2", ""),
        "provider-meca" => ("meca", "valid.meca", ""),
        "provider-warc" => ("commoncrawl", "crawl.paths", ""),
        "provider-huggingface" => (
            "huggingface",
            "revision.json",
            "/api/datasets/acme/corpus/revision/main",
        ),
        _ => panic!("unknown provider benchmark: {case}"),
    };
    let data = if case == "provider-biorxiv" {
        format!(r#"{{"messages":[{{"status":"ok","cursor":"0","total":"1"}}],"collection":[{{"doi":"a","version":"1","jatsxml":"{}/paper.xml"}}]}}"#,f.endpoint()).into_bytes()
    } else {
        std::fs::read(fixture_path!(format, file)).unwrap()
    };
    let data = String::from_utf8(data.clone())
        .map(|v| v.replace("ORIGIN", f.endpoint()).into_bytes())
        .unwrap_or(data);
    if !path.is_empty() {
        f.respond(path, http::StatusCode::OK, data.clone());
    }
    if case == "provider-semantic-scholar" {
        f.respond(
            "/datasets/v1/release/2026-10-27",
            http::StatusCode::OK,
            std::fs::read(fixture_path!("semantic-scholar", "release.json")).unwrap(),
        );
    }
    let hf = if case == "provider-huggingface" {
        let bytes = open_arrow_access_core::Bytes::from(
            std::fs::read(fixture_path!("parquet", "ids.parquet")).unwrap(),
        );
        let metadata = data.clone();
        f.handle(move |request| {
  let uri=request.uri().to_string();
  if uri.contains("/revision/"){return Ok(open_arrow_access_test_support::http::response(http::StatusCode::OK,metadata.clone()));}
  let entry=json!({"type":"file","oid":"1234567890123456789012345678901234567890","path":"default/train/0000.parquet","size":bytes.len(),"lfs":{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":bytes.len(),"pointerSize":100}});
  if uri.contains("/tree/") || uri.contains("/paths-info/"){ return Ok(open_arrow_access_test_support::http::response(http::StatusCode::OK,serde_json::to_vec(&vec![entry]).unwrap())); }
  if uri.contains("/resolve/"){return Ok(open_arrow_access_test_support::http::file_response(&request,bytes.clone()));}
  panic!("unexpected HF request: {uri}")
 });
        Some(
            s::huggingface::HuggingFaceService::new(
                s::huggingface::HuggingFaceOptions::default()
                    .with_endpoint(f.endpoint())
                    .with_token("benchmark-only"),
            )
            .await
            .unwrap(),
        )
    } else {
        None
    };
    let zenodo = s::zenodo::ZenodoService::with_endpoint(f.endpoint()).unwrap();
    let dataverse = s::dataverse::DataverseService::new(f.endpoint()).unwrap();
    let internet_archive =
        s::internet_archive::InternetArchiveService::with_endpoint(f.endpoint()).unwrap();
    let memory = MemoryAccessor::new(&data, 64 * 1024).with_ranges();
    let mut elapsed = Duration::ZERO;
    let mut iterations = 0;
    let mut requests = 0;
    loop {
        f.clear_requests();
        memory.observations.requests.lock().unwrap().clear();
        memory.observations.ranges.lock().unwrap().clear();
        let start = Instant::now();
        match case {
            "provider-figshare" => {
                black_box(
                    s::figshare::FigshareService::with_connection(c.clone())
                        .article(NonZeroU64::new(21422193).unwrap(), NonZeroU32::new(4))
                        .await
                        .unwrap(),
                );
            }
            "provider-federal-register" => {
                black_box(
                    s::federal_register::FederalRegisterService::with_connection(c.clone())
                        .document("2026-22789")
                        .await
                        .unwrap(),
                );
            }
            "provider-govinfo" => {
                black_box(
                    s::govinfo::GovInfoService::with_connections(c.clone(), c.clone())
                        .bulk_directory("FR/2026")
                        .await
                        .unwrap(),
                );
            }
            "provider-hathifiles" => {
                black_box(
                    s::hathifiles::HathifilesService::with_connection(c.clone())
                        .catalog()
                        .await
                        .unwrap(),
                );
            }
            "provider-library-of-congress" => {
                black_box(
                    s::library_of_congress::LibraryOfCongressService::with_connections(
                        c.clone(),
                        c.clone(),
                    )
                    .resource("r1")
                    .await
                    .unwrap(),
                );
            }
            "provider-openstax" => {
                black_box(
                    s::openstax::OpenStaxService::with_connections(c.clone(), c.clone())
                        .book("osbooks-physics", "main")
                        .await
                        .unwrap(),
                );
            }
            "provider-regulations" => {
                black_box(
                    s::regulations::RegulationsService::with_connection(c.clone())
                        .record(s::regulations::RegulationsResource::Documents, "DOC-1")
                        .await
                        .unwrap(),
                );
            }
            "provider-uspto" => {
                black_box(
                    s::uspto::UsptoService::with_connection(c.clone())
                        .product("PTGRXML", 0, 100)
                        .await
                        .unwrap(),
                );
            }
            "provider-zenodo" => {
                black_box(
                    zenodo
                        .record(NonZeroU64::new(3551612).unwrap())
                        .await
                        .unwrap(),
                );
            }
            "provider-dataverse" => {
                black_box(
                    dataverse
                        .dataset("doi:10.7910/DVN/A&B+", "1.0")
                        .await
                        .unwrap(),
                );
            }
            "provider-internet-archive" => {
                black_box(internet_archive.item("sample-item").await.unwrap());
            }
            "provider-semantic-scholar" => {
                black_box(
                    s::semantic_scholar::SemanticScholarService::with_connection(c.clone())
                        .dataset("2026-10-27", "s2orc")
                        .await
                        .unwrap(),
                );
            }
            "provider-bhl" => {
                black_box(
                    s::bhl::BhlService::with_services(
                        c.clone(),
                        s::figshare::FigshareService::with_connection(c.clone()),
                    )
                    .item(NonZeroU64::new(125582).unwrap())
                    .await
                    .unwrap(),
                );
            }
            "provider-doab" => {
                black_box(
                    s::doab::DoabService::with_connection(c.clone())
                        .unwrap()
                        .records(None, None, None)
                        .await
                        .unwrap(),
                );
            }
            "provider-oapen" => {
                black_box(
                    s::oapen::OapenService::with_connection(c.clone())
                        .unwrap()
                        .records(None, None, None)
                        .await
                        .unwrap(),
                );
            }
            "provider-arxiv-metadata" => {
                black_box(
                    s::arxiv_metadata::ArxivMetadataService::with_connection(c.clone())
                        .unwrap()
                        .records(None, None, None)
                        .await
                        .unwrap(),
                );
            }
            "provider-biorxiv" => {
                let service = s::biorxiv::BiorxivService::with_connection(c.clone());
                let page = service
                    .details(s::biorxiv::Server::Biorxiv, "2026-11-01", "2026-11-02", 0)
                    .await
                    .unwrap();
                black_box(service.jats(page).unwrap());
            }
            "provider-arxiv" => {
                black_box(
                    s::arxiv::ArxivService::new(memory.clone())
                        .manifest(s::arxiv::ArxivKind::Pdf, Some(2410), Default::default())
                        .await
                        .unwrap(),
                );
            }
            "provider-pmc" => {
                assert_eq!(
                    super::drain(
                        s::pmc::PmcAccessor::with_default_decoder(memory.clone())
                            .open(ReadRequest::new("data"))
                            .await
                            .unwrap()
                    )
                    .await,
                    1
                );
            }
            "provider-gutenberg" => {
                assert_eq!(
                    super::drain(
                        s::gutenberg::GutenbergAccessor::with_default_decoder(memory.clone())
                            .open(ReadRequest::new("data"))
                            .await
                            .unwrap()
                    )
                    .await,
                    1
                );
            }
            "provider-courtlistener" => {
                let mut reader = s::courtlistener::CourtListenerBulkAccessor::new(memory.clone())
                    .open(ByteReadRequest::new("bulk-data/courts-2026-09-30.csv.bz2"))
                    .await
                    .unwrap();
                while let Some(bytes) = reader.next().await.unwrap() {
                    black_box(bytes);
                }
            }
            "provider-meca" => {
                let input = memory
                    .open(ByteReadRequest::new("bundle.meca"))
                    .await
                    .unwrap();
                black_box(
                    s::meca::MecaArchive::open(input, Default::default())
                        .await
                        .unwrap(),
                );
            }
            "provider-warc" => {
                black_box(
                    s::warc::CommonCrawlWarcAccessor::new(memory.clone(), ())
                        .resolve(s::warc::CommonCrawlWarcRequest::new("CC-MAIN-2022-40").unwrap())
                        .await
                        .unwrap(),
                );
            }
            "provider-huggingface" => {
                assert_eq!(
                    super::drain(
                        hf.as_ref()
                            .unwrap()
                            .dataset(s::huggingface::HuggingFaceDatasetRequest::new(
                                "acme/corpus",
                                "default",
                                "train"
                            ))
                            .await
                            .unwrap()
                    )
                    .await,
                    3
                );
            }
            _ => unreachable!(),
        }
        let time = start.elapsed();
        if iterations > 0 {
            elapsed += time;
            requests += f.requests().len();
        }
        iterations += 1;
        if elapsed >= duration {
            break;
        }
    }
    let iterations = iterations - 1;
    println!(
        "{}",
        json!({"case":case,"iterations":iterations,"wall_seconds":elapsed.as_secs_f64(),"ns_per_iteration":elapsed.as_nanos() as f64/iterations as f64,"input_bytes":data.len(),"http_requests":requests,"scope":"local fixture resolution/decoding; no network latency or attachment download"})
    );
}
