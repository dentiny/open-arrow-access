//! No-network provider microbenchmarks; a full response body, no test request log.
use futures::stream;
use http::{Request, Response};
use open_arrow_access_core::{Accessor, Bytes, ReadRequest};
use open_arrow_access_service as svc;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext};
use serde_json::json;
use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroU64},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use svc::common::Connection;

#[derive(Clone, Debug)]
struct Transport {
    routes: Arc<HashMap<String, Bytes>>,
    requests: Arc<AtomicUsize>,
    hf_metadata: Bytes,
    hf_parquet: Bytes,
    hf_revision: Bytes,
}
impl HttpTransport for Transport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let path = request.uri().path_and_query().unwrap().as_str();
        let bytes = if let Some(bytes) = self.routes.get(path) {
            bytes.clone()
        } else if path.contains("/revision/") {
            self.hf_revision.clone()
        } else if path.contains("/tree/") || path.contains("/paths-info/") {
            self.hf_metadata.clone()
        } else if path.contains("/resolve/") {
            self.hf_parquet.clone()
        } else {
            panic!("unregistered route {path}")
        };
        let total = bytes.len();
        let (start, end) = request
            .headers()
            .get("range")
            .map(|range| {
                let (a, b) = range
                    .to_str()
                    .unwrap()
                    .strip_prefix("bytes=")
                    .unwrap()
                    .split_once('-')
                    .unwrap();
                (
                    a.parse::<usize>().unwrap(),
                    if b.is_empty() {
                        total
                    } else {
                        (b.parse::<usize>().unwrap() + 1).min(total)
                    },
                )
            })
            .unwrap_or((0, total));
        let len = end - start;
        let mut response = Response::builder()
            .status(if request.headers().contains_key("range") {
                206
            } else {
                200
            })
            .header("content-length", len);
        if request.headers().contains_key("range") {
            response = response.header(
                "content-range",
                format!("bytes {start}-{}/{total}", end - 1),
            );
        }
        let body = if request.method() == http::Method::HEAD {
            Bytes::new()
        } else {
            bytes.slice(start..end)
        };
        let body_len = body.len() as u64;
        Ok(response
            .body(HttpBody::new(
                stream::iter([Ok(Buffer::from(body))]),
                Some(body_len),
            ))
            .unwrap())
    }
}
fn fixture(directory: &str, name: &str) -> Bytes {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata");
    let bytes = std::fs::read(root.join(directory).join(name)).unwrap();
    match String::from_utf8(bytes) {
        Ok(x) => x.replace("ORIGIN", "https://bench.invalid").into(),
        Err(error) => error.into_bytes().into(),
    }
}

#[derive(Clone)]
struct Inventory {
    manifest: super::Memory,
    first: super::Memory,
    second: super::Memory,
}
#[open_arrow_access_core::async_trait]
impl Accessor for Inventory {
    type Request = open_arrow_access_core::ByteReadRequest;
    type Reader = dyn open_arrow_access_core::ByteReader;
    fn capability(&self) -> open_arrow_access_core::Capability {
        super::Memory(Bytes::new()).capability()
    }
    async fn open(
        &self,
        request: Self::Request,
    ) -> open_arrow_access_core::Result<Box<Self::Reader>> {
        let source = if request.object.locator().ends_with("first.csv.gz") {
            &self.first
        } else if request.object.locator().ends_with("second.csv.gz") {
            &self.second
        } else {
            &self.manifest
        };
        source.open(request).await
    }
}
impl open_arrow_access_core::ByteAccessor for Inventory {}

pub async fn run(
    name: &str,
    iterations: usize,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    assert!(iterations > 0);
    let mut routes = HashMap::new();
    for (path, dir, file) in [
        (
            "/v2/articles/21422193/versions/4",
            "figshare",
            "article.json",
        ),
        (
            "/files/hathifiles/hathi_file_list.json",
            "hathifiles",
            "listing.json",
        ),
        (
            "/api/v1/documents/2026-22789.json",
            "federal-register",
            "document.json",
        ),
        ("/bulkdata/json/FR/2026", "govinfo", "bulk.json"),
        ("/packages/P1/summary", "govinfo", "package.json"),
        (
            "/resource/r1/?fo=json",
            "library-of-congress",
            "resource.json",
        ),
        (
            "/repos/openstax/osbooks-physics/commits/main",
            "openstax",
            "commit.json",
        ),
        (
            "/datasets/v1/release/latest",
            "semantic-scholar",
            "release.json",
        ),
        (
            "/datasets/v1/release/2026-10-27/dataset/s2orc",
            "semantic-scholar",
            "dataset.json",
        ),
        (
            "/api/v1/datasets/products/PTGRXML?includeFiles=true&offset=0&limit=100",
            "uspto",
            "product.json",
        ),
        (
            "/v4/documents/DOC-1?include=attachments",
            "regulations",
            "record.json",
        ),
        (
            "/api3?op=GetItemMetadata&format=json&id=125582&idtype=bhl&pages=f&ocr=f&parts=f",
            "bhl",
            "item.json",
        ),
        (
            "/oai/request?verb=ListRecords&metadataPrefix=xoai",
            "oai",
            "page.xml",
        ),
        ("/api/records/3551612", "zenodo", "record.json"),
        (
            "/api/v1/datasets/:persistentId/versions/1.0?persistentId=doi%3A10.7910%2FDVN%2FA%26B%2B&excludeFiles=false&excludeMetadataBlocks=true",
            "dataverse",
            "dataset.json",
        ),
        ("/metadata/sample-item", "internet_archive", "item.json"),
    ] {
        routes.insert(path.to_owned(), fixture(dir, file));
    }
    routes.insert("/details/biorxiv/2026-11-01/2026-11-02/0/json".into(),Bytes::from_static(br#"{"messages":[{"status":"ok","cursor":"0","total":"1"}],"collection":[{"doi":"a","version":"1","license":"cc_by","jatsxml":"https://bench.invalid/paper.xml"}]}"#));
    let requests = Arc::new(AtomicUsize::new(0));
    let transport = Transport {
        routes: Arc::new(routes),
        hf_metadata: {
            let entries: serde_json::Value =
                serde_json::from_slice(&fixture("huggingface/exports", "train.json")).unwrap();
            serde_json::to_vec(
                &entries
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|x| &x["metadata"])
                    .collect::<Vec<_>>(),
            )
            .unwrap()
            .into()
        },
        hf_parquet: fixture("parquet/directory", "a.parquet"),
        hf_revision: fixture("huggingface", "revision.json"),
        requests: requests.clone(),
    };
    HttpTransporter::install_default(transport.clone());
    let context = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
    let conn = || {
        Connection::new("https://bench.invalid")
            .unwrap()
            .with_context(context.clone())
    };
    // The concrete service is built once; each iteration repeats the public operation.
    macro_rules! bench {
        ($operation:expr) => {{
            std::hint::black_box($operation);
            requests.store(0, Ordering::Relaxed);
            let allocations = super::AllocationRegion::new();
            let start = Instant::now();
            for _ in 0..iterations {
                std::hint::black_box($operation);
            }
            let mut output = json!({
                "provider": name,
                "iterations": iterations,
                "seconds": start.elapsed().as_secs_f64(),
                "http_requests": requests.load(Ordering::Relaxed),
                "transport": "in-process immutable full-body response; excludes network and TLS"
            });
            allocations.record(&mut output);
            println!("{output}");
        }};
    }
    match name {
        "figshare" => {
            let s = svc::figshare::FigshareService::with_connection(conn());
            bench!(
                s.article(NonZeroU64::new(21422193).unwrap(), NonZeroU32::new(4))
                    .await?
            );
        }
        "hathifiles" => {
            let s = svc::hathifiles::HathifilesService::with_connection(conn());
            bench!(s.catalog().await?);
        }
        "federal-register" => {
            let s = svc::federal_register::FederalRegisterService::with_connection(conn());
            bench!(s.document("2026-22789").await?);
        }
        "govinfo" => {
            let s = svc::govinfo::GovInfoService::with_connections(conn(), conn());
            bench!(s.bulk_directory("FR/2026").await?);
        }
        "library-of-congress" => {
            let s = svc::library_of_congress::LibraryOfCongressService::with_connections(
                conn(),
                conn(),
            );
            bench!(s.resource("r1").await?);
        }
        "openstax" => {
            let s = svc::openstax::OpenStaxService::with_connections(conn(), conn());
            bench!(s.book("osbooks-physics", "main").await?);
        }
        "semantic-scholar" => {
            let s = svc::semantic_scholar::SemanticScholarService::with_connection(conn());
            bench!(s.dataset("latest", "s2orc").await?);
        }
        "uspto" => {
            let s = svc::uspto::UsptoService::with_connection(conn());
            bench!(s.product("PTGRXML", 0, 100).await?);
        }
        "regulations" => {
            let s = svc::regulations::RegulationsService::with_connection(conn());
            bench!(
                s.record(svc::regulations::RegulationsResource::Documents, "DOC-1")
                    .await?
            );
        }
        "bhl" => {
            let s = svc::bhl::BhlService::with_services(
                conn(),
                svc::figshare::FigshareService::with_connection(conn()),
            );
            bench!(s.item(NonZeroU64::new(125582).unwrap()).await?);
        }
        "biorxiv" => {
            let s = svc::biorxiv::BiorxivService::with_connection(conn());
            bench!(
                s.jats(
                    s.details(svc::biorxiv::Server::Biorxiv, "2026-11-01", "2026-11-02", 0)
                        .await?
                )?
            );
        }
        "doab" => {
            let s = svc::doab::DoabService::with_connection(conn())?;
            bench!(s.records(None, None, None).await?);
        }
        "oapen" => {
            let s = svc::oapen::OapenService::with_connection(conn())?;
            bench!(s.records(None, None, None).await?);
        }
        "zenodo" => {
            let s = svc::zenodo::ZenodoService::with_endpoint("https://bench.invalid")?;
            bench!(s.record(NonZeroU64::new(3551612).unwrap()).await?);
        }
        "dataverse" => {
            let s = svc::dataverse::DataverseService::new("https://bench.invalid")?;
            bench!(s.dataset("doi:10.7910/DVN/A&B+", "1.0").await?);
        }
        "internet-archive" => {
            let s = svc::internet_archive::InternetArchiveService::with_endpoint(
                "https://bench.invalid",
            )?;
            bench!(s.item("sample-item").await?);
        }
        "pmc" => {
            let s = svc::pmc::PmcAccessor::with_default_decoder(super::Memory(fixture(
                "pmc",
                "metadata.json",
            )));
            bench!({
                let mut r = s.open(ReadRequest::new("metadata")).await?;
                let mut n = 0;
                while let Some(b) = r.next().await? {
                    n += b.num_rows();
                }
                assert_eq!(n, 1);
                n
            });
        }
        "gutenberg" => {
            let s = svc::gutenberg::GutenbergAccessor::with_default_decoder(super::Memory(
                fixture("gutenberg", "book.rdf"),
            ));
            bench!({
                let mut r = s.open(ReadRequest::new("book")).await?;
                let mut n = 0;
                while let Some(b) = r.next().await? {
                    n += b.num_rows();
                }
                assert_eq!(n, 1);
                n
            });
        }

        "arxiv" => {
            let s = svc::arxiv::ArxivService::new(super::Memory(fixture(
                "arxiv/pdf",
                "arXiv_pdf_manifest.xml",
            )));
            bench!(
                s.manifest(svc::arxiv::ArxivKind::Pdf, None, Default::default())
                    .await?
            );
        }
        "commoncrawl" => {
            let s = svc::warc::CommonCrawlWarcAccessor::new(
                super::Memory(fixture("commoncrawl", "good.paths")),
                open_arrow_access_file_format::FileAccessor::from_format(
                    super::Memory(Bytes::new()),
                    open_arrow_access_file_format::warc::WarcArrowFormat,
                ),
            );
            bench!(
                s.resolve(svc::warc::CommonCrawlWarcRequest::new("CC-MAIN-2022-40")?)
                    .await?
            );
        }
        "courtlistener" => {
            let s = svc::courtlistener::CourtListenerBulkAccessor::new(super::Memory(fixture(
                "courtlistener",
                "courts-2026-09-30.csv.bz2",
            )));
            bench!({
                let mut r = s
                    .open(open_arrow_access_core::ByteReadRequest::new(
                        "bulk-data/courts-2026-09-30.csv.bz2",
                    ))
                    .await?;
                let mut n = 0;
                while let Some(b) = r.next().await? {
                    n += b.len();
                }
                assert!(n > 0);
                n
            });
        }
        "pmc-inventory" => {
            let s = svc::pmc_inventory::PmcInventoryAccessor::new(Inventory {
                manifest: super::Memory(fixture("pmc/inventory", "manifest.json")),
                first: super::Memory(fixture("pmc/inventory", "first.csv.gz")),
                second: super::Memory(fixture("pmc/inventory", "second.csv.gz")),
            });
            bench!({
                let mut r = s
                    .open(ReadRequest::new(
                        "s3://pmc-oa-opendata/snapshot/inventory.json",
                    ))
                    .await?;
                let mut n = 0;
                while let Some(b) = r.next().await? {
                    n += b.num_rows();
                }
                assert!(n > 0);
                n
            });
        }
        "meca" => {
            let data = super::Memory(fixture("meca", "valid.meca"));
            bench!({
                let r = data.open("bundle.meca".into()).await?;
                let archive = svc::meca::MecaArchive::open(
                    r,
                    open_arrow_access_decompression::zip::ZipLimits::default(),
                )
                .await?;
                assert_eq!(archive.files().len(), 2);
                archive
            });
        }

        "huggingface" => {
            let s = svc::huggingface::HuggingFaceService::new(
                svc::huggingface::HuggingFaceOptions::default()
                    .with_endpoint("https://bench.invalid")
                    .with_token("benchmark-token"),
            )
            .await?;
            bench!({
                let mut r = s
                    .open(
                        svc::huggingface::HuggingFaceDatasetRequest::new(
                            "owner/example",
                            "default",
                            "train",
                        )
                        .with_revision("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                    )
                    .await?;
                let mut n = 0;
                while let Some(b) = r.next().await? {
                    n += b.num_rows();
                }
                assert!(n > 0);
                n
            });
        }

        "batch-builder" => {
            let schema = std::sync::Arc::new(arrow_schema::Schema::new(
                (0..8)
                    .map(|i| {
                        arrow_schema::Field::new(
                            format!("c{i}"),
                            arrow_schema::DataType::UInt64,
                            false,
                        )
                    })
                    .collect::<Vec<_>>(),
            ));
            let names = (0..8).map(|i| format!("c{i}")).collect::<Vec<_>>();
            bench!({
                let mut builder =
                    open_arrow_access_core::RecordBatchBuilder::new(schema.clone(), None)?;
                for value in 0..1024u64 {
                    builder.push(|row| {
                        for name in &names {
                            row.append(name, value)?;
                        }
                        Ok(())
                    })?;
                }
                let batch = builder.finish()?;
                assert_eq!(batch.num_rows(), 1024);
                batch
            });
        }
        _ => return Err(format!("unknown provider: {name}").into()),
    }
    Ok(())
}
