//! Fixture-backed provider pipelines that do not resolve JSON HTTP manifests.
use open_arrow_access_core::{Accessor, ByteReadRequest, ReadRequest};
use open_arrow_access_service as svc;
use open_arrow_access_test_support::{MemoryAccessor, fixture};
use std::{hint::black_box, time::Instant};

pub async fn run(name: &str, iterations: usize) {
    let (dir, file) = match name {
        "pmc" => ("pmc", "metadata.json"),
        "gutenberg" => ("gutenberg", "book.rdf"),
        "courtlistener" => ("courtlistener", "courts-2026-09-30.csv.bz2"),
        "meca" => ("meca", "valid.meca"),
        "arxiv-bulk" => ("arxiv", "pdf/arXiv_pdf_manifest.xml"),
        "commoncrawl" => ("commoncrawl", "good.paths"),
        _ => unreachable!(),
    };
    let data = fixture!(dir, file).await;
    let memory = MemoryAccessor::new(&data, 65536).with_ranges();
    let start = Instant::now();
    let mut units = 0;
    for _ in 0..iterations {
        let count = match name {
            "pmc" => {
                super::drain(
                    svc::pmc::PmcAccessor::with_default_decoder(memory.clone())
                        .open(ReadRequest::new("data"))
                        .await
                        .unwrap(),
                )
                .await
            }
            "gutenberg" => {
                super::drain(
                    svc::gutenberg::GutenbergAccessor::with_default_decoder(memory.clone())
                        .open(ReadRequest::new("data"))
                        .await
                        .unwrap(),
                )
                .await
            }
            "courtlistener" => {
                let mut reader = svc::courtlistener::CourtListenerBulkAccessor::new(memory.clone())
                    .open(ByteReadRequest::new("bulk-data/courts-2026-09-30.csv.bz2"))
                    .await
                    .unwrap();
                let mut bytes = 0;
                while let Some(chunk) = reader.next().await.unwrap() {
                    bytes += chunk.len();
                    black_box(chunk);
                }
                bytes
            }
            "meca" => svc::meca::MecaArchive::open(
                memory.open(ByteReadRequest::new("data")).await.unwrap(),
                Default::default(),
            )
            .await
            .unwrap()
            .files()
            .len(),
            "arxiv-bulk" => svc::arxiv::ArxivService::new(memory.clone())
                .manifest(svc::arxiv::ArxivKind::Pdf, None, Default::default())
                .await
                .unwrap()
                .len(),
            "commoncrawl" => svc::warc::CommonCrawlWarcAccessor::new(memory.clone(), ())
                .resolve(svc::warc::CommonCrawlWarcRequest::new("CC-MAIN-2022-40").unwrap())
                .await
                .unwrap()
                .objects
                .len(),
            _ => unreachable!(),
        };
        assert!(count > 0);
        units += count;
        memory.observations.requests.lock().unwrap().clear();
        memory.observations.ranges.lock().unwrap().clear();
    }
    println!(
        "{}",
        serde_json::json!({"case":format!("service-{name}"),"iterations":iterations,"input_bytes":data.len(),"units":units,"seconds":start.elapsed().as_secs_f64(),"transport":"memory fixture"})
    );
}
