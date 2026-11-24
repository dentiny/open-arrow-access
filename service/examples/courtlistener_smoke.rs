//! Audit complete local exports or explicitly bounded, record-aligned samples.
//! Usage: courtlistener_smoke CSV_ROOT DATE [DATABASE_PATH] [MEDIA_ROOT]
//! An existing database is reused; no document URLs are opened.

use open_arrow_access_core::{Accessor, ReadRequest};
use open_arrow_access_service::courtlistener::{
    CourtListenerDatabaseAccessor, CourtListenerDatePartitionAccessor, CourtListenerTableAccessor,
};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::fs_operator;
use serde_json::json;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    time::Instant,
};

fn fingerprint(array: &arrow_array::StructArray, hash: &mut DefaultHasher) {
    use arrow_array::{Array, StringArray};
    for row in 0..array.len() {
        for column in array.columns() {
            column.is_null(row).hash(hash);
            if !column.is_null(row) {
                if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
                    strings.value(row).hash(hash);
                } else {
                    arrow_cast::display::array_value_to_string(column.as_ref(), row)
                        .unwrap()
                        .hash(hash);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 3 {
        eprintln!("usage: courtlistener_smoke CSV_ROOT DATE [DATABASE_PATH] [MEDIA_ROOT]");
        std::process::exit(2);
    }
    let root = &args[1];
    let date = args[2].as_str();
    let media_root = args
        .get(4)
        .map(String::as_str)
        .unwrap_or("https://media.invalid/");
    let source = CourtListenerTableAccessor::new(OpenDalAccessor::new(fs_operator(root)));
    let mut valid = true;
    let mut had_error = false;
    let mut expected_fingerprint = None;
    for table in ["opinions", "opinion-clusters", "dockets", "courts"] {
        let start = Instant::now();
        let result = async {
            let mut reader = source
                .open(ReadRequest::new(format!(
                    "bulk-data/{table}-{date}.csv.bz2"
                )))
                .await?;
            let mut rows = 0;
            let mut arrow_bytes = 0;
            let mut hash = DefaultHasher::new();
            while let Some(batch) = reader.next().await? {
                if table == "opinions" {
                    fingerprint(&arrow_array::StructArray::from(batch.clone()), &mut hash);
                }
                rows += batch.num_rows();
                arrow_bytes += batch.get_array_memory_size();
            }
            Ok::<_, open_arrow_access_core::Error>((rows, arrow_bytes, hash.finish()))
        }
        .await;
        match result {
            Ok((rows, arrow_bytes, hash)) => {
                if table == "opinions" {
                    expected_fingerprint = Some(hash);
                }
                println!(
                    "{}",
                    json!({"date":date,"table":table,"rows":rows,"arrow_bytes":arrow_bytes,"seconds":start.elapsed().as_secs_f64(),"ok":true,"fingerprint":hash})
                );
            }
            Err(error) => {
                valid = false;
                had_error = true;
                println!(
                    "{}",
                    json!({"date":date,"table":table,"seconds":start.elapsed().as_secs_f64(),"ok":false,"error":format!("{error:?}")})
                );
            }
        }
    }
    if valid {
        let start = Instant::now();
        let result = async {
            let source = CourtListenerDatePartitionAccessor::new(
                OpenDalAccessor::new(fs_operator(root)),
                format!("file://{root}"),
            )?
            .with_document_urls(media_root)?;
            let mut reader = source.open(ReadRequest::new(date)).await?;
            let open_seconds = start.elapsed().as_secs_f64();
            let mut rows = 0;
            while let Some(batch) = reader.next().await? {
                rows += batch.num_rows();
            }
            Ok::<_, open_arrow_access_core::Error>((rows, open_seconds))
        }
        .await;
        match result {
            Ok((rows, open_seconds)) => println!(
                "{}",
                json!({"date":date,"table":"joined","rows":rows,"open_seconds":open_seconds,"seconds":start.elapsed().as_secs_f64(),"ok":true})
            ),
            Err(error) => {
                had_error = true;
                println!(
                    "{}",
                    json!({"date":date,"table":"joined","seconds":start.elapsed().as_secs_f64(),"ok":false,"error":format!("{error:?}")})
                );
            }
        }
    }
    if valid && let Some(destination) = args.get(3) {
        let result = async {
            let source = CourtListenerDatePartitionAccessor::new(
                OpenDalAccessor::new(fs_operator(root)), format!("file://{root}"),
            )?;
            let start = Instant::now();
            let reused_existing = std::path::Path::new(destination).exists();
            let database = if reused_existing {
                CourtListenerDatabaseAccessor::from_path(destination).await?
            } else { source.create_database(date, destination).await? };
            println!("{}", json!({"date":date,"table":"database_build","ok":true,
                "seconds":start.elapsed().as_secs_f64(),"path":database.path(),
                "reused_existing":reused_existing,
                "row_counts":database.info().row_counts,
                "database_bytes":std::fs::metadata(destination).unwrap().len()}));
            drop(database);
            for iteration in 0..3 {
                let start = Instant::now();
                let database = CourtListenerDatabaseAccessor::from_path(destination).await?;
                let connect_seconds = start.elapsed().as_secs_f64();
                let source = database.with_document_urls(media_root)?;
                let mut reader = source.open(ReadRequest::new(date)).await?;
                let open_seconds = start.elapsed().as_secs_f64();
                let mut first_seconds = None;
                let mut rows = 0;
                let mut missing_cluster = 0;
                let mut missing_docket = 0;
                let mut missing_court = 0;
                let mut text_rows = 0;
                let mut xml_rows = 0;
                let mut pdf_rows = 0;
                let mut hash = DefaultHasher::new();
                while let Some(batch) = reader.next().await? {
                    first_seconds.get_or_insert_with(|| start.elapsed().as_secs_f64());
                    use arrow_array::{Array, StringArray, StructArray};
                    let opinion = batch.column(1).as_any().downcast_ref::<StructArray>().unwrap();
                    fingerprint(opinion, &mut hash);
                    let text = opinion.column_by_name("plain_text").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                    let xml1 = opinion.column_by_name("xml_harvard").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                    let xml2 = opinion.column_by_name("xml_scan").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                    let docs = batch.column(6).as_any().downcast_ref::<StructArray>().unwrap();
                    let pdf = docs.column_by_name("cluster_pdf_url").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                    for i in 0..batch.num_rows() {
                        text_rows += usize::from(!text.value(i).is_empty());
                        xml_rows += usize::from(!xml1.value(i).is_empty() || !xml2.value(i).is_empty());
                        pdf_rows += usize::from(!pdf.is_null(i));
                    }
                    missing_cluster += batch.column(2).null_count();
                    missing_docket += batch.column(3).null_count();
                    missing_court += batch.column(4).null_count();
                    rows += batch.num_rows();
                }
                let actual_fingerprint = hash.finish();
                if Some(actual_fingerprint) != expected_fingerprint {
                    return Err(open_arrow_access_core::Error::new(open_arrow_access_core::ErrorKind::Decode,
                        "smoke_fingerprint", "persisted opinion fields differ from the source"));
                }
                println!("{}", json!({"date":date,"table":"database_reuse","iteration":iteration,
                    "ok":true,"seconds":start.elapsed().as_secs_f64(),"rows":rows,
                    "connect_seconds":connect_seconds,"open_seconds":open_seconds,"first_seconds":first_seconds,
                    "missing_cluster":missing_cluster,"missing_docket":missing_docket,"missing_court":missing_court,
                    "text_rows":text_rows,"xml_rows":xml_rows,"cluster_pdf_url_rows":pdf_rows,"fingerprint":actual_fingerprint}));
            }
            Ok::<_,open_arrow_access_core::Error>(())
        }.await;
        if let Err(error) = result {
            had_error = true;
            println!(
                "{}",
                json!({"date":date,"table":"persistent","ok":false,"error":format!("{error:?}")})
            );
        }
    }
    if had_error {
        std::process::exit(1);
    }
}
