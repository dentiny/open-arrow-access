//! Opt-in live smoke check: pass a bounded selection of bucket-relative OCR keys.
use std::num::NonZeroUsize;

use arrow_array::cast::AsArray;
use open_arrow_access_core::{Accessor, AccessorExt, BinaryDecoder, DocumentAccessor, Result};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::{
    FileSetAccessor, FileSetRequest,
    bhl::{BhlOpenData, open_data_bucket_operator},
};
use open_arrow_access_storage::OpenDalAccessor;

#[tokio::main]
async fn main() -> Result<()> {
    let keys = std::env::args().skip(1).collect::<Vec<_>>();
    let source = BhlOpenData::new(OpenDalAccessor::new(open_data_bucket_operator()?));
    let files = source.partition(keys.clone())?;
    let records = FileSetAccessor::new(DocumentAccessor::new(files, BinaryDecoder))
        .with_file_planning()
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ));
    let mut reader = records.open(FileSetRequest::new(keys)).await?;
    let mut rows = 0;
    let mut bytes = 0;
    while let Some(batch) = reader.next().await? {
        let data = batch["data"].as_binary::<i64>();
        rows += batch.num_rows();
        bytes += data.values().len();
    }
    println!("read {rows} independently selected page objects, {bytes} original bytes");
    Ok(())
}
