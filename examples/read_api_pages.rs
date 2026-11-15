use std::{num::NonZeroUsize, sync::Arc};

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, AccessorExt, ReadRequest,
    file_format::json::JsonFormat,
    layers::ParallelReadLayer,
    service::common::{Connection, PageSetAccessor},
};

/// Read explicit, independently addressable pages with unsigned integer `id` rows.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: read_api_pages <origin> <relative-path> <JSON-pointer> <page> [page ...]";
    let connection = Connection::new(&args.next().ok_or(usage)?)?;
    let path = args.next().ok_or(usage)?;
    let pointer = args.next().ok_or(usage)?;
    let selection: Vec<u64> = args.map(|value| value.parse()).collect::<Result<_, _>>()?;
    if selection.is_empty() {
        return Err(usage.into());
    }
    let format = JsonFormat::new(Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::UInt64,
        false,
    )])));
    let pages = PageSetAccessor::new(move |number: u64| {
        let connection = connection.clone();
        let path = path.clone();
        let pointer = pointer.clone();
        let format = format.clone();
        async move {
            let document = connection
                .json(&path, &[("page", &number.to_string())])
                .await?;
            document
                .records_at(&pointer, format)?
                .open(ReadRequest::new(format!("page-{number}")))
                .await
        }
    });
    // Clone the shared configuration for this scan; future scans own fresh requests/readers.
    let parallel = pages.clone().layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ));
    let mut reader = parallel.open(selection).await?;
    let mut rows = 0;
    while let Some(batch) = reader.next().await? {
        rows += batch.num_rows();
    }
    println!("Read {rows} rows in requested page order");
    Ok(())
}
