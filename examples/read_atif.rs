use open_arrow_access::{
    Accessor, ReadRequest,
    file_format::{AtifAccessor, AtifFormat},
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::current_dir()?;
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root(root.to_str().ok_or("root must be valid UTF-8")?),
    )?;
    let bytes = OpenDalAccessor::new(operator);
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .unwrap_or_else(|| "testdata/atif/trajectory.json".into());
    let format = match args.next().as_deref() {
        None | Some("json") => AtifFormat::Json,
        Some("jsonl") => AtifFormat::JsonLines,
        Some(_) => return Err("usage: read_atif [path] [json|jsonl]".into()),
    };
    let mut reader = AtifAccessor::new(bytes, format)
        .open(ReadRequest::new(path))
        .await?;
    println!("schema: {:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!(
            "{} trajectories, {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
    }
    Ok(())
}
