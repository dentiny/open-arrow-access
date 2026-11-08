//! Read lossless CSV bytes from a CourtListener bulk export in a local mirror.
use open_arrow_access::{
    Accessor,
    service::courtlistener::{CourtListenerBulkAccessor, CourtListenerSnapshot},
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: read_courtlistener <mirror-root> <table> [date]")?;
    let table = args
        .next()
        .ok_or("expected a bulk table name, such as courts")?;
    let date = args.next();
    let storage = OpenDalAccessor::new(opendal::Operator::new(
        opendal::services::Fs::default().root(&root),
    )?);
    let service = CourtListenerBulkAccessor::new(storage);
    let Some(date) = date else {
        for snapshot in service.snapshots(&table).await? {
            println!("{}", snapshot.object().locator());
        }
        return Ok(());
    };
    let snapshot = CourtListenerSnapshot::new(table, date)?;
    let mut reader = service.open(snapshot.object().into()).await?;
    let mut total = 0u64;
    while let Some(bytes) = reader.next().await? {
        total += bytes.len() as u64;
    }
    println!("{}: {total} decoded CSV bytes", snapshot.object().locator());
    Ok(())
}
