//! Read lossless CSV bytes from a CourtListener bulk export in a local mirror.
mod common;

use open_arrow_access::{
    Accessor,
    service::courtlistener::{CourtListenerBulkAccessor, CourtListenerSnapshot},
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
    let storage = common::fs_accessor(&root)?;
    let service = CourtListenerBulkAccessor::new(storage);
    let Some(date) = date else {
        for snapshot in service.snapshots(&table).await? {
            println!("{}", snapshot.object().locator());
        }
        return Ok(());
    };
    let snapshot = CourtListenerSnapshot::new(table, date)?;
    let mut reader = service.open(snapshot.object().into()).await?;
    let total = common::count_bytes(reader.as_mut()).await?;
    println!("{}: {total} decoded CSV bytes", snapshot.object().locator());
    Ok(())
}
