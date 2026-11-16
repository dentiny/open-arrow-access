//! Read raw bytes and inspect separate OpenDAL I/O and reader-output metrics.
use open_arrow_access::{
    Accessor, AccessorExt, Result,
    layers::MetricsLayer,
    storage::{OpenDalAccessor, opendal, opendal_metrics_layer},
};

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).expect("usage: read_metrics <path>");
    let metrics = MetricsLayer::default();
    let operator = opendal::Operator::new(opendal::services::Fs::default().root("."))
        .expect("valid filesystem backend")
        .layer(opendal_metrics_layer(metrics.exporter().clone()));
    let accessor = OpenDalAccessor::new(operator).layer(metrics.clone());
    let mut reader = accessor.open(path.into()).await?;
    while reader.next().await?.is_some() {}
    // OpenDAL can emit final observations on drop.
    drop(reader);
    for ((name, operation), value) in metrics.exporter().snapshot() {
        println!("{name} operation={operation}: {value:?}");
    }
    Ok(())
}
