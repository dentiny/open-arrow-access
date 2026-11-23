//! Stage two: SQL left join and incremental Arrow output, as in BHL metadata.
use super::{
    CourtListenerSchemas,
    error::{arrow_error, db_error, task_error},
    ingest::{Database, ORDINAL},
};
use ::duckdb::params;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::{CastOptions, cast_with_options};
use open_arrow_access_core::Result;
use tokio::{sync::mpsc, task::JoinHandle};

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn query_sql(schemas: &CourtListenerSchemas) -> String {
    let entity = |i: usize, alias: &str| {
        let fields = schemas.tables()[i]
            .iter()
            .map(|f| format!("{} := {alias}.{}", quoted(f.name()), quoted(f.name())))
            .collect::<Vec<_>>()
            .join(",");
        let value = format!("struct_pack({fields})");
        if i == 0 {
            value
        } else {
            format!("CASE WHEN {alias}.id IS NULL THEN NULL ELSE {value} END")
        }
    };
    format!(
        include_str!("join.sql"),
        opinion = entity(0, "o"),
        cluster = entity(1, "c"),
        docket = entity(2, "d"),
        court = entity(3, "k"),
        ordinal = ORDINAL,
    )
}

pub(super) fn query(
    database: Database,
    schemas: &CourtListenerSchemas,
    day: String,
    uris: Vec<String>,
    batch_size: usize,
) -> QueryState {
    let schema = schemas.schema();
    let sql = query_sql(schemas);
    let (sender, receiver) = mpsc::channel(1);
    let task = tokio::task::spawn_blocking(move || {
        // Keep the scratch directory alive with its connection until query completion.
        let database = database;
        if sender.is_closed() {
            return Ok(());
        }
        let mut statement = database.connection.prepare(&sql).map_err(db_error)?;
        let _ = statement
            .stream_arrow(params![day, &uris[0], &uris[1], &uris[2], &uris[3]])
            .map_err(db_error)?;
        while !sender.is_closed() {
            let Some(batch) = statement.step().map_err(db_error)? else {
                break;
            };
            let batch = RecordBatch::from(&batch);
            let columns = batch
                .columns()
                .iter()
                .zip(schema.fields())
                .map(|(array, field)| {
                    cast_with_options(
                        array,
                        field.data_type(),
                        &CastOptions {
                            safe: false,
                            ..Default::default()
                        },
                    )
                    .map_err(arrow_error)
                })
                .collect::<Result<Vec<ArrayRef>>>()?;
            let batch = RecordBatch::try_new(schema.clone(), columns).map_err(arrow_error)?;
            for offset in (0..batch.num_rows()).step_by(batch_size) {
                if sender
                    .blocking_send(batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
        Ok(())
    });
    QueryState {
        receiver,
        task: Some(task),
    }
}

pub(super) struct QueryState {
    receiver: mpsc::Receiver<RecordBatch>,
    task: Option<JoinHandle<Result<()>>>,
}
impl QueryState {
    pub async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(batch) = self.receiver.recv().await {
            return Ok(Some(batch));
        }
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|e| task_error("courtlistener_query_task", e))??;
        }
        Ok(None)
    }
}
