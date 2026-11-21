use super::{BhlMetadata, BhlPageMetadata, error, join};
use futures::{
    Future, Stream, StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{ObjectRef, Result};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{sync::mpsc, task::JoinHandle};

// DuckDB's cursor borrows its connection. Keep both on the same blocking worker
// and hand rows to async partition discovery without collecting the query result.
struct Query {
    receiver: mpsc::Receiver<BhlPageMetadata>,
    worker: JoinHandle<Result<()>>,
    interrupt: Arc<duckdb::InterruptHandle>,
    finished: bool,
}

impl Query {
    fn new(metadata: &BhlMetadata, keys: Vec<ObjectRef>) -> Result<Self> {
        let connection = metadata.query_connection()?;
        let interrupt = connection.interrupt_handle();
        let (sender, receiver) = mpsc::channel(1);
        let worker = tokio::task::spawn_blocking(move || {
            join::for_each(
                &connection,
                &keys,
                |page| {
                    sender
                        .blocking_send(page)
                        .map_err(|_| error::invalid("page reader dropped"))
                },
                || sender.is_closed(),
            )
        });
        Ok(Self {
            receiver,
            worker,
            interrupt,
            finished: false,
        })
    }
}

impl Stream for Query {
    type Item = Result<BhlPageMetadata>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(page)) => Poll::Ready(Some(Ok(page))),
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                let result = match Pin::new(&mut self.worker).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result.map_err(error::task).and_then(|result| result),
                };
                self.finished = true;
                Poll::Ready(result.err().map(Err))
            }
        }
    }
}

impl Drop for Query {
    fn drop(&mut self) {
        self.receiver.close(); // Wake a producer waiting to send its next row.
        if !self.finished {
            self.interrupt.interrupt(); // Stop a query still executing in DuckDB.
            self.worker.abort(); // Also cancel a worker that has not started yet.
        }
    }
}

pub(super) fn pages(
    metadata: Arc<BhlMetadata>,
    keys: Vec<ObjectRef>,
) -> BoxStream<'static, Result<BhlPageMetadata>> {
    // Planning and opening an unpolled reader do not start a SQL worker.
    stream::once(async move { Query::new(&metadata, keys) })
        .try_flatten()
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dropping_query_stops_its_blocking_producer() {
        let metadata = super::super::tests::load().await;
        let keys = vec![ObjectRef::new("ocr/item-2/item-2-3-5.txt"); 20];
        let mut query = Query::new(&metadata, keys).unwrap();
        assert!(query.next().await.unwrap().is_ok());
        let worker = query.worker.abort_handle();
        drop(query);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !worker.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn query_failures_reach_the_async_consumer() {
        let metadata = Arc::new(super::super::tests::load().await);
        let mut rows = pages(
            metadata.clone(),
            vec![ObjectRef::new("ocr/item-2/item-2-3-5.txt")],
        );
        // Creating the stream has not started SQL yet.
        metadata
            .connection
            .lock()
            .unwrap()
            .execute_batch("DROP TABLE page;")
            .unwrap();
        assert!(rows.next().await.unwrap().is_err());
        assert!(rows.next().await.is_none());
    }
}
