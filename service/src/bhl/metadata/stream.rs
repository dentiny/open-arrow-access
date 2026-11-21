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
// and hand rows to async partition discovery with non-blocking sends.
struct Query {
    receiver: mpsc::UnboundedReceiver<BhlPageMetadata>,
    worker: JoinHandle<Result<()>>,
    finished: bool,
}

impl Query {
    fn new(metadata: &BhlMetadata, keys: Vec<ObjectRef>) -> Result<Self> {
        let connection = metadata.query_connection()?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let worker = tokio::task::spawn_blocking(move || {
            join::for_each(&connection, &keys, |page| {
                sender
                    .send(page)
                    .map_err(|_| error::invalid("page reader dropped"))
            })
        });
        Ok(Self {
            receiver,
            worker,
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

pub(super) fn pages(
    metadata: Arc<BhlMetadata>,
    keys: Vec<ObjectRef>,
) -> BoxStream<'static, Result<BhlPageMetadata>> {
    // Planning and opening an unpolled reader do not start a SQL worker.
    stream::once(async move { Query::new(&metadata, keys) })
        .try_flatten()
        .boxed()
}
