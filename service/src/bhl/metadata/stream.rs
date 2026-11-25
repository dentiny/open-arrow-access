use super::{BhlMetadata, BhlPageSelection, error};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{ObjectRef, Result};
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};

pub(super) fn pages(
    metadata: Arc<BhlMetadata>,
    keys: Vec<ObjectRef>,
) -> BoxStream<'static, Result<BhlPageSelection>> {
    // Start only when discovery is polled. DuckDB must never run on the async
    // executor, and slow OCR consumers must not accumulate all joined rows.
    stream::once(async move {
        Ok(stream::try_unfold(
            Query::start(move |emit| metadata.for_each_selection(&keys, emit)),
            |mut query| async move { Ok(query.next().await?.map(|page| (page, query))) },
        ))
    })
    .try_flatten()
    .boxed()
}

struct Query {
    receiver: mpsc::Receiver<BhlPageSelection>,
    task: Option<JoinHandle<Result<()>>>,
}

impl Query {
    fn start(
        run: impl FnOnce(&mut dyn FnMut(BhlPageSelection) -> Result<()>) -> Result<()> + Send + 'static,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let task = tokio::task::spawn_blocking(move || {
            if sender.is_closed() {
                return Ok(());
            }
            run(&mut |page| {
                sender
                    .blocking_send(page)
                    .map_err(|_| error::invalid("page reader dropped"))
            })
        });
        Self {
            receiver,
            task: Some(task),
        }
    }

    async fn next(&mut self) -> Result<Option<BhlPageSelection>> {
        if let Some(page) = self.receiver.recv().await {
            return Ok(Some(page));
        }
        if let Some(task) = self.task.take() {
            task.await.map_err(error::task)??;
        }
        Ok(None)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/bhl_metadata_stream.rs"]
mod tests;
