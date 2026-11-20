//! Lazy, shared OAI lookups for the default content metadata path.
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, OnceLock},
    time::Duration,
};

use futures::{
    TryStreamExt,
    lock::{Mutex, MutexGuard},
};
use open_arrow_access_core::{Accessor, ReadRequest, Result, into_stream};
use open_arrow_access_storage::{RetryConfig, StorageConfig, TimeoutConfig};
use tokio::time::{Instant, sleep_until};

use super::{ArxivMetadataIndex, error, id, validation};
use crate::{
    arxiv_metadata::{ArxivMetadata, single_record_format},
    common::Connection,
    oai::{OaiProvider, OaiService},
};

const REQUEST_INTERVAL: Duration = Duration::from_secs(3);
const CACHE_SIZE: usize = 1024;

// Preserve the gap after the final attempt, including retries and cancellation.
struct RequestPermit<'a>(MutexGuard<'a, Option<Instant>>);
impl Drop for RequestPermit<'_> {
    fn drop(&mut self) {
        *self.0 = Some(Instant::now() + REQUEST_INTERVAL);
    }
}

#[derive(Default)]
pub(super) struct MetadataResolver {
    state: Mutex<State>,
    gate: Mutex<Option<Instant>>,
}

#[derive(Default)]
struct State {
    connection: Option<Connection>,
    cache: HashMap<String, Arc<ArxivMetadataIndex>>,
    order: VecDeque<String>,
}

impl std::fmt::Debug for MetadataResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataResolver").finish_non_exhaustive()
    }
}

impl MetadataResolver {
    pub(super) fn new(connection: Connection) -> Self {
        Self {
            state: Mutex::new(State {
                connection: Some(connection),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub(super) async fn get(&self, article: &str) -> Result<Arc<ArxivMetadataIndex>> {
        let key = id::metadata_id(article)?;
        // Hold the lock through decoding: concurrent requests for the same ID
        // share one successful lookup, including all TeX members of an article.
        let mut state = self.state.lock().await;
        if let Some(index) = state.cache.get(&key) {
            return Ok(index.clone());
        }
        if state.connection.is_none() {
            state.connection = Some(Connection::new(ArxivMetadata::ORIGIN)?.with_http_config(
                StorageConfig {
                    retry: Some(RetryConfig {
                        min_delay: REQUEST_INTERVAL,
                        jitter: false,
                        ..Default::default()
                    }),
                    timeout: Some(TimeoutConfig::default()),
                },
            )?);
        }
        let connection = state.connection.as_ref().unwrap();
        let service = OaiService::new(
            connection.clone(),
            ArxivMetadata::PATH,
            ArxivMetadata::METADATA_PREFIX,
        )?;
        // Public arXiv requests share one process-wide gate across services.
        // Mirrors/custom endpoints have a separate gate per resolver.
        static PUBLIC_GATE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
        let gate = if connection.origin().host_str() == Some("oaipmh.arxiv.org") {
            PUBLIC_GATE.get_or_init(Mutex::default)
        } else {
            &self.gate
        };
        let page = {
            let next = gate.lock().await;
            if let Some(deadline) = *next {
                sleep_until(deadline).await;
            }
            let _permit = RequestPermit(next);
            service.get_record(&id::oai_identifier(&key)?).await?
        };
        validation::validate_metadata_record_count(page.record_count)?;
        let reader = page
            .records(single_record_format())
            .open(ReadRequest::new("metadata"))
            .await?;
        let batches: Vec<_> = into_stream(reader).try_collect().await?;
        let batch = arrow_select::concat::concat_batches(
            &crate::arxiv_metadata::schema().arrow_schema(),
            &batches,
        )
        .map_err(error::arrow_error)?;
        validation::validate_fetched_metadata(&batch, &key)?;
        let index = Arc::new(ArxivMetadataIndex::from_oai_records(batch)?);
        if state.cache.len() == CACHE_SIZE {
            let oldest = state.order.pop_front().unwrap();
            state.cache.remove(&oldest);
        }
        state.order.push_back(key.clone());
        state.cache.insert(key, index.clone());
        Ok(index)
    }
}
