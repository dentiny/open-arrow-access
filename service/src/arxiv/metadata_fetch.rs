//! Lazy, shared OAI lookups for the default content metadata path.
mod rate_limit;
use std::{collections::HashMap, sync::Arc};

use futures::{TryStreamExt, lock::Mutex};
use open_arrow_access_core::{Accessor, ReadRequest, Result, into_stream};

use super::{ArxivMetadataIndex, error, id, validation};
use crate::{
    arxiv_metadata::{ArxivMetadata, single_record_format},
    common::Connection,
    oai::{OaiPage, OaiProvider, OaiService},
};

#[derive(Default)]
pub(super) struct MetadataResolver {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    connection: Option<Connection>,
    /// Maps normalized, unversioned article IDs (legacy IDs omit the slash)
    /// to shared indexes containing each article's complete typed OAI record.
    cache: HashMap<String, Arc<ArxivMetadataIndex>>,
}

impl State {
    fn connection(&mut self) -> Result<&Connection> {
        if self.connection.is_none() {
            self.connection = Some(Connection::new(ArxivMetadata::ORIGIN)?);
        }
        Ok(self.connection.as_ref().unwrap())
    }
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
        let connection = state.connection()?;
        let _permit = rate_limit::acquire(connection).await;
        let page = fetch_metadata(connection, &key).await?;
        let index = Arc::new(decode_metadata(page, &key).await?);
        state.cache.insert(key, index.clone());
        Ok(index)
    }
}

async fn fetch_metadata(connection: &Connection, article: &str) -> Result<OaiPage> {
    let service = OaiService::new(
        connection.clone(),
        ArxivMetadata::PATH,
        ArxivMetadata::METADATA_PREFIX,
    )?;
    service.get_record(&id::oai_identifier(article)?).await
}

async fn decode_metadata(page: OaiPage, article: &str) -> Result<ArxivMetadataIndex> {
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
    validation::validate_fetched_metadata(&batch, article)?;
    ArxivMetadataIndex::from_oai_records(batch)
}
