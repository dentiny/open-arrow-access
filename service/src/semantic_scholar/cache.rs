//! Explicit process-local metadata sharing, scoped to one immutable service client.
use super::{DatasetMetadata, SemanticScholarService};
use crate::common::{ApiDocument, RemoteFiles, text};
use open_arrow_access_core::{Error, Result};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Semaphore};

/// Limits apply per shared cache instance. Capacity bytes count encoded JSON,
/// not total allocator usage or caller-retained document clones.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ManifestCacheOptions {
    pub max_entries: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
    /// Bounds admitted callers and live per-key refresh slots, including waiters.
    pub max_concurrent: NonZeroUsize,
    pub max_age: Duration,
    pub latest_max_age: Duration,
    pub expiry_margin: Duration,
}
impl Default for ManifestCacheOptions {
    fn default() -> Self {
        Self {
            max_entries: NonZeroUsize::new(32).unwrap(),
            max_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
            max_concurrent: NonZeroUsize::new(32).unwrap(),
            max_age: Duration::from_secs(300),
            latest_max_age: Duration::from_secs(30),
            expiry_margin: Duration::from_secs(60),
        }
    }
}
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
enum Key {
    Release(String),
    Dataset(String, String),
}
struct Document {
    value: Arc<ApiDocument>,
    expires: Option<Instant>,
    bytes: usize,
}
type SharedResult = std::result::Result<Arc<Document>, Arc<Error>>;
type Refresh = Mutex<Option<SharedResult>>;
#[derive(Default)]
struct State {
    documents: BTreeMap<Key, Arc<Document>>,
    refreshes: BTreeMap<Key, Weak<Refresh>>,
    bytes: usize,
}
struct Inner {
    service: SemanticScholarService,
    options: ManifestCacheOptions,
    state: Mutex<State>,
    callers: Semaphore,
}
/// Clones share one cache. Build another instance after changing endpoint/key;
/// there is no global cache or credential mutation API on this wrapper.
#[derive(Clone)]
pub struct CachedSemanticScholarService {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for CachedSemanticScholarService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedSemanticScholarService")
            .field("options", &self.inner.options)
            .finish_non_exhaustive()
    }
}
impl SemanticScholarService {
    /// Opt into bounded process-local metadata sharing. This does not coordinate
    /// other processes; applications may host a broker around this client.
    pub fn with_manifest_cache(
        self,
        options: ManifestCacheOptions,
    ) -> Result<CachedSemanticScholarService> {
        if options.max_age.is_zero()
            || options.latest_max_age.is_zero()
            || options.max_concurrent.get() > Semaphore::MAX_PERMITS
        {
            return Err(crate::common::invalid(
                "manifest cache requires positive TTLs and a supported concurrency limit",
            ));
        }
        Ok(CachedSemanticScholarService {
            inner: Arc::new(Inner {
                service: self,
                callers: Semaphore::new(options.max_concurrent.get()),
                options,
                state: Mutex::new(State::default()),
            }),
        })
    }
}
impl CachedSemanticScholarService {
    pub async fn release(&self, id: &str) -> Result<Arc<ApiDocument>> {
        if id.is_empty() {
            return Err(crate::common::invalid("release ID must not be empty"));
        }
        self.document(Key::Release(id.into())).await
    }
    pub async fn dataset_manifest(&self, release: &str, dataset: &str) -> Result<Arc<ApiDocument>> {
        if release.is_empty() || release == "latest" || dataset.is_empty() {
            return Err(crate::common::invalid(
                "dataset manifest requires a resolved release ID and dataset name",
            ));
        }
        self.document(Key::Dataset(release.into(), dataset.into()))
            .await
    }
    pub async fn dataset(
        &self,
        release: &str,
        dataset: &str,
    ) -> Result<RemoteFiles<DatasetMetadata>> {
        let release = self.release(release).await?;
        let release_id = text(release.value(), "release_id")?.to_owned();
        let document = self.dataset_manifest(&release_id, dataset).await?;
        self.inner.service.files_from_manifest(
            dataset,
            DatasetMetadata {
                release_id,
                release: release.as_ref().clone(),
                dataset: document.as_ref().clone(),
            },
        )
    }
    async fn document(&self, key: Key) -> Result<Arc<ApiDocument>> {
        // Admission precedes per-key allocation: neither the slot map nor active
        // refreshes can grow with an unbounded number of distinct caller keys.
        let _permit = self
            .inner
            .callers
            .acquire()
            .await
            .map_err(|_| crate::common::invalid("manifest cache is closed"))?;
        let refresh = {
            let mut state = self.inner.state.lock().await;
            prune(&mut state);
            if let Some(document) = state.documents.get(&key) {
                return Ok(document.value.clone());
            }
            state.refreshes.retain(|_, slot| slot.strong_count() != 0);
            if let Some(slot) = state.refreshes.get(&key).and_then(Weak::upgrade) {
                slot
            } else {
                let slot = Arc::new(Mutex::new(None));
                state.refreshes.insert(key.clone(), Arc::downgrade(&slot));
                slot
            }
        };
        // Only equal keys wait for this network operation. Cancellation releases
        // the lock/permit; another waiter can retry an unfinished refresh.
        let mut result = refresh.lock().await;
        if let Some(completed) = result.as_ref() {
            let fresh = completed
                .as_ref()
                .map_or(true, |doc| doc.expires.is_none_or(|e| e > Instant::now()));
            if fresh {
                return share(completed);
            }
        }
        let started = Instant::now();
        let fetched = match &key {
            Key::Release(id) => self.inner.service.release(id).await,
            Key::Dataset(release, dataset) => {
                self.inner.service.dataset_manifest(release, dataset).await
            }
        };
        let fetched = fetched
            .and_then(|document| {
                let bytes = serde_json::to_vec(document.value())
                    .map_err(|_| super::error::decode("cannot measure manifest cache document"))?
                    .len();
                let expires = deadline(
                    &key,
                    &document,
                    &self.inner.options,
                    started,
                    SystemTime::now(),
                );
                Ok(Arc::new(Document {
                    value: Arc::new(document),
                    expires,
                    bytes,
                }))
            })
            .map_err(Arc::new);
        if let Ok(document) = &fetched {
            let mut state = self.inner.state.lock().await;
            prune(&mut state);
            let options = &self.inner.options;
            if document.bytes <= options.max_bytes.get()
                && document.expires.is_some_and(|e| e > Instant::now())
            {
                while state.documents.len() >= options.max_entries.get()
                    || document.bytes > options.max_bytes.get() - state.bytes
                {
                    if let Some((_, evicted)) = state.documents.pop_first() {
                        state.bytes -= evicted.bytes;
                    } else {
                        break;
                    }
                }
                if let Some(old) = state.documents.insert(key, document.clone()) {
                    state.bytes -= old.bytes;
                }
                state.bytes += document.bytes;
            }
        }
        // Even uncacheable results/errors may be shared by already-overlapping
        // callers. Weak slots retain no result once the last such caller exits.
        *result = Some(fetched.clone());
        share(&fetched)
    }
}
fn share(result: &SharedResult) -> Result<Arc<ApiDocument>> {
    result
        .as_ref()
        .map(|document| document.value.clone())
        .map_err(|source| {
            let mut error = Error::new(
                source.kind(),
                source.operation(),
                "Semantic Scholar manifest request failed",
            )
            .with_object(source.object());
            if let Some(status) = source.http_status() {
                error = error.with_http_response(status, source.retry_after().map(str::to_owned));
            }
            error.with_source(source.clone())
        })
}
fn prune(state: &mut State) {
    let now = Instant::now();
    state
        .documents
        .retain(|_, document| document.expires.is_some_and(|expires| expires > now));
    state.bytes = state
        .documents
        .values()
        .map(|document| document.bytes)
        .sum();
}
fn deadline(
    key: &Key,
    document: &ApiDocument,
    options: &ManifestCacheOptions,
    started: Instant,
    now: SystemTime,
) -> Option<Instant> {
    let ttl = match key {
        Key::Release(id) if id == "latest" => options.latest_max_age.min(options.max_age),
        Key::Release(_) => options.max_age,
        Key::Dataset(_, _) => {
            let files = document.value().get("files")?.as_array()?;
            if files.is_empty() {
                return None;
            }
            let earliest = files
                .iter()
                .map(|file| signed_expiry(file.as_str()?))
                .collect::<Option<Vec<_>>>()?
                .into_iter()
                .min()?;
            let expires = UNIX_EPOCH.checked_add(Duration::from_secs(earliest))?;
            let remaining = expires
                .duration_since(now)
                .ok()?
                .checked_sub(options.expiry_margin)?;
            if remaining.is_zero() {
                return None;
            }
            return Some(
                started
                    .checked_add(options.max_age)?
                    .min(Instant::now().checked_add(remaining)?),
            );
        }
    };
    started.checked_add(ttl)
}
fn signed_expiry(value: &str) -> Option<u64> {
    let url = url::Url::parse(value).ok()?;
    let mut query = BTreeMap::new();
    for (key, value) in url.query_pairs() {
        if query.insert(key.into_owned(), value.into_owned()).is_some() {
            return None;
        }
    }
    if let Some(expires) = query.get("Expires") {
        if !query.contains_key("Signature")
            || !query.contains_key("AWSAccessKeyId")
            || query.contains_key("X-Amz-Date")
        {
            return None;
        }
        return expires.parse().ok();
    }
    if query.get("X-Amz-Algorithm")?.as_str() != "AWS4-HMAC-SHA256"
        || !query.contains_key("X-Amz-Signature")
        || !query.contains_key("X-Amz-Credential")
    {
        return None;
    }
    let date =
        chrono::NaiveDateTime::parse_from_str(query.get("X-Amz-Date")?, "%Y%m%dT%H%M%SZ").ok()?;
    let start = u64::try_from(date.and_utc().timestamp()).ok()?;
    start.checked_add(query.get("X-Amz-Expires")?.parse().ok()?)
}

#[cfg(test)]
mod tests;
