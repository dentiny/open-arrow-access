//! Frozen AWS OCR membership and count-bounded, self-contained worker jobs.
mod validation;

use super::{BhlOpenData, BhlPageIdentity};
use crate::FileSetRequest;
use open_arrow_access_core::{ByteAccessor, ListRequest, Result};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

const VERSION: u32 = 1;

/// Explicit namespace ownership. `All` preserves both representations of a page;
/// it does not deduplicate item and part OCR using PageID alone.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BhlOcrScope {
    Items,
    Parts,
    All,
}

impl BhlOcrScope {
    fn accepts(self, identity: &BhlPageIdentity) -> bool {
        match self {
            Self::Items => identity.entity_type == "item",
            Self::Parts => identity.entity_type == "part",
            Self::All => true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InventoryDocument {
    version: u32,
    snapshot_id: String,
    scope: BhlOcrScope,
    objects: Vec<String>,
}

/// Sorted object membership captured by a coordinator. The caller's snapshot ID
/// identifies this inventory, not an immutable S3 object-version snapshot.
/// Deserialization validates the saved membership without listing source objects.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "InventoryDocument")]
pub struct BhlInventory(InventoryDocument);

impl TryFrom<InventoryDocument> for BhlInventory {
    type Error = open_arrow_access_core::Error;

    fn try_from(document: InventoryDocument) -> Result<Self> {
        validation::header(document.version, &document.snapshot_id)?;
        validation::objects(document.scope, &document.objects)?;
        Ok(Self(document))
    }
}

impl BhlInventory {
    /// Freeze explicitly selected OCR keys. Reject duplicates, including keys
    /// whose different zero padding resolves to the same logical identity.
    pub fn new(
        snapshot_id: impl Into<String>,
        scope: BhlOcrScope,
        objects: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let mut objects = objects.into_iter().map(Into::into).collect::<Vec<_>>();
        objects.sort();
        InventoryDocument {
            version: VERSION,
            snapshot_id: snapshot_id.into(),
            scope,
            objects,
        }
        .try_into()
    }

    pub fn snapshot_id(&self) -> &str {
        &self.0.snapshot_id
    }

    pub fn scope(&self) -> BhlOcrScope {
        self.0.scope
    }

    pub fn objects(&self) -> &[String] {
        &self.0.objects
    }

    /// Generate disjoint chunks lazily in canonical key order. Persist each job
    /// independently; completed jobs can be skipped by snapshot/sizing/index.
    pub fn jobs(&self, pages_per_job: NonZeroUsize) -> impl Iterator<Item = BhlPageJob> + '_ {
        self.0
            .objects
            .chunks(pages_per_job.get())
            .enumerate()
            .map(move |(index, objects)| {
                BhlPageJob(JobDocument {
                    version: VERSION,
                    snapshot_id: self.0.snapshot_id.clone(),
                    scope: self.0.scope,
                    inventory_pages: self.0.objects.len(),
                    pages_per_job: pages_per_job.get(),
                    index,
                    objects: objects.to_vec(),
                })
            })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct JobDocument {
    version: u32,
    snapshot_id: String,
    scope: BhlOcrScope,
    inventory_pages: usize,
    pages_per_job: usize,
    index: usize,
    objects: Vec<String>,
}

/// One independently persisted worker selection. Its logical identity is the
/// tuple (snapshot_id, pages_per_job, index). Keep sizing fixed within a run.
/// Deserialization validates its header, object keys and chunk sizing.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "JobDocument")]
pub struct BhlPageJob(JobDocument);

impl TryFrom<JobDocument> for BhlPageJob {
    type Error = open_arrow_access_core::Error;

    fn try_from(document: JobDocument) -> Result<Self> {
        validation::header(document.version, &document.snapshot_id)?;
        validation::objects(document.scope, &document.objects)?;
        validation::job(&document)?;
        Ok(Self(document))
    }
}

impl BhlPageJob {
    pub fn snapshot_id(&self) -> &str {
        &self.0.snapshot_id
    }

    pub fn scope(&self) -> BhlOcrScope {
        self.0.scope
    }

    pub fn index(&self) -> usize {
        self.0.index
    }

    pub fn pages_per_job(&self) -> usize {
        self.0.pages_per_job
    }

    pub fn objects(&self) -> &[String] {
        &self.0.objects
    }

    /// Pass to BhlPageAccessor (configured with this job's snapshot ID) and the
    /// shared parallel layer. No source listing or download occurs here.
    pub fn request(&self) -> FileSetRequest {
        FileSetRequest::new(self.0.objects.clone())
    }
}

impl<A: ByteAccessor> BhlOpenData<A> {
    /// List the complete OCR prefix once, then freeze the selected namespace.
    /// No object bodies or per-page stat/HEAD requests are made. Listing is live,
    /// not a point-in-time S3 snapshot; inventory and validation retain O(keys).
    pub async fn inventory(
        &self,
        snapshot_id: impl Into<String>,
        scope: BhlOcrScope,
    ) -> Result<BhlInventory> {
        let snapshot_id = snapshot_id.into();
        validation::header(VERSION, &snapshot_id)?;
        let mut objects = Vec::new();
        for object in self.discover(ListRequest::new("ocr/")).await? {
            let identity = BhlPageIdentity::parse(object.locator())?;
            if scope.accepts(&identity) {
                objects.push(object.locator().to_owned());
            }
        }
        BhlInventory::new(snapshot_id, scope, objects)
    }
}
