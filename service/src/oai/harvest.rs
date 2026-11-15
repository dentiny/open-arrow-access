//! Demand-driven OAI continuation scanning over the existing single-page protocol.
use std::{collections::HashSet, num::NonZeroUsize};

use futures::stream;
use open_arrow_access_core::{
    Accessor, OrderRequirement, ReadRequest, ReaderFactory, RecordCapability, RecordReader, Result,
    ScanPlan, ScanPlanner, StreamReader, async_trait,
};
use open_arrow_access_file_format::xml::XmlFormat;

use super::{OaiService, error, validation};

/// One independent harvest. `read` controls logical source, projection and batch size.
/// Initial selectors and a resumption token are mutually exclusive.
#[derive(Clone, Debug)]
pub struct OaiHarvestRequest {
    pub read: ReadRequest,
    pub from: Option<String>,
    pub until: Option<String>,
    pub set: Option<String>,
    pub resumption_token: Option<String>,
    /// Count fetched pages, including empty pages and the first/resumed page.
    /// Reaching this limit with a continuation returns an error, never silent EOF.
    pub max_pages: Option<NonZeroUsize>,
}

impl OaiHarvestRequest {
    pub fn new(read: ReadRequest) -> Self {
        Self {
            read,
            from: None,
            until: None,
            set: None,
            resumption_token: None,
            max_pages: None,
        }
    }

    fn validate(&self) -> Result<()> {
        if let Some(token) = &self.resumption_token {
            validation::validate_nonempty_text(token, "resumption token")?;
            if self.from.is_some() || self.until.is_some() || self.set.is_some() {
                return Err(error::invalid_harvest_selectors());
            }
        }
        Ok(())
    }
}

/// Sequential continuation accessor. Construction performs no I/O; opening fetches
/// the first page for schema binding. Later pages are fetched only after the current
/// reader reaches EOF. Each open has independent continuation state.
#[derive(Clone, Debug)]
pub struct OaiHarvester {
    service: OaiService,
    format: XmlFormat,
}

impl OaiHarvester {
    pub(super) fn new(service: OaiService, format: XmlFormat) -> Self {
        Self { service, format }
    }

    /// Prefer explicit field mappings for metadata columns across empty/variable pages.
    /// Inferred formats are allowed, but each page must produce the same schema.
    pub fn with_format(mut self, format: XmlFormat) -> Self {
        self.format = format;
        self
    }
}

#[async_trait]
impl Accessor for OaiHarvester {
    type Request = OaiHarvestRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn open(&self, request: OaiHarvestRequest) -> Result<Box<dyn RecordReader>> {
        request.validate()?;
        // Check format and explicit projection configuration before network I/O.
        let _ = self.format.prepare(&request.read)?;
        let mut seen = HashSet::new();
        let page = if let Some(token) = &request.resumption_token {
            seen.insert(token.clone());
            self.service.resume(token).await?
        } else {
            self.service
                .records(
                    request.from.as_deref(),
                    request.until.as_deref(),
                    request.set.as_deref(),
                )
                .await?
        };
        let current = page
            .records(self.format.clone())
            .open(request.read.clone())
            .await?;
        let metadata = current.metadata().clone();
        let schema = metadata.schema.clone();
        let state = (
            self.clone(),
            request,
            current,
            page.resumption_token,
            seen,
            1usize,
        );
        let batches = stream::try_unfold(
            state,
            move |(harvester, request, mut current, mut next, mut seen, mut pages)| {
                let schema = schema.clone();
                async move {
                    loop {
                        if let Some(batch) = current.next().await? {
                            return Ok(Some((
                                batch,
                                (harvester, request, current, next, seen, pages),
                            )));
                        }
                        let Some(token) = next.take() else {
                            return Ok(None);
                        };
                        if !seen.insert(token.clone()) {
                            return Err(error::repeated_harvest_token());
                        }
                        if request.max_pages.is_some_and(|limit| pages >= limit.get()) {
                            return Err(error::harvest_page_limit());
                        }
                        let page = harvester.service.resume(&token).await?;
                        pages = pages.checked_add(1).ok_or_else(error::harvest_page_limit)?;
                        current = page
                            .records(harvester.format.clone())
                            .open(request.read.clone())
                            .await?;
                        next = page.resumption_token;
                        if current.metadata().schema != schema {
                            return Err(error::harvest_schema_changed());
                        }
                    }
                }
            },
        );
        Ok(Box::new(StreamReader::records(
            batches,
            metadata,
            OrderRequirement::PreserveGlobal,
        )))
    }
}

#[async_trait]
impl ScanPlanner for OaiHarvester {
    async fn plan_scan(&self, request: OaiHarvestRequest) -> Result<ScanPlan> {
        // Token-dependent discovery stays indivisible even under ParallelReadLayer.
        Ok(ScanPlan::whole_input(self.open(request).await?))
    }
}
