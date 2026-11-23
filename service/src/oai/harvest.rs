//! Demand-driven OAI continuation scanning over the existing single-page protocol.
use std::num::NonZeroUsize;

use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, OrderRequirement, ReadRequest, ReaderFactory, RecordCapability, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, async_trait,
};
use open_arrow_access_file_format::xml::XmlFormat;

use super::{OaiPage, OaiService, error};
use crate::pagination::{Page, PageSource, Pager, PaginationErrors};

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
        if request.resumption_token.is_some()
            && (request.from.is_some() || request.until.is_some() || request.set.is_some())
        {
            return Err(error::invalid_harvest_selectors());
        }
        // Check format and explicit projection configuration before network I/O.
        let _ = self.format.prepare(&request.read)?;
        let page = if let Some(token) = &request.resumption_token {
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
        let pager = Pager::new(
            self.service.clone(),
            page.resumption_token,
            request.resumption_token.clone(),
            request.max_pages,
            PaginationErrors {
                repeated_cursor: error::repeated_harvest_token,
                page_limit: error::harvest_page_limit,
            },
        );
        let harvester = self.clone();
        let continuations = stream::try_unfold(pager, move |mut pager| {
            let harvester = harvester.clone();
            let request = request.clone();
            async move {
                let Some(page) = pager.next().await? else {
                    return Ok(None);
                };
                let reader = page.records(harvester.format).open(request.read).await?;
                let partition: ScanPartition = Box::pin(async move { Ok(reader) });
                Ok(Some((partition, pager)))
            }
        });
        let first: ScanPartition = Box::pin(async move { Ok(current) });
        // Sequential execution drains each page before discovering its continuation.
        let partitions = stream::iter([Ok(first)]).chain(continuations);
        Ok(Box::new(
            ScanPlan::from_stream(metadata, partitions)
                .into_reader_with_schema_error(error::harvest_schema_changed),
        ))
    }
}

#[async_trait]
impl ScanPlanner for OaiHarvester {
    async fn plan_scan(&self, request: OaiHarvestRequest) -> Result<ScanPlan> {
        // Token-dependent discovery stays indivisible even under ParallelReadLayer.
        Ok(ScanPlan::whole_input(self.open(request).await?))
    }
}

#[async_trait]
impl PageSource for OaiService {
    type Cursor = String;
    type Body = OaiPage;

    async fn resume_page(&self, cursor: String) -> Result<Page<OaiPage, String>> {
        let page = self.resume(&cursor).await?;
        Ok(Page {
            next: page.resumption_token.clone(),
            body: page,
        })
    }
}
