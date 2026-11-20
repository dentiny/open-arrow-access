//! Explicit article representations over caller-owned HTTP or mirror storage.
use super::{ArxivService, validation};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability, ObjectRef,
    OrderRequirement, ReadOptions, ReadOptionsProvider, RecordBatchBuilder, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, SchemaRef, StreamReader,
    async_trait, read_all,
};
use std::sync::{Arc, OnceLock};

/// Native article download routes, independent of the S3 bulk manifests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArxivArticleFormat {
    Pdf,
    Latex,
    Html,
}
impl ArxivArticleFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pdf => "pdf",
            Self::Latex => "latex",
            Self::Html => "html",
        }
    }
    fn prefix(self) -> &'static str {
        match self {
            Self::Pdf => "pdf",
            Self::Latex => "src",
            Self::Html => "html",
        }
    }
}

/// An explicit article/version and representation. No format fallback is implicit.
#[derive(Clone, Debug)]
pub struct ArxivArticle {
    /// Modern ID, optionally versioned, e.g. `2410.00001v2`.
    pub id: String,
    pub format: ArxivArticleFormat,
}
impl ArxivArticle {
    pub fn new(id: impl Into<String>, format: ArxivArticleFormat) -> Self {
        Self {
            id: id.into(),
            format,
        }
    }
    /// Relative website route; configure byte storage above pdf/src/html.
    pub fn locator(&self) -> Result<String> {
        let id = self.id.as_str();
        validation::validate_article_id(id)?;
        Ok(format!("{}/{}", self.format.prefix(), id))
    }
}

/// Explicit representations, each planned as one independent partition.
#[derive(Clone, Debug)]
pub struct ArxivArticleReadRequest {
    pub articles: Vec<ArxivArticle>,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}
impl ArxivArticleReadRequest {
    pub fn new(articles: Vec<ArxivArticle>) -> Self {
        Self {
            articles,
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        }
    }
}
impl ReadOptionsProvider for ArxivArticleReadRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
    fn byte_read_options_mut(&mut self) -> Option<&mut ByteReadOptions> {
        Some(&mut self.byte_options)
    }
}

/// Format-neutral artifact rows. `data` is ordinary binary, not a Lance blob marker.
pub fn article_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("article_id", DataType::LargeUtf8, false),
                Field::new("format", DataType::LargeUtf8, false),
                Field::new("source_uri", DataType::LargeUtf8, false),
                Field::new("entry_index", DataType::UInt64, false),
                Field::new("entry_name", DataType::LargeBinary, false),
                Field::new("data", DataType::LargeBinary, false),
            ]))
        })
        .clone()
}

pub struct ArxivArticles<A> {
    storage: Arc<A>,
}
impl<A> ArxivService<A> {
    /// Read explicit website routes (or mirrors preserving those routes).
    /// This is not S3 manifest discovery; HTML has no implied bulk manifest.
    pub fn article_records(self) -> ArxivArticles<A> {
        ArxivArticles {
            storage: Arc::new(self.storage),
        }
    }
}

#[async_trait]
impl<A> Accessor for ArxivArticles<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    type Request = ArxivArticleReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        Capability {
            read: self.storage.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }
    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}
#[async_trait]
impl<A> ScanPlanner for ArxivArticles<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        let schema = request
            .options
            .resolve_projection(&article_schema())?
            .schema;
        // Validate every selection before storage I/O, including later partitions.
        let selections = request
            .articles
            .iter()
            .map(|a| a.locator())
            .collect::<Result<Vec<_>>>()?;
        let metadata = RecordMetadata {
            schema: schema.clone(),
            source: None,
        };
        let storage = self.storage.clone();
        let partitions =
            request
                .articles
                .into_iter()
                .zip(selections)
                .map(move |(article, locator)| {
                    let storage = storage.clone();
                    let options = request.options.clone();
                    let byte_options = request.byte_options;
                    let schema = schema.clone();
                    open_arrow_access_core::scan_partition(async move {
                        let source = ObjectRef::new(locator.clone());
                        let output = RecordMetadata {
                            schema,
                            source: Some(source),
                        };
                        let batches = stream::once(async move {
                            let input = storage
                                .open(
                                    ByteReadRequest::new(locator.clone())
                                        .with_options(byte_options),
                                )
                                .await?;
                            let data = read_all(input, "read_arxiv_article").await?;
                            let mut builder =
                                RecordBatchBuilder::new(article_schema(), options.projection)?;
                            builder.push(|row| {
                                row.append("article_id", article.id.as_str())?;
                                row.append("format", article.format.as_str())?;
                                row.append("source_uri", locator.as_str())?;
                                row.append("entry_index", 0_u64)?;
                                row.append("entry_name", b"".as_slice())?;
                                row.append("data", data.as_slice())
                            })?;
                            builder.finish()
                        });
                        Ok(StreamReader::records(
                            batches,
                            output,
                            OrderRequirement::Reorderable,
                        ))
                    })
                });
        Ok(ScanPlan::from_iter(metadata, partitions).with_ordering(OrderRequirement::Reorderable))
    }
}
