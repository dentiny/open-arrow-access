use crate::{Accessor, RecordReader, Result, ScanPlan, ScanPlanner, SourceColumn, async_trait};
use std::sync::Arc;

/// Optional parser extension, keeping provenance out of ordinary request structs.
#[async_trait]
pub trait SourceColumnAccessor: Accessor<Reader = dyn RecordReader> {
    async fn open_source_columns(
        &self,
        request: Self::Request,
        columns: &[SourceColumn],
    ) -> Result<Box<dyn RecordReader>>;
}
#[async_trait]
pub trait SourceColumnPlanner: SourceColumnAccessor + ScanPlanner {
    async fn plan_source_columns(
        &self,
        request: Self::Request,
        columns: &[SourceColumn],
    ) -> Result<ScanPlan>;
}
#[async_trait]
impl<A: SourceColumnAccessor + ?Sized> SourceColumnAccessor for Arc<A> {
    async fn open_source_columns(
        &self,
        request: Self::Request,
        columns: &[SourceColumn],
    ) -> Result<Box<dyn RecordReader>> {
        (**self).open_source_columns(request, columns).await
    }
}
#[async_trait]
impl<A: SourceColumnPlanner + ?Sized> SourceColumnPlanner for Arc<A> {
    async fn plan_source_columns(
        &self,
        request: Self::Request,
        columns: &[SourceColumn],
    ) -> Result<ScanPlan> {
        (**self).plan_source_columns(request, columns).await
    }
}
/// Bind source columns while retaining the provider's existing request type.
/// Used by datasets to forward the same selection to independently opened files.
pub struct SourceColumnRequestAccessor<A> {
    inner: A,
    columns: Vec<SourceColumn>,
}
impl<A> SourceColumnRequestAccessor<A> {
    pub fn new(inner: A, columns: Vec<SourceColumn>) -> Self {
        Self { inner, columns }
    }
}
#[async_trait]
impl<A: SourceColumnAccessor> Accessor for SourceColumnRequestAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;
    fn capability(&self) -> crate::RecordCapability {
        self.inner.capability()
    }
    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        self.inner.open_source_columns(request, &self.columns).await
    }
}
#[async_trait]
impl<A: SourceColumnPlanner> ScanPlanner for SourceColumnRequestAccessor<A> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        self.inner.plan_source_columns(request, &self.columns).await
    }
}
