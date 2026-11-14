use open_arrow_access_core::{
    Accessor, Capability, RecordCapability, RecordReader, Result, ScanPlan, ScanPlanner,
    async_trait,
};

use crate::parallel_read::open_plan;

/// Changes scan settings without executing partitions or relaxing source ordering.
pub(crate) trait ScanPolicy: Send + Sync {
    fn read_parallel(&self, inner: RecordCapability) -> bool;
    fn apply(&self, plan: &mut ScanPlan) -> Result<()>;
}

/// Shared implementation behind the concrete public execution-policy accessors.
pub(crate) struct ScanPolicyAccessor<A, P> {
    inner: A,
    policy: P,
}

impl<A, P> ScanPolicyAccessor<A, P> {
    pub(crate) fn new(inner: A, policy: P) -> Self {
        Self { inner, policy }
    }
}

#[async_trait]
impl<A: ScanPlanner, P: ScanPolicy> Accessor for ScanPolicyAccessor<A, P> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_parallel: self.policy.read_parallel(inner),
            read_with_range: false,
            list: false,
        }
        .with_ordering(inner.ordering)
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        open_plan(self.plan_scan(request).await?)
    }
}

#[async_trait]
impl<A: ScanPlanner, P: ScanPolicy> ScanPlanner for ScanPolicyAccessor<A, P> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        let mut plan = self
            .inner
            .plan_scan(request)
            .await?
            .require_ordering(self.inner.capability().ordering);
        self.policy.apply(&mut plan)?;
        Ok(plan)
    }
}
