use open_arrow_access_core::{
    Accessor, Capability, EmissionPolicy, Layer, RecordCapability, RecordReader, Result, ScanPlan,
    ScanPlanner, async_trait,
};

use crate::parallel_read::open_plan;

/// Opt into ready-batch emission when the entire source permits reordering.
/// Does not add partition workers; combine with `ParallelReadLayer` for concurrency.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnorderedEmissionLayer;

impl UnorderedEmissionLayer {
    pub fn new() -> Self {
        Self
    }
}

pub struct UnorderedEmissionAccessor<A> {
    inner: A,
}

impl<A: ScanPlanner> Layer<A> for UnorderedEmissionLayer {
    type Layered = UnorderedEmissionAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        UnorderedEmissionAccessor { inner }
    }
}

#[async_trait]
impl<A: ScanPlanner> Accessor for UnorderedEmissionAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_parallel: inner.read_parallel,
            read_with_range: false,
            list: false,
        }
        .with_ordering(self.inner.capability().ordering)
    }

    async fn open_for_schema(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        let plan = self.plan_scan(request).await?;
        crate::validation::validate_emission_policy(plan.emission, plan.ordering)?;
        Ok(Box::new(plan.into_reader()))
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        open_plan(self.plan_scan(request).await?)
    }
}

#[async_trait]
impl<A: ScanPlanner> ScanPlanner for UnorderedEmissionAccessor<A> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        let mut plan = self
            .inner
            .plan_scan(request)
            .await?
            .require_ordering(self.inner.capability().ordering);
        plan.emission = EmissionPolicy::Unordered;
        Ok(plan)
    }
}
