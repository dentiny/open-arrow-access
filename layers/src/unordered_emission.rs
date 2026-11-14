use open_arrow_access_core::{
    Accessor, EmissionPolicy, Layer, RecordCapability, RecordReader, Result, ScanPlan, ScanPlanner,
    async_trait,
};

use crate::scan_policy::{ScanPolicy, ScanPolicyAccessor};

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
    policy: ScanPolicyAccessor<A, UnorderedEmissionLayer>,
}

impl<A: ScanPlanner> Layer<A> for UnorderedEmissionLayer {
    type Layered = UnorderedEmissionAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        UnorderedEmissionAccessor {
            policy: ScanPolicyAccessor::new(inner, *self),
        }
    }
}

#[async_trait]
impl<A: ScanPlanner> Accessor for UnorderedEmissionAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.policy.capability()
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        self.policy.open(request).await
    }
}

#[async_trait]
impl<A: ScanPlanner> ScanPlanner for UnorderedEmissionAccessor<A> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        self.policy.plan_scan(request).await
    }
}

impl ScanPolicy for UnorderedEmissionLayer {
    fn read_parallel(&self, inner: RecordCapability) -> bool {
        inner.read_parallel
    }

    fn apply(&self, plan: &mut ScanPlan) -> Result<()> {
        plan.emission = EmissionPolicy::Unordered;
        Ok(())
    }
}
