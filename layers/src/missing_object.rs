use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, Error, ErrorKind, Layer, OrderRequirement, RecordCapability, RecordMetadata,
    RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, StreamReader, async_trait,
};

use crate::scan_policy::{ScanPolicy, ScanPolicyAccessor};

/// Handling of missing source objects. Other failures always remain fatal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MissingObjectPolicy {
    #[default]
    Error,
    Skip,
}

type Reporter = dyn Fn(&Error) -> Result<()> + Send + Sync;

/// Handle missing objects at independent scan-partition boundaries.
///
/// Default: fail. Skip requires a reporter that records the object and error;
/// reporter failures fail the read. Callbacks may run concurrently and should
/// not block. The library does not accumulate an unbounded error list.
/// Planning/discovery failures remain fatal, as do failures after a partition
/// has emitted rows: skipping cannot roll back already delivered records.
/// Apply before coalescing and other transformations that merge partitions.
#[derive(Clone)]
pub struct MissingObjectLayer {
    policy: MissingObjectPolicy,
    report: Arc<Reporter>,
}

impl Default for MissingObjectLayer {
    fn default() -> Self {
        Self::new(MissingObjectPolicy::Error, |_| Ok(()))
    }
}

impl MissingObjectLayer {
    pub fn new(
        policy: MissingObjectPolicy,
        report: impl Fn(&Error) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            policy,
            report: Arc::new(report),
        }
    }

    async fn open_partition(
        self,
        partition: ScanPartition,
        metadata: RecordMetadata,
        ordering: OrderRequirement,
    ) -> Result<Box<dyn RecordReader>> {
        match partition.await {
            Ok(reader) => Ok(self.wrap_reader(reader)),
            Err(error) => {
                if self.skip(&error)? {
                    Ok(Box::new(StreamReader::records(
                        stream::empty(),
                        metadata,
                        ordering,
                    )))
                } else {
                    Err(error)
                }
            }
        }
    }

    fn wrap_reader(self, reader: Box<dyn RecordReader>) -> Box<dyn RecordReader> {
        let metadata = reader.metadata().clone();
        let ordering = reader.capability().ordering;
        let batches = stream::try_unfold((reader, false), move |(mut reader, emitted)| {
            let policy = self.clone();
            async move {
                match reader.next().await {
                    Ok(Some(batch)) => {
                        let emitted = emitted || batch.num_rows() > 0;
                        Ok(Some((batch, (reader, emitted))))
                    }
                    Ok(None) => Ok(None),
                    Err(error) => {
                        if !emitted && policy.skip(&error)? {
                            Ok(None)
                        } else {
                            Err(error)
                        }
                    }
                }
            }
        });
        Box::new(StreamReader::records(batches, metadata, ordering))
    }

    fn skip(&self, error: &Error) -> Result<bool> {
        if self.policy != MissingObjectPolicy::Skip
            || error.kind() != ErrorKind::NotFound
            || error.object().is_none()
            || has_non_404_http_status(error)
        {
            return Ok(false);
        }
        (self.report)(error).map_err(|error| {
            Error::new(
                ErrorKind::Io,
                "record_missing_object",
                "failed to record skipped object",
            )
            .with_source(error)
        })?;
        Ok(true)
    }
}

fn has_non_404_http_status(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(error);
    while let Some(current) = source {
        if let Some(error) = current.downcast_ref::<Error>()
            && let Some(status) = error.http_status()
            && status != 404
        {
            return true;
        }
        if let Some(shared) = current.downcast_ref::<Arc<std::io::Error>>() {
            source = Some(shared.as_ref());
            continue;
        }
        if let Some(error) = current.downcast_ref::<std::io::Error>()
            && let Some(inner) = error.get_ref()
        {
            source = Some(inner);
            continue;
        }
        source = current.source();
    }
    false
}

pub struct MissingObjectAccessor<A> {
    policy: ScanPolicyAccessor<A, MissingObjectLayer>,
}

impl<A: ScanPlanner> Layer<A> for MissingObjectLayer {
    type Layered = MissingObjectAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        MissingObjectAccessor {
            policy: ScanPolicyAccessor::new(inner, self.clone()),
        }
    }
}

#[async_trait]
impl<A: ScanPlanner> Accessor for MissingObjectAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.policy.capability()
    }

    async fn metadata(
        &self,
        request: Self::Request,
    ) -> Result<open_arrow_access_core::RecordMetadata> {
        self.policy.metadata(request).await
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        self.policy.open(request).await
    }
}

#[async_trait]
impl<A: ScanPlanner> ScanPlanner for MissingObjectAccessor<A> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        self.policy.plan_scan(request).await
    }
}

impl ScanPolicy for MissingObjectLayer {
    fn read_parallel(&self, inner: RecordCapability) -> bool {
        inner.read_parallel
    }

    fn apply(&self, plan: &mut ScanPlan) -> Result<()> {
        if self.policy == MissingObjectPolicy::Error {
            return Ok(());
        }
        let metadata = plan.metadata.clone();
        let ordering = plan.ordering;
        let policy = self.clone();
        let partitions = std::mem::replace(&mut plan.partitions, stream::empty().boxed());
        plan.partitions = partitions
            .map_ok(move |partition| {
                Box::pin(
                    policy
                        .clone()
                        .open_partition(partition, metadata.clone(), ordering),
                ) as ScanPartition
            })
            .boxed();
        Ok(())
    }
}
