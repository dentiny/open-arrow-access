use std::sync::Arc;

use futures::{TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest,
    ReaderFactory, RecordCapability, RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner,
    Schema, StreamReader, async_trait,
};

use crate::{FileAccessor, scan::FileScanPlanner};

struct NoStorage;

#[async_trait]
impl Accessor for NoStorage {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, _: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        panic!("ordering composition must not open storage")
    }
}

struct PlanFormat {
    accessor_ordering: OrderRequirement,
    child_ordering: OrderRequirement,
    whole_input: bool,
}

#[async_trait]
impl ReaderFactory for PlanFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = ();

    fn capability(&self, input: Capability) -> RecordCapability {
        input.with_ordering(self.accessor_ordering)
    }

    fn prepare(&self, _: &ReadRequest) -> Result<()> {
        panic!("ordering composition must not prepare the format")
    }

    async fn create_reader(
        &self,
        _: &FactoryToken,
        _: Box<dyn ByteReader>,
        _: ReadRequest,
        _: (),
    ) -> Result<Box<dyn RecordReader>> {
        unreachable!()
    }
}

#[async_trait]
impl FileScanPlanner for FileAccessor<NoStorage, PlanFormat> {
    async fn plan_file_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let format = self.binding.factory();
        let metadata = RecordMetadata {
            schema: Arc::new(Schema::empty()),
            source: Some(request.object),
        };
        Ok(if format.whole_input {
            ScanPlan::whole_input(Box::new(StreamReader::records(
                stream::empty(),
                metadata,
                format.child_ordering,
            )))
        } else {
            ScanPlan::new(metadata, vec![]).with_ordering(format.child_ordering)
        })
    }
}

#[tokio::test]
async fn shared_planner_combines_requirements_for_empty_and_whole_input_plans() {
    use OrderRequirement::{PreserveGlobal, Reorderable};

    for whole_input in [false, true] {
        for (accessor_ordering, child_ordering, expected) in [
            (Reorderable, PreserveGlobal, PreserveGlobal),
            (PreserveGlobal, Reorderable, PreserveGlobal),
            (Reorderable, Reorderable, Reorderable),
        ] {
            let accessor = FileAccessor::from_format(
                NoStorage,
                PlanFormat {
                    accessor_ordering,
                    child_ordering,
                    whole_input,
                },
            );
            let plan = accessor.plan_scan(ReadRequest::new("input")).await.unwrap();
            assert_eq!(plan.ordering, expected);
            let partitions = plan.partitions.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(partitions.len(), usize::from(whole_input));
        }
    }
}
