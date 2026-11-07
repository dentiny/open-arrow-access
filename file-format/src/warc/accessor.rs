use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, ObjectRef, Reader, Result, async_trait,
};

use super::{WarcRecord, reader::create_reader};

/// Parse physical WARC records individually, in input order.
///
/// Each item is an owned parser record, including its buffered body. Metadata is
/// the input object locator. Arrow conversion and batching are deferred; this
/// accessor does not implement the Arrow `RecordReader` interface.
#[derive(Clone, Debug)]
pub struct WarcAccessor<A> {
    inner: A,
}

impl<A> WarcAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<A> Accessor for WarcAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Request = ByteReadRequest;
    type Reader = dyn Reader<Item = WarcRecord, Metadata = ObjectRef>;

    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<Self::Reader>> {
        let source = request.object.clone();
        let input = self.inner.open(request).await?;
        Ok(Box::new(create_reader(input, source)))
    }
}
