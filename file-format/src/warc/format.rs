use futures::stream;
use open_arrow_access_core::{
    ByteReadRequest, ByteReader, FactoryToken, ObjectRef, Reader, ReaderFactory, Result,
    StreamReader, async_trait, into_async_read,
};

use super::{WarcRecord, decode_error};

/// Parse physical WARC records with owned headers and buffered binary bodies.
#[derive(Clone, Copy, Debug, Default)]
pub struct WarcFormat;

#[async_trait]
impl ReaderFactory for WarcFormat {
    type Request = ByteReadRequest;
    type Prepared = ();
    type Reader = dyn Reader<Item = WarcRecord, Metadata = ObjectRef>;

    fn prepare(&self, _request: &ByteReadRequest) -> Result<()> {
        Ok(())
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ByteReadRequest,
        (): (),
    ) -> Result<Box<Self::Reader>> {
        let input = into_async_read(input, "read_warc");
        let records = warc::WarcReader::new(input).iter_records();
        let stream = stream::try_unfold(records, |mut records| async move {
            Ok(records
                .next()
                .await
                .transpose()
                .map_err(decode_error)?
                .map(|record| (record, records)))
        });
        Ok(Box::new(StreamReader::new(stream, request.object)))
    }
}
