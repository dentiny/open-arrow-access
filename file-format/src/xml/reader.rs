use open_arrow_access_core::{
    Capability, Reader, RecordBatch, RecordMetadata, Result, async_trait,
};

use super::xml_error;

pub(super) struct XmlRecordReader {
    pub(super) reader: xml_arrow::XmlReader<'static>,
    pub(super) metadata: RecordMetadata,
}

#[async_trait]
impl Reader for XmlRecordReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.reader.next_batch().await.map_err(xml_error)
    }
}
