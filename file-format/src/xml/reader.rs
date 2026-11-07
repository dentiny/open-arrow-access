use open_arrow_access_core::{
    Capability, Error, ErrorKind, ReadState, Reader, RecordBatch, RecordMetadata, Result,
    async_trait,
};

use super::xml_error;

pub(super) struct XmlRecordReader {
    pub(super) reader: xml_arrow::XmlReader<'static>,
    pub(super) metadata: RecordMetadata,
    pub(super) projection: Option<Vec<usize>>,
    pub(super) state: ReadState,
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
        if self.state.is_finished() {
            return Ok(None);
        }
        let result = self
            .reader
            .next_batch()
            .await
            .map_err(xml_error)
            .and_then(|batch| {
                batch
                    .map(|batch| match &self.projection {
                        None => Ok(batch),
                        Some(indices) => batch.project(indices).map_err(|error| {
                            Error::new(ErrorKind::Decode, "read_xml", error.to_string())
                                .with_source(error)
                        }),
                    })
                    .transpose()
            });
        self.state.observe(&result);
        result
    }
}
