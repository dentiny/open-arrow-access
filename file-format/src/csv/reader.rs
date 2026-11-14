use arrow_array::StringArray;
use arrow_csv::reader::Decoder;
use open_arrow_access_core::{
    BatchDecoder, ByteReader, OrderRequirement, RecordBatch, RecordMetadata, RecordReader, Result,
    decode_batches,
};

use super::error::decode_error;

struct CsvDecoder {
    data: Decoder,
    header: Option<Decoder>,
}

impl CsvDecoder {
    fn validate_header(&mut self) -> Result<()> {
        let Some(batch) = self
            .header
            .as_mut()
            .expect("pending header")
            .flush()
            .map_err(decode_error)?
        else {
            self.header = None;
            return Ok(());
        };
        for (index, field) in batch.schema().fields().iter().enumerate() {
            let actual = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("header columns are strings")
                .value(0);
            if actual != field.name() {
                return Err(decode_error(arrow_schema::ArrowError::CsvError(format!(
                    "CSV header does not match schema at column {index}: expected {:?} but found {actual:?}",
                    field.name(),
                ))));
            }
        }
        self.header = None;
        Ok(())
    }
}

impl BatchDecoder for CsvDecoder {
    const INPUT_OPERATION: &'static str = "read_csv";

    fn is_ready(&self) -> bool {
        self.data.capacity() == 0
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<usize> {
        if let Some(header) = &mut self.header {
            let consumed = header.decode(bytes).map_err(decode_error)?;
            let ready = header.capacity() == 0;
            // Advance the ordinary decoder through the same header bytes. This
            // retains its source-start/BOM and line-number state for data rows.
            let skipped = self.data.decode(&bytes[..consumed]).map_err(decode_error)?;
            debug_assert_eq!(skipped, consumed);
            if ready {
                self.validate_header()?;
            }
            return Ok(consumed);
        }
        self.data.decode(bytes).map_err(decode_error)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        self.data.flush().map_err(decode_error)
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        // Arrow CSV needs an empty decode to finalize a row without a newline.
        if let Some(header) = &mut self.header {
            header.decode(&[]).map_err(decode_error)?;
            self.validate_header()?;
        }
        self.data.decode(&[]).map_err(decode_error)?;
        self.data.flush().map_err(decode_error)
    }
}

pub(super) fn csv_reader(
    input: Box<dyn ByteReader>,
    decoder: Decoder,
    header: Option<Decoder>,
    metadata: RecordMetadata,
) -> impl RecordReader {
    decode_batches(
        input,
        CsvDecoder {
            data: decoder,
            header,
        },
        metadata,
        OrderRequirement::Reorderable,
    )
}
