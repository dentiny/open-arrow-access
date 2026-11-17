use crate::{
    BytePosition, Error, ErrorKind, Reader, RecordBatch, RecordCapability, RecordMetadata, Result,
};

/// One decoded Arrow batch with one original byte coordinate per row.
///
/// Coordinates travel with their rows, rather than through a separate stream or
/// a byte buffer that another decoder must parse. Construction validates their
/// count even for zero-column batches. Row-changing transforms must update the
/// coordinates together with the rows.
#[derive(Clone, Debug)]
pub struct PositionedBatch {
    batch: RecordBatch,
    positions: Vec<BytePosition>,
}

impl PositionedBatch {
    pub fn try_new(batch: RecordBatch, positions: Vec<BytePosition>) -> Result<Self> {
        if batch.num_rows() != positions.len() {
            return Err(Error::new(
                ErrorKind::Decode,
                "positioned_batch",
                format!(
                    "batch produced {} rows for {} source positions",
                    batch.num_rows(),
                    positions.len()
                ),
            ));
        }
        Ok(Self { batch, positions })
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }
    pub fn positions(&self) -> &[BytePosition] {
        &self.positions
    }
    pub fn into_parts(self) -> (RecordBatch, Vec<BytePosition>) {
        (self.batch, self.positions)
    }
}

/// A decoder that supplies row coordinates together with decoded Arrow batches.
/// Metadata describes the Arrow schema; the usual reader lifecycle applies.
/// Unsupported formats continue to produce ordinary `RecordReader`s.
pub trait PositionedRecordReader:
    Reader<Item = PositionedBatch, Metadata = RecordMetadata, Capability = RecordCapability>
{
}

impl<T> PositionedRecordReader for T where
    T: Reader<Item = PositionedBatch, Metadata = RecordMetadata, Capability = RecordCapability>
        + ?Sized
{
}
