use open_arrow_access_core::{BytePosition, PositionedBatch, RecordBatch};

mod sealed {
    pub trait Sealed {}
    impl Sealed for open_arrow_access_core::RecordBatch {}
    impl Sealed for open_arrow_access_core::PositionedBatch {}
}

/// Batch inputs accepted by `SourceColumns`.
///
/// Ordinary Arrow batches support object columns; `PositionedBatch` also supports
/// offsets. This sealed bridge lets the same adapter accept either reader interface.
pub trait SourceColumnBatch: sealed::Sealed + Send + 'static {
    const HAS_POSITIONS: bool;
    fn into_source_parts(self) -> (RecordBatch, Vec<BytePosition>);
}
impl SourceColumnBatch for RecordBatch {
    const HAS_POSITIONS: bool = false;
    fn into_source_parts(self) -> (RecordBatch, Vec<BytePosition>) {
        (self, Vec::new())
    }
}
impl SourceColumnBatch for PositionedBatch {
    const HAS_POSITIONS: bool = true;
    fn into_source_parts(self) -> (RecordBatch, Vec<BytePosition>) {
        self.into_parts()
    }
}
