use futures::stream::BoxStream;
use open_arrow_access_core::{BytePosition, ByteReader, Bytes, Result};

/// Complete, independently decodable input for an ordinary format reader.
///
/// `positions` contains one coordinate per output row, in parser order. A unit
/// may group several records to amortize reader creation. Include any headers
/// the format needs to decode the unit independently. All units must produce
/// the same data schema. The layer buffers decoded batches within one unit
/// to check the row count before exposing its source columns.
pub struct SourceUnit {
    pub bytes: Bytes,
    pub positions: Vec<BytePosition>,
}

/// User-supplied record framing; the source layer has no format-specific parser.
///
/// Units must cover the input exactly once in logical record order. The framer
/// owns boundary detection and row coordinates, including any records a parser
/// skips or expands. Discovery is polled only as the scan window has room.
/// For byte-based framing, `into_positioned_async_read` observes consumed bytes
/// rather than decoder read-ahead. External indexes may supply coordinates too.
pub trait SourceFramer: Send + Sync + 'static {
    fn frame(&self, input: Box<dyn ByteReader>) -> BoxStream<'static, Result<SourceUnit>>;
}
