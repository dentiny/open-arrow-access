use futures::{
    StreamExt,
    io::AsyncBufReadExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{
    BytePosition, ByteReader, Error, ErrorKind, PositionedAsyncRead, Result,
    into_positioned_async_read,
};
use open_arrow_access_layers::{SourceFramer, SourceUnit};

const UNIT_BYTES: usize = 64 << 20;

/// Groups physical WARC records into independently decodable units.
///
/// Use with `SourceColumnsLayer` and positioned input (for compressed storage,
/// install `SourcePositionLayer` before this framer). Headers are parsed and
/// reserialized by warc-rs; bodies and original record coordinates are preserved.
/// Compression frame boundaries need not coincide with record or unit boundaries.
///
/// ```
/// use open_arrow_access_file_format::warc::WarcFramer;
/// use open_arrow_access_layers::SourceColumnsLayer;
///
/// let columns = SourceColumnsLayer::new()
///     .with_framer(WarcFramer)
///     .with_offset("record_offset");
/// ```
/// Units group complete records until they reach an internal 64 MiB target.
/// This is a batching target, not a size limit: the final record may take a unit
/// past it, and individual records are never rejected because of their size.
/// Units and decoded batches are buffered in memory; account for record size
/// and concurrently executing units when budgeting memory.
#[derive(Clone, Copy, Debug, Default)]
pub struct WarcFramer;

impl SourceFramer for WarcFramer {
    fn frame(&self, input: Box<dyn ByteReader>) -> BoxStream<'static, Result<SourceUnit>> {
        let input = into_positioned_async_read(input, "frame_warc");
        stream::try_unfold(input, |mut input| async move {
            let mut writer = warc::WarcWriter::new(Vec::new());
            let mut positions = Vec::new();
            let mut bytes = 0;
            while bytes < UNIT_BYTES {
                let Some((position, written)) = read_record(&mut input, &mut writer).await? else {
                    break;
                };
                positions.push(position);
                bytes += written;
            }
            Ok((!positions.is_empty()).then(|| {
                (
                    SourceUnit {
                        bytes: writer.into_inner().into(),
                        positions,
                    },
                    input,
                )
            }))
        })
        .boxed()
    }
}

async fn read_record(
    input: &mut PositionedAsyncRead,
    writer: &mut warc::WarcWriter<Vec<u8>>,
) -> Result<Option<(BytePosition, usize)>> {
    // Ignore inter-record whitespace without buffering it or assigning it to a
    // record's coordinate. Do not wrap in BufReader: read-ahead would move marks.
    loop {
        let available = input.fill_buf().await.map_err(super::decode_error)?;
        if available.is_empty() {
            return Ok(None);
        }
        let whitespace = available
            .iter()
            .take_while(|b| b.is_ascii_whitespace())
            .count();
        if whitespace == 0 {
            break;
        }
        input.consume_unpin(whitespace);
    }
    let observer = input.observer();
    observer.mark();
    let record = warc::WarcReader::new(input)
        .iter_raw_records()
        .next()
        .await
        .ok_or_else(|| Error::new(ErrorKind::Decode, "frame_warc", "missing WARC record"))?
        .map_err(|error| {
            Error::new(
                ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode),
                "frame_warc",
                "invalid WARC record",
            )
            .with_source(error)
        })?;
    let position = observer.marked_position().ok_or_else(|| {
        Error::new(
            ErrorKind::Unsupported,
            "frame_warc",
            "WARC input has no byte positions",
        )
    })?;
    let written = writer
        .write_raw(record.0, &record.1)
        .await
        .map_err(super::decode_error)?;
    Ok(Some((position, written)))
}
