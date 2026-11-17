use futures::{
    StreamExt,
    io::{AsyncBufReadExt, AsyncReadExt},
    stream::{self, BoxStream},
};
use open_arrow_access_core::{
    ByteReader, Error, ErrorKind, PositionedAsyncRead, Result, into_positioned_async_read,
};
use open_arrow_access_layers::{SourceFramer, SourceUnit};

/// Groups physical WARC records into bounded, independently decodable units.
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
///     .with_framer(WarcFramer::default().with_unit_bytes(64 << 20))
///     .with_offset("record_offset");
/// ```
#[derive(Clone, Copy, Debug)]
pub struct WarcFramer {
    unit_bytes: usize,
}

impl Default for WarcFramer {
    fn default() -> Self {
        Self {
            unit_bytes: 64 << 20,
        }
    }
}

impl WarcFramer {
    /// Set the maximum serialized bytes per unit (default: 64 MiB).
    ///
    /// Records are never split. A record whose input or reserialized size exceeds
    /// this limit fails with `Decode`; zero fails with `InvalidInput` when polled.
    /// Several records are packed into each unit up to this limit, amortizing
    /// format-reader creation. Framing also holds at most one bounded lookahead
    /// record and parser scratch space. Account separately for decoded batches
    /// and concurrently executing units in the source layer.
    pub fn with_unit_bytes(mut self, unit_bytes: usize) -> Self {
        self.unit_bytes = unit_bytes;
        self
    }
}

impl SourceFramer for WarcFramer {
    fn frame(&self, input: Box<dyn ByteReader>) -> BoxStream<'static, Result<SourceUnit>> {
        let limit = self.unit_bytes;
        let input = into_positioned_async_read(input, "frame_warc");
        stream::try_unfold((input, None), move |(mut input, pending)| async move {
            if limit == 0 {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "frame_warc",
                    "WARC unit byte limit must be positive",
                ));
            }
            let mut unit = pending.unwrap_or(SourceUnit {
                bytes: Default::default(),
                positions: Vec::new(),
            });
            let mut bytes = unit.bytes.to_vec();
            while bytes.len() < limit {
                let Some(record) = read_record(&mut input, limit).await? else {
                    break;
                };
                if record.bytes.len() > limit - bytes.len() {
                    unit.bytes = bytes.into();
                    return Ok(Some((unit, (input, Some(record)))));
                }
                bytes.extend_from_slice(&record.bytes);
                unit.positions.extend(record.positions);
            }
            unit.bytes = bytes.into();
            Ok((!unit.positions.is_empty()).then_some((unit, (input, None))))
        })
        .boxed()
    }
}

async fn read_record(input: &mut PositionedAsyncRead, limit: usize) -> Result<Option<SourceUnit>> {
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
    // Limit parser consumption before it can buffer an arbitrarily large body.
    // A fresh borrowed iterator also resets this budget at each record boundary.
    let record = warc::WarcReader::new(input.take(limit as u64))
        .iter_raw_records()
        .next()
        .await
        .ok_or_else(|| Error::new(ErrorKind::Decode, "frame_warc", "missing WARC record"))?
        .map_err(|error| {
            Error::new(
                ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode),
                "frame_warc",
                format!("invalid WARC record or record exceeds {limit} byte unit limit"),
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
    let mut writer = warc::WarcWriter::new(Vec::new());
    let written = writer
        .write_raw(record.0, &record.1)
        .await
        .map_err(super::decode_error)?;
    if written > limit {
        return Err(Error::new(
            ErrorKind::Decode,
            "frame_warc",
            format!("reserialized WARC record exceeds {limit} byte unit limit"),
        ));
    }
    Ok(Some(SourceUnit {
        bytes: writer.into_inner().into(),
        positions: vec![position],
    }))
}
