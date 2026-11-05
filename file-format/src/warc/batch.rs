use std::sync::Arc;

use arrow_array::{
    ArrayRef, RecordBatchOptions,
    builder::{BinaryViewBuilder, StringBuilder, TimestampMillisecondBuilder, UInt64Builder},
};
use open_arrow_access_core::{Error, ErrorKind, RecordBatch, Result, SchemaRef};
use warc::{EmptyBody, Record, WarcHeader};

use super::decode_error;

enum Column {
    Text(StringBuilder),
    UInt64(UInt64Builder),
    Timestamp(TimestampMillisecondBuilder),
    Body(BinaryViewBuilder),
}

pub(super) struct WarcBatch {
    schema: SchemaRef,
    indices: Vec<usize>,
    builders: Vec<Column>,
    pub(super) len: usize,
    pub(super) body_bytes: usize,
}

impl WarcBatch {
    pub(super) fn new(schema: SchemaRef, indices: &[usize], capacity: usize) -> Self {
        let builders = indices
            .iter()
            .map(|index| match index {
                1 | 16 | 18 => Column::UInt64(UInt64Builder::with_capacity(capacity)),
                2 => Column::Timestamp(TimestampMillisecondBuilder::with_capacity(capacity)),
                19 => Column::Body(BinaryViewBuilder::with_capacity(capacity)),
                _ => Column::Text(StringBuilder::with_capacity(
                    capacity,
                    capacity.saturating_mul(16),
                )),
            })
            .collect();
        Self {
            schema,
            indices: indices.to_vec(),
            builders,
            len: 0,
            body_bytes: 0,
        }
    }

    pub(super) fn append(&mut self, record: Record<EmptyBody>, body: Vec<u8>) -> Result<()> {
        let body_len = body.len();
        let mut body = Some(body);
        for (&index, builder) in self.indices.iter().zip(&mut self.builders) {
            match (index, builder) {
                (1, Column::UInt64(builder)) => builder.append_value(body_len as u64),
                (16 | 18, Column::UInt64(builder)) => {
                    let header = if index == 16 {
                        WarcHeader::SegmentNumber
                    } else {
                        WarcHeader::SegmentTotalLength
                    };
                    let value = record
                        .header(header)
                        .map(|value| {
                            value.parse::<u64>().map_err(|error| {
                                Error::new(
                                    ErrorKind::Decode,
                                    "decode_warc",
                                    "invalid numeric segment header",
                                )
                                .with_source(error)
                            })
                        })
                        .transpose()?;
                    builder.append_option(value);
                }
                (2, Column::Timestamp(builder)) => {
                    builder.append_value(record.date().timestamp_millis())
                }
                (19, Column::Body(builder)) => {
                    let value = body.take().unwrap();
                    if value.len() >= u32::MAX as usize {
                        return Err(Error::new(
                            ErrorKind::Decode,
                            "decode_warc",
                            "WARC body exceeds BinaryView's value-size limit",
                        ));
                    }
                    let block = builder.append_block(value.into());
                    builder
                        .try_append_view(block, 0, body_len as u32)
                        .map_err(decode_error)?;
                }
                (0, Column::Text(builder)) => builder.append_value(record.warc_id()),
                (3, Column::Text(builder)) => builder.append_value(record.warc_type().as_str()),
                (index, Column::Text(builder)) => {
                    builder.append_option(record.header(string_header(index)).as_deref())
                }
                _ => unreachable!("WARC schema and builders must stay aligned"),
            }
        }
        self.len += 1;
        self.body_bytes = self.body_bytes.saturating_add(body_len);
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(|builder| match builder {
                Column::Text(builder) => Arc::new(builder.finish()) as ArrayRef,
                Column::UInt64(builder) => Arc::new(builder.finish()),
                Column::Timestamp(builder) => Arc::new(builder.finish()),
                Column::Body(builder) => Arc::new(builder.finish()),
            })
            .collect();
        RecordBatch::try_new_with_options(
            self.schema,
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(self.len)),
        )
        .map_err(decode_error)
    }
}

fn string_header(index: usize) -> WarcHeader {
    match index {
        4 => WarcHeader::ContentType,
        5 => WarcHeader::ConcurrentTo,
        6 => WarcHeader::BlockDigest,
        7 => WarcHeader::PayloadDigest,
        8 => WarcHeader::IPAddress,
        9 => WarcHeader::RefersTo,
        10 => WarcHeader::TargetURI,
        11 => WarcHeader::Truncated,
        12 => WarcHeader::WarcInfoID,
        13 => WarcHeader::Filename,
        14 => WarcHeader::Profile,
        15 => WarcHeader::IdentifiedPayloadType,
        17 => WarcHeader::SegmentOriginID,
        _ => unreachable!("WARC string field has a header"),
    }
}
