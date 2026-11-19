//! Typed WARC builders. Body views adopt parser allocations instead of copying them.
use std::sync::Arc;

use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        BinaryViewBuilder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
        TimestampMillisecondBuilder,
    },
};
use arrow_schema::DataType;
use open_arrow_access_core::{Result, SchemaRef};

use super::{arrow::HEADER_COLUMNS, decode_error};

enum Value {
    Id,
    Type,
    Version,
    Source,
    Length,
    Header(warc::WarcHeader),
    Native,
}

enum Column {
    Text(StringBuilder),
    LargeText(LargeStringBuilder),
    Date(TimestampMillisecondBuilder),
    Body(LargeBinaryBuilder),
    BodyView(BinaryViewBuilder),
}

pub(super) struct WarcBatchBuilder {
    schema: SchemaRef,
    columns: Vec<Column>,
    values: Vec<Value>,
    rows: usize,
    body_bytes: usize,
}

impl WarcBatchBuilder {
    pub(super) fn new(schema: SchemaRef, capacity: usize) -> Result<Self> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Utf8 => {
                    Column::Text(StringBuilder::with_capacity(capacity, capacity * 16))
                }
                DataType::LargeUtf8 => {
                    Column::LargeText(LargeStringBuilder::with_capacity(capacity, capacity * 16))
                }
                DataType::LargeBinary => Column::Body(LargeBinaryBuilder::new()),
                DataType::BinaryView => {
                    Column::BodyView(BinaryViewBuilder::with_capacity(capacity))
                }
                DataType::Timestamp(_, _) => {
                    Column::Date(TimestampMillisecondBuilder::with_capacity(capacity))
                }
                _ => unreachable!("WARC schema contains only supported scalar types"),
            })
            .collect();
        let values = schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "id" => Value::Id,
                "type" => Value::Type,
                "warc_version" => Value::Version,
                "source_uri" => Value::Source,
                "content_length" => Value::Length,
                "body" | "date" => Value::Native,
                name => Value::Header(
                    HEADER_COLUMNS
                        .iter()
                        .find(|(column, _, _, _)| *column == name)
                        .expect("WARC column has a header mapping")
                        .1
                        .clone(),
                ),
            })
            .collect();
        Ok(Self {
            schema,
            columns,
            values,
            rows: 0,
            body_bytes: 0,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.rows
    }
    pub(super) fn body_bytes(&self) -> usize {
        self.body_bytes
    }
    pub(super) fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub(super) fn append(
        &mut self,
        headers: warc::RawRecordHeader,
        body: Vec<u8>,
        source: &str,
    ) -> Result<()> {
        let body_len = body.len();
        // Validate headers once. Keep the owned body separate so a BinaryView can
        // adopt it without reconstructing the parser's normalized raw headers.
        let record = warc::Record::<warc::EmptyBody>::try_from(headers).map_err(decode_error)?;
        for (input, column) in self.values.iter().zip(&mut self.columns) {
            match column {
                Column::Date(builder) => builder.append_value(record.date().timestamp_millis()),
                Column::Body(builder) => builder.append_value(&body),
                Column::BodyView(_) => {}
                Column::Text(_) | Column::LargeText(_) => {
                    let value = match input {
                        Value::Id => Some(std::borrow::Cow::Borrowed(record.warc_id())),
                        Value::Type => {
                            Some(std::borrow::Cow::Borrowed(record.warc_type().as_str()))
                        }
                        Value::Version => Some(std::borrow::Cow::Borrowed(record.warc_version())),
                        Value::Source => Some(std::borrow::Cow::Borrowed(source)),
                        Value::Length => Some(std::borrow::Cow::Owned(body.len().to_string())),
                        Value::Header(header) => record.header(header.clone()),
                        Value::Native => unreachable!("native WARC field has a typed builder"),
                    };
                    match column {
                        Column::Text(builder) => builder.append_option(value.as_deref()),
                        Column::LargeText(builder) => builder.append_option(value.as_deref()),
                        _ => unreachable!(),
                    }
                }
            }
        }
        if let Some(Column::BodyView(builder)) = self
            .columns
            .iter_mut()
            .find(|column| matches!(column, Column::BodyView(_)))
        {
            // Long values retain the exact parser allocation. Tiny values are
            // stored inline by Arrow, avoiding an allocation retained per row.
            if body.len() <= 12 {
                builder.append_value(&body);
            } else {
                let length = u32::try_from(body.len()).map_err(decode_error)?;
                let block = builder.append_block(body.into());
                builder
                    .try_append_view(block, 0, length)
                    .map_err(decode_error)?;
            }
        }
        self.body_bytes = self.body_bytes.saturating_add(body_len);
        self.rows += 1;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = self
            .columns
            .into_iter()
            .map(|column| match column {
                Column::Text(mut builder) => Arc::new(builder.finish()) as ArrayRef,
                Column::LargeText(mut builder) => Arc::new(builder.finish()),
                Column::Date(mut builder) => Arc::new(builder.finish()),
                Column::Body(mut builder) => Arc::new(builder.finish()),
                Column::BodyView(mut builder) => Arc::new(builder.finish()),
            })
            .collect();
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(self.rows));
        RecordBatch::try_new_with_options(self.schema, columns, &options).map_err(decode_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BinaryViewArray, TimestampMillisecondArray};
    use std::collections::HashMap;

    fn headers(length: usize) -> warc::RawRecordHeader {
        use warc::WarcHeader;
        warc::RawRecordHeader {
            version: "1.1".into(),
            headers: HashMap::from([
                (WarcHeader::WarcType, b"resource".to_vec()),
                (WarcHeader::RecordID, b"<urn:test:owned-body>".to_vec()),
                (WarcHeader::Date, b"2020-07-08T02:52:55.123Z".to_vec()),
                (WarcHeader::ContentLength, length.to_string().into_bytes()),
            ]),
        }
    }

    #[test]
    fn body_views_retain_the_parser_allocation_and_typed_dates() {
        let body = vec![0xfe; 4096];
        let allocation = body.as_ptr();
        let mut batch = WarcBatchBuilder::new(super::super::arrow::body_view_schema(), 3).unwrap();
        batch
            .append(headers(body.len()), body, "input.warc")
            .unwrap();
        batch
            .append(headers(3), b"abc".to_vec(), "input.warc")
            .unwrap();
        batch.append(headers(0), Vec::new(), "input.warc").unwrap();
        let output = batch.finish().unwrap();
        let bodies = output
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        assert_eq!(bodies.data_buffers()[0].as_ptr(), allocation);
        assert_eq!(bodies.value(0), vec![0xfe; 4096]);
        assert_eq!(bodies.value(1), b"abc");
        assert_eq!(bodies.value(2), b"");
        assert_eq!(bodies.null_count(), 0);
        let dates = output
            .column_by_name("date")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(dates.value(0), 1_594_176_775_123);
        assert_eq!(dates.null_count(), 0);
    }
}
