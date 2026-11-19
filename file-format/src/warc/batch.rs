//! Typed WARC batches; body views retain the parser's owned allocations.
use std::borrow::Cow;

use arrow_array::{
    RecordBatch, RecordBatchOptions,
    builder::{
        ArrayBuilder, BinaryViewBuilder, LargeStringBuilder, StringBuilder, StructBuilder,
        TimestampMillisecondBuilder,
    },
};
use open_arrow_access_core::{Result, SchemaRef};

use super::{arrow::HEADER_COLUMNS, decode_error};

pub(super) const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct WarcBatchBuilder {
    schema: SchemaRef,
    builder: StructBuilder,
    headers: Vec<Option<warc::WarcHeader>>,
    body_bytes: usize,
}

impl WarcBatchBuilder {
    pub(super) fn new(schema: SchemaRef, capacity: usize) -> Self {
        let builder = StructBuilder::from_fields(schema.fields().clone(), capacity);
        let headers = schema
            .fields()
            .iter()
            .map(|field| {
                HEADER_COLUMNS
                    .iter()
                    .find(|(name, _, _, _)| *name == field.name())
                    .map(|(_, header, _, _)| header.clone())
            })
            .collect();
        Self {
            schema,
            builder,
            headers,
            body_bytes: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.builder.len()
    }

    pub(super) fn body_bytes(&self) -> usize {
        self.body_bytes
    }

    pub(super) fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    pub(super) fn append(
        &mut self,
        headers: warc::RawRecordHeader,
        body: Vec<u8>,
        source: &str,
    ) -> Result<()> {
        // Validate even when no header columns are projected.
        let record = warc::Record::<warc::EmptyBody>::try_from(headers).map_err(decode_error)?;
        for ((field, header), column) in self
            .schema
            .fields()
            .iter()
            .zip(&self.headers)
            .zip(self.builder.field_builders_mut())
        {
            let value = match field.name().as_str() {
                "date" => {
                    column
                        .as_any_mut()
                        .downcast_mut::<TimestampMillisecondBuilder>()
                        .unwrap()
                        .append_value(record.date().timestamp_millis());
                    continue;
                }
                "body" => continue,
                "id" => Some(Cow::Borrowed(record.warc_id())),
                "type" => Some(Cow::Borrowed(record.warc_type().as_str())),
                "warc_version" => Some(Cow::Borrowed(record.warc_version())),
                "source_uri" => Some(Cow::Borrowed(source)),
                "content_length" => Some(Cow::Owned(body.len().to_string())),
                _ => record.header(header.as_ref().expect("WARC header mapping").clone()),
            };
            if let Some(builder) = column.as_any_mut().downcast_mut::<StringBuilder>() {
                builder.append_option(value.as_deref());
            } else {
                column
                    .as_any_mut()
                    .downcast_mut::<LargeStringBuilder>()
                    .unwrap()
                    .append_option(value.as_deref());
            }
        }
        self.body_bytes = self.body_bytes.saturating_add(body.len());
        if let Ok(index) = self.schema.index_of("body") {
            let builder = self
                .builder
                .field_builder::<BinaryViewBuilder>(index)
                .unwrap();
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
        self.builder.append(true);
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<RecordBatch> {
        let options = RecordBatchOptions::new().with_row_count(Some(self.len()));
        let columns = self.builder.finish().columns().to_vec();
        RecordBatch::try_new_with_options(self.schema, columns, &options).map_err(decode_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    #[tokio::test]
    async fn default_batch_retains_the_parsers_owned_body_allocation() {
        let body = b"a body longer than the twelve byte inline limit";
        let mut input = format!(
            "WARC/1.1\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:test:view>\r\nWARC-Date: 2020-07-08T02:52:55Z\r\nContent-Length: {}\r\n\r\n", body.len(),
        ).into_bytes();
        input.extend_from_slice(body);
        input.extend_from_slice(b"\r\n\r\n");
        let (headers, owned) = warc::WarcReader::new(futures::io::Cursor::new(input))
            .iter_raw_records()
            .next()
            .await
            .unwrap()
            .unwrap();
        let pointer = owned.as_ptr();
        let mut builder = WarcBatchBuilder::new(super::super::arrow_schema(), 1);
        builder.append(headers, owned, "input.warc").unwrap();
        let batch = builder.finish().unwrap();
        let view = batch["body"].as_binary_view();
        assert_eq!(view.value(0), body);
        assert_eq!(view.data_buffers()[0].as_ptr(), pointer);
    }
}
