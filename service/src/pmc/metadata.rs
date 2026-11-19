use super::{PmcFormat, schema, validation};
use arrow_schema::Schema;
use open_arrow_access_core::{
    FieldNameIndex, RecordBatch, Result, SchemaRef, record_batch_with_rows,
};
use std::{
    cell::RefCell,
    sync::{Arc, OnceLock},
};

// All PMC documents use the same input schema, independently of output format
// and projection. Retain one clean decoder per thread without a shared lock.
// Large documents must not pin their parser allocations for the thread's life.
const MAX_CACHED_DOCUMENT_BYTES: usize = 64 * 1024;
thread_local! {
    static DECODER: RefCell<Option<arrow_json::reader::Decoder>> = const { RefCell::new(None) };
}

/// Input contains the 16 upstream fields, excluding the derived content_url.
fn source_metadata_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let fields = schema()
                .fields()
                .iter()
                .filter(|field| field.name() != "content_url")
                .cloned()
                .collect::<Vec<_>>();
            Arc::new(Schema::new(fields))
        })
        .clone()
}

pub(super) async fn decode_batch(
    raw: &[u8],
    path: &str,
    output_schema: SchemaRef,
    format: PmcFormat,
) -> Result<RecordBatch> {
    // Read and validate the complete metadata before applying output projection.
    let source_schema = source_metadata_schema();
    // Reject unrecognized fields instead of silently dropping upstream metadata.
    let decode_error = |source: arrow_schema::ArrowError| {
        super::error::decode(format!("{path}: {source}")).with_source(source)
    };
    // Take the decoder out of the slot: any parse, flush or validation error
    // drops it, so a subsequent document cannot inherit partial parser state.
    let mut decoder = match DECODER.with(|slot| slot.borrow_mut().take()) {
        Some(decoder) => decoder,
        None => arrow_json::ReaderBuilder::new(source_schema.clone())
            .with_strict_mode(true)
            .with_coerce_primitive(false)
            .with_batch_size(2)
            .build_decoder()
            .map_err(decode_error)?,
    };
    if decoder.decode(raw).map_err(decode_error)? != raw.len() {
        return Err(super::error::decode(format!(
            "{path}: expected one metadata object"
        )));
    }
    let batch = decoder
        .flush()
        .map_err(decode_error)?
        .ok_or_else(|| super::error::decode(format!("{path}: expected one metadata object")))?;
    if batch.num_rows() != 1 {
        return Err(super::error::decode(format!(
            "{path}: expected one metadata object"
        )));
    }
    static FIELD_INDEX: OnceLock<FieldNameIndex> = OnceLock::new();
    let field_index = FIELD_INDEX.get_or_init(|| FieldNameIndex::new(source_schema.fields()));
    validation::validate_metadata(&batch, path, field_index)?;
    let columns = output_schema
        .fields()
        .iter()
        .map(|field| {
            let source_name = match field.name().as_str() {
                "content_url" => format.metadata_field(),
                name => name,
            };
            let index = field_index
                .index_of(source_name)
                .expect("validated PMC projection");
            batch.column(index).clone()
        })
        .collect();
    let output = record_batch_with_rows(output_schema, columns, batch.num_rows())
        .map_err(|error| super::error::decode(error.to_string()).with_source(error))?;
    if raw.len() <= MAX_CACHED_DOCUMENT_BYTES {
        DECODER.with(|slot| *slot.borrow_mut() = Some(decoder));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_documents_and_errors_do_not_leave_a_cached_decoder() {
        futures::executor::block_on(async {
            decode_batch(
                br#"{"pmcid":"PMC1"}"#,
                "small.json",
                schema(),
                PmcFormat::Pdf,
            )
            .await
            .unwrap();
            assert!(DECODER.with(|slot| slot.borrow().is_some()));
            let large = format!(
                r#"{{"pmcid":"PMC2","title":"{}"}}"#,
                "x".repeat(MAX_CACHED_DOCUMENT_BYTES)
            );
            decode_batch(large.as_bytes(), "large.json", schema(), PmcFormat::Pdf)
                .await
                .unwrap();
            assert!(DECODER.with(|slot| slot.borrow().is_none()));
            decode_batch(
                br#"{"pmcid":"PMC1"}"#,
                "small.json",
                schema(),
                PmcFormat::Pdf,
            )
            .await
            .unwrap();
            assert!(
                decode_batch(
                    br#"{"pmcid":"unfinished"#,
                    "bad.json",
                    schema(),
                    PmcFormat::Pdf
                )
                .await
                .is_err()
            );
            assert!(DECODER.with(|slot| slot.borrow().is_none()));
        });
    }
}
