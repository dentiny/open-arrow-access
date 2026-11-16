//! Normalize PMC content links after JSON decoding, before returning Arrow rows.
use arrow_array::{ArrayRef, ListArray, StringArray, cast::AsArray};
use std::sync::Arc;

pub(super) fn sanitize_column(column: &ArrayRef, name: &str) -> ArrayRef {
    match name {
        "pdf_url" | "xml_url" | "text_url" => Arc::new(sanitize_strings(column)),
        "media_urls" => {
            let (field, offsets, values, nulls) = column.as_list::<i32>().clone().into_parts();
            Arc::new(ListArray::new(
                field,
                offsets,
                Arc::new(sanitize_strings(&values)),
                nulls,
            ))
        }
        _ => column.clone(),
    }
}

fn sanitize_strings(column: &ArrayRef) -> StringArray {
    column
        .as_string::<i32>()
        .iter()
        .map(|value| value.map(sanitize_url))
        .collect()
}

/// PMC S3 links use a single MD5 query as metadata, not as part of the key.
/// Validate its shape without verifying content. Keep other URLs unchanged,
/// including queries that may select object versions or carry authentication.
fn sanitize_url(value: &str) -> &str {
    if value.starts_with("s3://")
        && !value.contains('#')
        && let Some((object, query)) = value.split_once('?')
        && let Some(md5) = query.strip_prefix("md5=")
        && md5.len() == 32
        && md5.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return object;
    }
    value
}
