use open_arrow_access_core::{Error, ErrorKind, ObjectRef, Result};
use open_arrow_access_decompression::CompressionFactory;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetectedFormat {
    Parquet,
    Csv,
    Tsv,
    Json,
    Warc,
}

/// Detect suffixes without I/O, independently of enabled readers and codecs.
/// Queries/fragments affect routing only; storage retains the original locator.
pub fn detect_format(locator: &str) -> Option<DetectedFormat> {
    let path = locator.split(['?', '#']).next()?;
    let factory = CompressionFactory::default();
    let inner = factory.strip_suffix(path);
    let name = inner.rsplit('/').next()?;
    match name
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase())
        .as_deref()
    {
        Some("parquet") => Some(DetectedFormat::Parquet),
        Some("csv") => Some(DetectedFormat::Csv),
        Some("tsv") => Some(DetectedFormat::Tsv),
        Some("json" | "jsonl" | "ndjson") => Some(DetectedFormat::Json),
        Some("warc") => Some(DetectedFormat::Warc),
        _ => None,
    }
}

/// Select one format without opening files. Ignore unknown suffixes and reject
/// mixed formats, including CSV/TSV. Preserve recognized objects' order and duplicates.
pub fn select_format(objects: &[ObjectRef]) -> Result<(DetectedFormat, Vec<ObjectRef>)> {
    let mut detected = objects
        .iter()
        .filter_map(|object| detect_format(object.locator()).map(|format| (format, object)));
    let Some((first, example)) = detected.next() else {
        return Err(Error::new(
            ErrorKind::NotFound,
            "select_format",
            "no recognized dataset files",
        )
        .with_object(objects.first()));
    };
    let mut selected = vec![example.clone()];
    for (other, object) in detected {
        if other != first {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "select_format",
                format!(
                    "mixed formats {first:?} ({:?}) and {other:?} ({:?})",
                    example.locator(),
                    object.locator()
                ),
            )
            .with_object(Some(object)));
        }
        selected.push(object.clone());
    }
    Ok((first, selected))
}
