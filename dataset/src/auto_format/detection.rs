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
///
/// # Examples
///
/// ```
/// use open_arrow_access_dataset::auto_format::{DetectedFormat, detect_format};
///
/// assert_eq!(detect_format("data/events.v1.JSONL.gz?token=x"), Some(DetectedFormat::Json));
/// assert_eq!(detect_format("data/records.TSV.zstd"), Some(DetectedFormat::Tsv));
/// assert_eq!(detect_format("dir.csv/records.gz"), None);
/// assert_eq!(detect_format("data/archive.tgz"), None);
/// ```
pub fn detect_format(locator: &str) -> Option<DetectedFormat> {
    let path = locator.split(['?', '#']).next()?;
    let factory = CompressionFactory::default();
    let inner = factory.strip_suffix(path);
    // Inspect only the filename, so dots in parent directories cannot supply a
    // format suffix: `dir.csv/records.gz` becomes `records`, which has none.
    let name = inner.rsplit('/').next()?;
    // Take the final extension (`events.v1.JSONL` -> `JSONL`) and normalize its
    // ASCII case. `as_deref` borrows the resulting Option<String> as Option<&str>
    // so the match below can use string-literal patterns such as Some("jsonl").
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
    let mut selected = Vec::with_capacity(objects.len());
    selected.push(example.clone());
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
