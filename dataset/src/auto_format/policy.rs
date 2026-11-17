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
    // .tgz is a tar archive, not a compressed record file.
    if path.to_ascii_lowercase().ends_with(".tgz") {
        return None;
    }
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
        None if inner != path && !name.is_empty() => Some(DetectedFormat::Json),
        _ => None,
    }
}

/// Filter allowed formats, apply exclusive preferences, then reject mixed formats.
/// CSV and TSV may be mixed because both use the CSV reader.
#[derive(Clone, Debug)]
pub struct FormatPolicy {
    /// If present, the first format in this list wins; discard all other formats.
    pub exclusive: Vec<DetectedFormat>,
    /// Accept CSV and TSV in the same selection. Defaults to true.
    pub allow_mixed_csv_tsv: bool,
    /// Ignore detected formats outside this list, before applying preferences.
    pub allowed: Vec<DetectedFormat>,
}
impl Default for FormatPolicy {
    fn default() -> Self {
        use DetectedFormat::*;
        Self {
            exclusive: vec![Parquet],
            allow_mixed_csv_tsv: true,
            allowed: vec![Parquet, Csv, Tsv, Json, Warc],
        }
    }
}
impl FormatPolicy {
    /// Select files without opening them. Preserve input order and duplicates.
    pub fn select(&self, objects: &[ObjectRef]) -> Result<(DetectedFormat, Vec<ObjectRef>)> {
        let mut selected: Vec<_> = objects
            .iter()
            .filter_map(|object| {
                let format = detect_format(object.locator())?;
                self.allowed.contains(&format).then_some((format, object))
            })
            .collect();
        if let Some(winner) = self
            .exclusive
            .iter()
            .find(|format| selected.iter().any(|(f, _)| f == *format))
        {
            selected.retain(|(format, _)| format == winner);
        }
        let Some(&(first, example)) = selected.first() else {
            return Err(Error::new(
                ErrorKind::NotFound,
                "select_format",
                "no recognized dataset files",
            )
            .with_object(objects.first()));
        };
        for &(other, object) in &selected {
            let csv_tsv = self.allow_mixed_csv_tsv
                && matches!(first, DetectedFormat::Csv | DetectedFormat::Tsv)
                && matches!(other, DetectedFormat::Csv | DetectedFormat::Tsv);
            if other == first || csv_tsv {
                continue;
            }
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
        Ok((
            first,
            selected
                .into_iter()
                .map(|(_, object)| object.clone())
                .collect(),
        ))
    }
}
