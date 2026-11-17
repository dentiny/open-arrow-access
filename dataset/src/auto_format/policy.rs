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

impl DetectedFormat {
    fn family(self) -> Self {
        if self == Self::Tsv { Self::Csv } else { self }
    }
}

/// Detect suffixes without I/O, independently of enabled readers and codecs.
/// Queries/fragments affect routing only; storage retains the original locator.
pub fn detect_format(locator: &str) -> Option<DetectedFormat> {
    let path = locator.split(['?', '#']).next()?;
    if path.to_ascii_lowercase().ends_with(".tgz") {
        return None;
    }
    let factory = CompressionFactory::default();
    let inner = match path.rsplit_once('.') {
        Some((stem, extension))
            if factory
                .extensions()
                .any(|suffix| extension.eq_ignore_ascii_case(suffix)) =>
        {
            stem
        }
        _ => path,
    };
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

/// Exclusive formats win in configured order after filtering by `allowed`.
/// Compatibility groups must cover the whole selection; they are not transitive.
/// Only CSV/TSV share a reader; unrelated reader families cannot be combined.
#[derive(Clone, Debug)]
pub struct FormatPolicy {
    pub exclusive: Vec<DetectedFormat>,
    pub compatible: Vec<Vec<DetectedFormat>>,
    pub allowed: Vec<DetectedFormat>,
}
impl Default for FormatPolicy {
    fn default() -> Self {
        use DetectedFormat::*;
        Self {
            exclusive: vec![Parquet],
            compatible: vec![vec![Csv, Tsv]],
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
        let compatible = self
            .compatible
            .iter()
            .any(|group| selected.iter().all(|(format, _)| group.contains(format)));
        let conflict = selected
            .iter()
            .find(|(f, _)| f.family() != first.family())
            .or_else(|| selected.iter().find(|(f, _)| !compatible && *f != first));
        if let Some((other, object)) = conflict {
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
