/// File format inferred from a filename, independently of enabled readers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DetectedFormat {
    Parquet,
    Csv,
    Tsv,
    Json,
    JsonLines,
    Warc,
    Xml,
}

/// Inspect a path suffix without I/O. One outer compression suffix is ignored;
/// query and fragment text is excluded. Detection does not enable a decoder.
/// Semantic formats such as ATIF require an explicitly selected reader.
pub fn detect_format(path: &str) -> Option<DetectedFormat> {
    let path = path.split(['?', '#']).next()?;
    let compression = open_arrow_access_decompression::CompressionFactory::default();
    let path = compression.strip_suffix(path);
    let extension = path.rsplit_once('.')?.1.to_ascii_lowercase();
    Some(match extension.as_str() {
        "parquet" => DetectedFormat::Parquet,
        "csv" => DetectedFormat::Csv,
        "tsv" => DetectedFormat::Tsv,
        "json" => DetectedFormat::Json,
        "jsonl" | "ndjson" => DetectedFormat::JsonLines,
        "warc" => DetectedFormat::Warc,
        "xml" => DetectedFormat::Xml,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_suffixes_without_enabling_readers() {
        for (path, format) in [
            ("a.parquet", Some(DetectedFormat::Parquet)),
            ("a.csv", Some(DetectedFormat::Csv)),
            ("a.tsv.bz2", Some(DetectedFormat::Tsv)),
            ("a.json", Some(DetectedFormat::Json)),
            ("a.ndjson.zstd?download=1", Some(DetectedFormat::JsonLines)),
            ("a.WARC.GZ", Some(DetectedFormat::Warc)),
            ("a.xml", Some(DetectedFormat::Xml)),
            ("a.atif", None),
            ("warc.paths.gz", None),
        ] {
            assert_eq!(detect_format(path), format, "{path}");
        }
    }
}
