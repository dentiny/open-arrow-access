//! Select one decoder family for a file set before schema inspection or reading.
mod json;

use std::num::NonZeroUsize;

use open_arrow_access_core::{ByteAccessor, Error, ErrorKind, ObjectRef, Result};
pub use open_arrow_access_file_format::{DetectedFormat, detect_format};

use crate::file_set::FileSetRequest;

/// CSV/TSV and JSON/JSONL share decoder families; ATIF uses `AtifFormat::Auto`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedFileFormat {
    Parquet,
    Delimited,
    Json,
    Atif,
    Warc,
    Xml,
}

impl From<DetectedFormat> for ResolvedFileFormat {
    fn from(format: DetectedFormat) -> Self {
        match format {
            DetectedFormat::Parquet => Self::Parquet,
            DetectedFormat::Csv | DetectedFormat::Tsv => Self::Delimited,
            DetectedFormat::Json | DetectedFormat::JsonLines => Self::Json,
            DetectedFormat::Warc => Self::Warc,
            DetectedFormat::Xml => Self::Xml,
        }
    }
}

/// File selection and bounded JSON/ATIF detection.
#[derive(Clone, Debug)]
pub struct FormatPolicy {
    /// If present, select only this decoder family when it occurs in the input.
    /// For example, Some(Parquet) ignores JSON sidecars. Default: reject mixed families.
    pub preferred: Option<DetectedFormat>,
    /// Maximum decoded JSON sample per file. None disables content probing.
    /// Default: 1 MiB; every nonempty JSON file must agree on JSON versus ATIF.
    pub json_probe_bytes: Option<NonZeroUsize>,
}

impl Default for FormatPolicy {
    fn default() -> Self {
        Self {
            preferred: None,
            json_probe_bytes: NonZeroUsize::new(1024 * 1024),
        }
    }
}

/// Feed `request` to a FileSetAccessor using the selected decoder family.
#[derive(Clone, Debug)]
pub struct ResolvedFiles {
    pub format: ResolvedFileFormat,
    pub request: FileSetRequest,
}

/// Resolve a file set, ignoring unknown suffixes and preserving order, duplicates,
/// original locators and read/byte options. Use `directory::resolve_directory` first
/// for directory/glob inputs. An empty recognized selection returns NotFound;
/// mixed families fail with example locators unless a preference selects one.
///
/// Only JSON files are opened, through raw storage and enabled outer compression
/// codecs. Probes identify ATIF by a first-record `schema_version` starting with
/// `ATIF-`; empty files do not vote. Full syntax/schema validation belongs to the
/// chosen reader. Resolution does not cache metadata or snapshot source contents.
pub async fn resolve_files<B: ByteAccessor>(
    storage: &B,
    mut request: FileSetRequest,
    policy: &FormatPolicy,
) -> Result<ResolvedFiles> {
    let mut files: Vec<_> = request
        .objects
        .into_iter()
        .filter_map(|object| detect_format(object.locator()).map(|format| (object, format.into())))
        .collect();
    if let Some(preferred) = policy.preferred.map(ResolvedFileFormat::from)
        && files.iter().any(|(_, format)| *format == preferred)
    {
        files.retain(|(_, format)| *format == preferred);
    }
    let (first, mut format) = files.first().cloned().ok_or_else(|| {
        Error::new(
            ErrorKind::NotFound,
            "resolve_files",
            "selection contains no recognized files",
        )
    })?;
    for (object, other) in &files {
        if *other != format {
            return Err(mixed(&first, format, object, *other));
        }
    }
    if let Some(limit) = policy
        .json_probe_bytes
        .filter(|_| format == ResolvedFileFormat::Json)
    {
        let mut first_json = None;
        for (object, _) in &files {
            let Some(detected) = json::detect(storage, object, request.byte_options, limit).await?
            else {
                continue;
            };
            if let Some((previous, kind)) = &first_json {
                if detected != *kind {
                    return Err(mixed(previous, *kind, object, detected));
                }
            } else {
                first_json = Some((object.clone(), detected));
                format = detected;
            }
        }
    }
    request.objects = files.into_iter().map(|(object, _)| object).collect();
    Ok(ResolvedFiles { format, request })
}

fn mixed(
    first: &ObjectRef,
    format: ResolvedFileFormat,
    other: &ObjectRef,
    other_format: ResolvedFileFormat,
) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "resolve_files",
        format!(
            "selection mixes {format:?} file {:?} and {other_format:?} file {:?}",
            first.locator(),
            other.locator()
        ),
    )
}
