//! Select one decoder family for a file set before schema inspection or reading.
mod error;
mod json;

use std::num::NonZeroUsize;

use open_arrow_access_core::{ByteAccessor, Error, ErrorKind, Result};
pub use open_arrow_access_file_format::{DetectedFormat, detect_format};

use crate::file_set::FileSetRequest;

/// File selection and bounded JSON/ATIF detection.
#[derive(Clone, Debug)]
pub struct FormatPolicy {
    /// If present, select only this decoder family when it occurs in the input.
    /// For example, Some(Parquet) ignores JSON sidecars. Default: reject mixed families.
    /// Content-only Atif is not a filename preference.
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
    /// Csv covers CSV/TSV; Json covers JSON/JSONL. Atif uses AtifFormat::Auto.
    pub format: DetectedFormat,
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
    if policy.preferred == Some(DetectedFormat::Atif) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "resolve_files",
            "ATIF requires content detection and cannot be a filename preference",
        ));
    }
    let mut files: Vec<_> = request
        .objects
        .into_iter()
        .filter_map(|object| {
            detect_format(object.locator()).map(|format| (object, decoder_format(format)))
        })
        .collect();
    if let Some(preferred) = policy.preferred.map(decoder_format)
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
            return Err(error::mixed(&first, format, object, *other));
        }
    }
    if let Some(limit) = policy
        .json_probe_bytes
        .filter(|_| format == DetectedFormat::Json)
    {
        let mut first_json = None;
        for (object, _) in &files {
            let Some(detected) = json::detect(storage, object, request.byte_options, limit).await?
            else {
                continue;
            };
            if let Some((previous, kind)) = &first_json {
                if detected != *kind {
                    return Err(error::mixed(previous, *kind, object, detected));
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

// Normalize only decoder-compatible aliases; filename detection keeps its distinctions.
fn decoder_format(format: DetectedFormat) -> DetectedFormat {
    match format {
        DetectedFormat::Tsv => DetectedFormat::Csv,
        DetectedFormat::JsonLines => DetectedFormat::Json,
        other => other,
    }
}
