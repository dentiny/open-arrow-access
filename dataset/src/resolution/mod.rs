//! Resolve file membership and one decoder family before schema inspection or reading.
mod json;

use std::num::NonZeroUsize;

use open_arrow_access_core::{ByteAccessor, Error, ErrorKind, ObjectRef, Result};
pub use open_arrow_access_file_format::{DetectedFormat, detect_format};

use crate::{
    directory::{DirectoryRequest, resolve_directory},
    file_set::FileSetRequest,
};

/// Decoder family shared by all selected files. CSV/TSV and JSON/JSONL are compatible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedFileFormat {
    Parquet,
    Delimited,
    Json,
    /// JSON objects whose first record has a `schema_version` starting with `ATIF-`.
    /// Use `AtifFormat::Auto` to accept JSON documents and JSONL uniformly.
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

/// How much JSON content to inspect when choosing between JSON and ATIF readers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum JsonDetection {
    /// Leave JSON filenames as JSON without opening any files.
    Disabled,
    /// Inspect through the first nonempty file, trusting later files to match.
    FirstNonEmpty,
    /// Inspect every file; reject JSON/ATIF mixtures. Empty files do not vote.
    #[default]
    EveryFile,
}

/// Selection rules independent of storage, format features, and schema options.
#[derive(Clone, Debug)]
pub struct FormatPolicy {
    /// Recognized filename formats; disallowed and unknown files are ignored.
    pub allowed: Vec<DetectedFormat>,
    /// First present family wins and excludes other families. Empty by default:
    /// mixed families then fail. For directories with JSON sidecars, opt into
    /// `[ResolvedFileFormat::Parquet]` rather than silently dropping files by default.
    /// Preferences are applied before JSON probing; `Atif` cannot be a preference.
    pub preferred: Vec<ResolvedFileFormat>,
    pub json_detection: JsonDetection,
    /// Maximum decoded bytes retained per JSON probe. Default: 1 MiB.
    pub max_probe_bytes: NonZeroUsize,
}

impl Default for FormatPolicy {
    fn default() -> Self {
        Self {
            allowed: vec![
                DetectedFormat::Parquet,
                DetectedFormat::Csv,
                DetectedFormat::Tsv,
                DetectedFormat::Json,
                DetectedFormat::JsonLines,
                DetectedFormat::Warc,
                DetectedFormat::Xml,
            ],
            preferred: Vec::new(),
            json_detection: JsonDetection::EveryFile,
            max_probe_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        }
    }
}

/// An explicit ordered list, or a directory/glob using the existing discovery rules.
#[derive(Clone, Debug)]
pub enum FileSelection {
    Files(FileSetRequest),
    Directory(DirectoryRequest),
}
impl From<FileSetRequest> for FileSelection {
    fn from(request: FileSetRequest) -> Self {
        Self::Files(request)
    }
}
impl From<DirectoryRequest> for FileSelection {
    fn from(request: DirectoryRequest) -> Self {
        Self::Directory(request)
    }
}

/// A reusable selection. Pass `request` to a FileSetAccessor for `format`.
/// Resolution is not a snapshot or a cache of schema/footer/inference metadata.
#[derive(Clone, Debug)]
pub struct ResolvedFiles {
    pub format: ResolvedFileFormat,
    /// Original locators and read/byte options, with only membership filtered.
    /// Explicit selections preserve order and duplicates; directories sort/deduplicate.
    pub request: FileSetRequest,
    /// Unknown, disallowed, or excluded-by-preference objects.
    pub ignored: Vec<ObjectRef>,
}

/// Resolve one decoder family without decoding Arrow batches or creating tasks.
///
/// Reuses `detect_format` (including compression suffixes), and directory/glob
/// discovery. Non-JSON files are never opened. JSON probing reads at most
/// `max_probe_bytes` decoded bytes into its probe buffer per file, sequentially,
/// then drops the reader. Storage chunk sizes are hints; an upstream reader may
/// still return a larger chunk. Supply raw storage: probes decompress one outer
/// suffix through the default CompressionFactory; enable the needed codecs separately. Codec
/// support and full format/schema validation remain the chosen reader's job.
///
/// JSON/ATIF mixtures fail even when both use JSON-compatible syntax. Empty JSON
/// files are retained but do not influence the family; all-empty JSON selects Json.
/// All other mixed families fail with example locators unless a preference selects
/// one family. An empty recognized selection returns NotFound. XML still requires
/// an explicit record path when constructing its reader.
pub async fn resolve_files<B: ByteAccessor>(
    storage: &B,
    selection: impl Into<FileSelection>,
    policy: &FormatPolicy,
) -> Result<ResolvedFiles> {
    if policy.preferred.contains(&ResolvedFileFormat::Atif) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "resolve_files",
            "ATIF is detected from JSON content and cannot be a filename preference",
        ));
    }
    let mut request = match selection.into() {
        FileSelection::Files(request) => request,
        FileSelection::Directory(request) => resolve_directory(storage, request).await?,
    };
    let mut recognized = Vec::new();
    let mut ignored = Vec::new();
    for object in request.objects {
        match detect_format(object.locator()).filter(|format| policy.allowed.contains(format)) {
            Some(format) => recognized.push((object, ResolvedFileFormat::from(format))),
            None => ignored.push(object),
        }
    }
    let preferred = policy
        .preferred
        .iter()
        .find(|preferred| recognized.iter().any(|(_, format)| format == *preferred));
    if let Some(preferred) = preferred {
        recognized.retain(|(object, format)| {
            if format == preferred {
                true
            } else {
                ignored.push(object.clone());
                false
            }
        });
    }
    let Some((first_object, mut format)) = recognized.first().cloned() else {
        return Err(Error::new(
            ErrorKind::NotFound,
            "resolve_files",
            "selection contains no allowed file formats",
        ));
    };
    for (object, other) in &recognized {
        if *other != format {
            return Err(mixed(&first_object, format, object, *other));
        }
    }
    if format == ResolvedFileFormat::Json && policy.json_detection != JsonDetection::Disabled {
        let mut first = None;
        for (object, _) in &recognized {
            let Some(detected) = json::detect(
                storage,
                object,
                request.byte_options,
                policy.max_probe_bytes,
            )
            .await?
            else {
                continue;
            };
            if let Some((previous, previous_format)) = &first {
                if detected != *previous_format {
                    return Err(mixed(previous, *previous_format, object, detected));
                }
            } else {
                first = Some((object.clone(), detected));
                format = detected;
            }
            if policy.json_detection == JsonDetection::FirstNonEmpty {
                break;
            }
        }
    }
    request.objects = recognized.into_iter().map(|(object, _)| object).collect();
    Ok(ResolvedFiles {
        format,
        request,
        ignored,
    })
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
