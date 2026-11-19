use open_arrow_access_core::Result;
use percent_encoding::percent_decode_str;

use super::{HuggingFaceDatasetRequest, HuggingFaceRepository, error, validation};

impl HuggingFaceDatasetRequest {
    /// Parse a fully qualified `hf://datasets/owner/repository@revision/config/split` URI.
    ///
    /// This is a pure parser: no credentials are loaded and no network I/O occurs.
    /// A revision is required; `~parquet` expands to `refs/convert/parquet`.
    /// Percent-encoded UTF-8 components are decoded once. Encode revision slashes
    /// as `%2F`; config and split must each name one directory. One trailing slash
    /// is accepted. Query strings, fragments, credentials and file paths are rejected.
    /// Partial exports still require [`Self::with_partial_exports`] before opening.
    ///
    /// ```
    /// use open_arrow_access_service::huggingface::HuggingFaceDatasetRequest;
    /// let request = HuggingFaceDatasetRequest::try_from_uri(
    ///     "hf://datasets/owner/example@~parquet/default/train/",
    /// )?;
    /// assert_eq!(request.revision, "refs/convert/parquet");
    /// # Ok::<(), open_arrow_access_core::Error>(())
    /// ```
    pub fn try_from_uri(locator: &str) -> Result<Self> {
        // Parse raw components rather than a URL-normalized path: dot segments
        // must be rejected rather than silently selecting a different split.
        if locator.chars().any(char::is_whitespace) {
            return Err(error::invalid_dataset_uri());
        }
        let path = locator
            .strip_prefix("hf://datasets/")
            .filter(|path| !path.contains(['?', '#', '\\']))
            .ok_or_else(error::invalid_dataset_uri)?;
        let path = path.strip_suffix('/').unwrap_or(path);
        let parts: Vec<_> = path.split('/').collect();
        let [owner, repository, config, split] = parts.as_slice() else {
            return Err(error::invalid_dataset_uri());
        };
        let (repository, revision) = repository
            .split_once('@')
            .ok_or_else(error::invalid_dataset_uri)?;
        let owner = component(owner, false)?;
        let repository = component(repository, false)?;
        let revision = component(revision, true)?;
        let config = component(config, false)?;
        let split = component(split, false)?;
        let request = Self::new(format!("{owner}/{repository}"), config, split).with_revision(
            if revision == "~parquet" {
                "refs/convert/parquet".to_owned()
            } else {
                revision
            },
        );
        validation::validate_repository(
            &HuggingFaceRepository::new(&request.repository).with_revision(&request.revision),
        )?;
        Ok(request)
    }
}

fn component(value: &str, allow_slashes: bool) -> Result<String> {
    // percent_decode_str intentionally accepts malformed escapes; URI input does not.
    for (index, byte) in value.bytes().enumerate() {
        if byte == b'%'
            && !value
                .as_bytes()
                .get(index + 1..index + 3)
                .is_some_and(|digits| digits.iter().all(u8::is_ascii_hexdigit))
        {
            return Err(error::invalid_dataset_uri());
        }
    }
    let decoded = percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| error::invalid_dataset_uri())?
        .into_owned();
    if decoded.is_empty()
        || decoded.trim() != decoded
        || decoded.chars().any(char::is_control)
        || decoded.contains(['\\', '@'])
        || (!allow_slashes && decoded.contains('/'))
        || decoded
            .split('/')
            .any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(error::invalid_dataset_uri());
    }
    Ok(decoded)
}
