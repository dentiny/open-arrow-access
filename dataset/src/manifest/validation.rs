use super::{ManifestBase, Mismatch, PathsManifest, detect_format, error::PathResult};
use open_arrow_access_core::{ErrorKind, Result};
use percent_encoding::percent_decode_str;
use url::Url;

pub(super) struct Resolved {
    /// Canonical output key or absolute URL, including query/fragment.
    pub locator: String,
    /// Decoded storage key used for prefix and format selection.
    pub path: String,
}

pub(super) struct Context {
    /// Validated, percent-decoded key used to open the manifest from the storage root.
    pub manifest_key: String,
    /// Decoded directory prefix prepended to relative entries and required for all resolved keys.
    /// Empty for the storage root; otherwise ends with `/` (for example, `lists/`).
    base: String,
}
impl Context {
    pub fn new(spec: &PathsManifest) -> PathResult<Self> {
        validate_raw(spec.manifest.locator())?;
        let manifest_key = if let Some(url) = parse_http_url(spec.manifest.locator())? {
            if url.query().is_some() || url.fragment().is_some() {
                return Err("manifest URL cannot contain query or fragment");
            }
            key_path(url.path())?
        } else {
            key_path(spec.manifest.locator())?
        };
        let base = match &spec.base {
            ManifestBase::Origin => String::new(),
            ManifestBase::ManifestDirectory => manifest_key
                .rsplit_once('/')
                .map_or(String::new(), |(dir, _)| format!("{dir}/")),
            ManifestBase::Explicit(object) => {
                if object.locator().contains([':', '?', '#']) {
                    return Err("explicit base must be a directory key");
                }
                let base = key_path(object.locator())?;
                format!("{}/", base.trim_end_matches('/'))
            }
        };
        Ok(Self { manifest_key, base })
    }
    pub fn resolve(&self, value: &str) -> PathResult<Resolved> {
        validate_raw(value)?;
        if value.starts_with("//") {
            return Err("network-path references are forbidden");
        }
        let (key, locator) = if let Some(mut url) = parse_http_url(value)? {
            let key = key_path(url.path())?;
            url.set_path(&format!("/{key}"));
            (key, url.into())
        } else {
            let split = value.find(['?', '#']).unwrap_or(value.len());
            let (path, suffix) = value.split_at(split);
            let key = key_path(path)?;
            let key = if path.starts_with('/') {
                key
            } else {
                format!("{}{key}", self.base)
            };
            let locator = format!("{key}{suffix}");
            (key, locator)
        };
        if !key.starts_with(&self.base) {
            return Err("path escapes the configured base");
        }
        Ok(Resolved { locator, path: key })
    }
}
// Only relative references fall back to storage keys; malformed or unsupported
// absolute URLs remain errors. The caller validates raw input before parsing.
fn parse_http_url(value: &str) -> PathResult<Option<Url>> {
    let url = match Url::parse(value) {
        Ok(url) => url,
        Err(url::ParseError::RelativeUrlWithoutBase) => return Ok(None),
        Err(_) => return Err("invalid URL"),
    };
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("expected an HTTP(S) URL without credentials");
    }
    Ok(Some(url))
}
// Validate before URL parsing: WHATWG URL parsing removes dot segments and
// normalizes backslashes. Reject dot segments conservatively, even if contained.
fn validate_raw(value: &str) -> PathResult<()> {
    if value
        .chars()
        .any(|c| c.is_control() || c.is_whitespace() || c == '\\')
    {
        return Err("whitespace, controls and backslashes are forbidden");
    }
    let path = value.split(['?', '#']).next().unwrap();
    for segment in path.split('/') {
        let decoded = percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| "path is not UTF-8")?;
        if decoded == "."
            || decoded == ".."
            || decoded.contains(['/', '\\', '%', '?', '#'])
            || decoded.chars().any(char::is_control)
        {
            return Err("dot segments and encoded path delimiters are forbidden");
        }
    }
    Ok(())
}
fn key_path(value: &str) -> PathResult<String> {
    validate_raw(value)?;
    if value.contains(['?', '#']) {
        return Err("invalid path delimiter");
    }
    let value = value.strip_prefix('/').unwrap_or(value);
    if value.is_empty() || value.starts_with('/') || value.contains("//") {
        return Err("expected a nonempty canonical object key");
    }
    Ok(percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| "path is not UTF-8")?
        .into_owned())
}

impl PathsManifest {
    pub(super) fn context(&self) -> Result<Context> {
        Context::new(self).map_err(|e| self.error(ErrorKind::InvalidInput, 1, e))
    }
    pub(super) fn selected_path(
        &self,
        line: &[u8],
        number: usize,
        context: &Context,
    ) -> Result<Option<String>> {
        let value = std::str::from_utf8(line).map_err(|e| self.invalid_utf8(number, e))?;
        let value = value.strip_suffix('\n').unwrap_or(value);
        let value = value.strip_suffix('\r').unwrap_or(value);
        if value.is_empty() {
            return Ok(None);
        }
        let resolved = context
            .resolve(value)
            .map_err(|e| self.error(ErrorKind::Decode, number, e))?;
        if self
            .prefix
            .as_ref()
            .is_some_and(|p| !resolved.path.starts_with(p))
        {
            return Ok(None);
        }
        if let Some((formats, on_mismatch)) = &self.formats
            && detect_format(&resolved.path).is_none_or(|f| !formats.contains(&f))
        {
            if *on_mismatch == Mismatch::Skip {
                return Ok(None);
            }
            return Err(self.format_mismatch(number));
        }
        Ok(Some(resolved.locator))
    }
}
