use super::{ManifestBase, Mismatch, PathsManifest, detect_format, error::PathResult};
use open_arrow_access_core::{ErrorKind, Result};
use percent_encoding::percent_decode_str;
use url::Url;

pub(super) struct Resolved {
    /// Canonical output key or foreign URL, including query/fragment.
    pub locator: String,
    /// Decoded storage key used for prefix and format selection.
    pub path: String,
}

pub(super) struct Context {
    /// Validated, percent-decoded key used to open the manifest from the storage root.
    pub manifest_key: String,
    /// Manifest or configured URL whose origin identifies same-origin entries.
    origin: Option<Url>,
    /// Decoded directory prefix prepended to relative entries and required for all resolved keys.
    /// Empty for the storage root; otherwise ends with `/` (for example, `lists/`).
    base: String,
}
impl Context {
    pub fn new(spec: &PathsManifest) -> PathResult<Self> {
        let configured = spec.origin.as_deref().map(origin).transpose()?;
        let (manifest_origin, manifest_key) = if spec.manifest.locator().contains(':') {
            validate_raw(spec.manifest.locator())?;
            let url = http_url(spec.manifest.locator())?;
            if configured
                .as_ref()
                .is_some_and(|o| o.origin() != url.origin())
            {
                return Err("manifest URL does not match configured origin");
            }
            if url.query().is_some() || url.fragment().is_some() {
                return Err("manifest URL cannot contain query or fragment");
            }
            (Some(url.clone()), key_path(url.path())?)
        } else {
            if spec.manifest.locator().contains(['?', '#']) {
                return Err("manifest key cannot contain query or fragment");
            }
            (configured, key_path(spec.manifest.locator())?)
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
        Ok(Self {
            manifest_key,
            origin: manifest_origin,
            base,
        })
    }
    pub fn resolve(&self, value: &str) -> PathResult<Resolved> {
        validate_raw(value)?;
        if value.starts_with("//") {
            return Err("network-path references are forbidden");
        }
        let absolute = value.split(['?', '#']).next().unwrap().contains(':');
        let (key, suffix, foreign) = if absolute {
            let url = http_url(value)?;
            let same = self
                .origin
                .as_ref()
                .is_some_and(|o| o.origin() == url.origin());
            let key = key_path(url.path())?;
            let suffix = &url[url::Position::AfterPath..];
            (key, suffix.to_owned(), (!same).then_some(url))
        } else {
            let split = value.find(['?', '#']).unwrap_or(value.len());
            let path = &value[..split];
            let key = key_path(path)?;
            let key = if path.starts_with('/') {
                key
            } else {
                format!("{}{key}", self.base)
            };
            (key, value[split..].to_owned(), None)
        };
        if !key.starts_with(&self.base) {
            return Err("path escapes the configured base");
        }
        let locator = if let Some(mut url) = foreign {
            url.set_path(&format!("/{key}"));
            url.into()
        } else {
            format!("{key}{suffix}")
        };
        Ok(Resolved { locator, path: key })
    }
}
fn origin(value: &str) -> PathResult<Url> {
    validate_raw(value)?;
    let url = http_url(value)?;
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err("expected an HTTP(S) origin without path, query or fragment");
    }
    Ok(url)
}
fn http_url(value: &str) -> PathResult<Url> {
    let url = Url::parse(value).map_err(|_| "invalid URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("expected an HTTP(S) URL without credentials");
    }
    Ok(url)
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
