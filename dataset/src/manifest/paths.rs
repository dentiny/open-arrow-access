use super::{ManifestBase, PathsManifest};
use percent_encoding::percent_decode_str;
use url::{Origin, Url};

type PathResult<T> = Result<T, &'static str>;

pub(super) struct Context {
    pub manifest_key: String,
    origin: Option<Origin>,
    base: String,
}
impl Context {
    pub fn new(spec: &PathsManifest) -> PathResult<Self> {
        let configured = spec.origin.as_deref().map(origin).transpose()?;
        let (manifest_origin, manifest_key) = location(spec.manifest.locator())?;
        if let (Some(expected), Some(actual)) = (&configured, &manifest_origin)
            && expected != actual
        {
            return Err("manifest URL does not match configured origin");
        }
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
            origin: manifest_origin.or(configured),
            base,
        })
    }
    pub fn resolve(&self, value: &str) -> PathResult<String> {
        let (origin, mut key) = location(value)?;
        if let Some(origin) = origin {
            if self.origin.as_ref() != Some(&origin) {
                return Err("absolute URL origin is not allowed");
            }
        } else if !value.starts_with('/') {
            key = format!("{}{key}", self.base);
        }
        if !key.starts_with(&self.base) {
            return Err("path escapes the configured base");
        }
        Ok(key)
    }
}
// Manifests and their entries use the same URL-to-storage-key rules.
fn location(value: &str) -> PathResult<(Option<Origin>, String)> {
    if value.contains(':') {
        let url = http_url(value)?;
        let key = key_path(url.path())?;
        Ok((Some(url.origin()), key))
    } else {
        Ok((None, key_path(value)?))
    }
}
fn origin(value: &str) -> PathResult<Origin> {
    let url = http_url(value)?;
    if url.path() != "/" {
        return Err("expected an HTTP(S) origin without path, query or fragment");
    }
    Ok(url.origin())
}
fn http_url(value: &str) -> PathResult<Url> {
    validate_raw(value)?;
    let url = Url::parse(value).map_err(|_| "invalid URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("expected an HTTP(S) URL without credentials");
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("query and fragment are forbidden");
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
