use open_arrow_access_core::Result;
use std::time::{Duration, SystemTime};

use super::error::invalid_origin;
use url::Url;

pub(crate) fn validate_origin(url: &Url, operation: &'static str) -> Result<()> {
    if !is_http_endpoint(url) || url.path() != "/" {
        return Err(invalid_origin(operation));
    }
    Ok(())
}

/// Shared endpoint rules; callers decide whether a path is allowed.
pub(crate) fn is_http_endpoint(url: &Url) -> bool {
    is_http_resource_url(url) && url.query().is_none()
}

/// Credential-free HTTP(S) resource with a host and no fragment. Paths and
/// signed queries are allowed; endpoint callers impose their additional rules.
pub(crate) fn is_http_resource_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}

pub(super) fn retry_after(headers: &http::HeaderMap, now: SystemTime) -> Option<Duration> {
    let value = headers
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        // Overflow is an excessive server delay, not permission to retry early.
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    Some(
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(now)
            .unwrap_or_default(),
    )
}

pub(super) fn bearer_header(token: &str, operation: &'static str) -> Result<http::HeaderValue> {
    let mut value = http::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|source| super::error::invalid_token(operation, source))?;
    value.set_sensitive(true);
    Ok(value)
}

pub(super) fn is_retry_safe_method(method: &http::Method) -> bool {
    matches!(*method, http::Method::GET | http::Method::HEAD)
}
