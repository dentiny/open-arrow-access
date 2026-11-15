use std::ops::Range;

use http::{HeaderMap, HeaderValue, header};
use open_arrow_access_core::Result;

use super::error;

fn single<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(value)
}

fn identity(headers: &HeaderMap) -> bool {
    !headers.contains_key(header::CONTENT_ENCODING)
        || single(headers, "content-encoding")
            .is_some_and(|value| value.eq_ignore_ascii_case("identity"))
}

pub(super) fn strong_etag(headers: &HeaderMap) -> Option<HeaderValue> {
    let value = single(headers, "etag")?;
    // Weak validators cannot pin exact byte coordinates with If-Match.
    (value.starts_with('"')
        && value.ends_with('"')
        && value.len() >= 2
        && value.as_bytes()[1..value.len() - 1]
            .iter()
            .all(|byte| *byte >= 0x21 && *byte != b'"' && *byte != 0x7f))
    .then(|| headers[header::ETAG].clone())
}

pub(super) fn supports_ranges(headers: &HeaderMap) -> bool {
    identity(headers)
        && single(headers, "accept-ranges").is_some_and(|value| value.eq_ignore_ascii_case("bytes"))
}

pub(super) fn validate_response(
    headers: &HeaderMap,
    range: &Range<u64>,
    length: u64,
    etag: Option<&HeaderValue>,
) -> Result<()> {
    if !identity(headers) {
        return Err(error::decode(
            "HTTP range response changed content encoding",
        ));
    }
    if let Some(etag) = etag
        && single(headers, "etag") != etag.to_str().ok()
    {
        return Err(error::decode(
            "HTTP object version changed during range reads",
        ));
    }
    let parsed = single(headers, "content-range")
        .and_then(|value| value.strip_prefix("bytes "))
        .and_then(|value| value.split_once('/'))
        .and_then(|(bounds, total)| {
            let (start, end) = bounds.split_once('-')?;
            Some((
                start.parse::<u64>().ok()?,
                end.parse::<u64>().ok()?,
                total.parse::<u64>().ok()?,
            ))
        });
    if parsed != Some((range.start, range.end - 1, length)) {
        return Err(error::decode(
            "HTTP Content-Range does not match the requested range and object length",
        ));
    }
    Ok(())
}
