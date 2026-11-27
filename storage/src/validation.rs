use std::{ops::Range, time::Duration};

use http::{HeaderMap, StatusCode, header};

use indexmap::{IndexMap, map::Entry};

use crate::error;
use open_arrow_access_core::Result;
pub(crate) use open_arrow_access_core::validate_byte_range;

pub(crate) fn validate_http_request_interval(interval: Duration) -> Result<()> {
    if interval.is_zero() || tokio::time::Instant::now().checked_add(interval).is_none() {
        return Err(error::invalid_http_request_interval());
    }
    Ok(())
}

pub(crate) fn validate_http_concurrency(limit: usize) -> Result<()> {
    if limit == 0 || limit > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(error::invalid_http_concurrency());
    }
    Ok(())
}

/// Reject empty or duplicate opaque names without interpreting them as paths.
pub fn validate_file_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<()> {
    index_files(names.into_iter().map(|name| (name, ()))).map(|_| ())
}

// Validate while indexing to avoid a second hash table.
pub(crate) fn index_files<K, V>(files: impl IntoIterator<Item = (K, V)>) -> Result<IndexMap<K, V>>
where
    K: AsRef<str> + Eq + std::hash::Hash,
{
    let files = files.into_iter();
    let mut indexed = IndexMap::with_capacity(files.size_hint().0);
    for (name, object) in files {
        if name.as_ref().is_empty() {
            return Err(error::invalid_names());
        }
        match indexed.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(object);
            }
            Entry::Occupied(_) => return Err(error::invalid_names()),
        }
    }
    Ok(indexed)
}

impl crate::StorageConfig {
    /// Validate policies without constructing a backend or performing I/O.
    pub fn validate(&self) -> Result<()> {
        if let Some(retry) = &self.retry {
            if !retry.factor.is_finite() || retry.factor < 1.0 {
                return Err(error::invalid_retry_factor());
            }
            if retry.min_delay > retry.max_delay {
                return Err(error::invalid_retry_delays());
            }
        }
        if let Some(timeout) = &self.timeout
            && (timeout.operation_timeout.is_zero() || timeout.io_timeout.is_zero())
        {
            return Err(error::invalid_timeouts());
        }
        Ok(())
    }
}

pub(crate) fn validate_regular_file(is_file: bool) -> Result<()> {
    if !is_file {
        return Err(crate::error::not_regular_file());
    }
    Ok(())
}

pub(crate) fn validate_listing(supported: bool) -> Result<()> {
    if !supported {
        return Err(crate::error::listing_unsupported());
    }
    Ok(())
}

pub(crate) fn validate_listing_prefix(prefix: &str) -> Result<()> {
    if !prefix.is_empty() {
        return Err(crate::error::invalid_listing_prefix());
    }
    Ok(())
}

pub(crate) fn validate_read_length(
    actual: u64,
    expected: u64,
    operation: &'static str,
) -> Result<()> {
    if actual != expected {
        return Err(crate::error::short_read(operation));
    }
    Ok(())
}

pub(crate) fn validate_chunk_length(actual: u64, remaining: u64) -> Result<()> {
    if actual > remaining {
        return Err(crate::error::input_too_long());
    }
    Ok(())
}

pub(crate) fn range_capacity(expected: u64) -> Result<usize> {
    usize::try_from(expected).map_err(|_| crate::error::unaddressable_range())
}

/// Validate the interval before subtracting its bounds or slicing its payload.
pub(crate) fn validate_prefetched_range(
    range: &Range<u64>,
    actual: u64,
    content_length: u64,
) -> Result<()> {
    validate_byte_range(range, content_length, "prefetch")?;
    validate_read_length(actual, range.end - range.start, "prefetch")
}

pub(crate) fn validate_read_path(path: &str) -> opendal::Result<()> {
    if !opendal::raw::validate_path(path, opendal::EntryMode::FILE) {
        return Err(error::read_path_is_directory());
    }
    Ok(())
}

/// Response handling needed after validating a bounded HTTP prefix request.
pub(crate) enum HttpPrefixResponse {
    Empty,
    Short(u64),
    Unchanged,
}

pub(crate) fn validate_http_prefix_response(
    status: StatusCode,
    headers: &HeaderMap,
    size: u64,
) -> opendal::Result<HttpPrefixResponse> {
    // Only an explicit zero total proves an empty object.
    if status == StatusCode::RANGE_NOT_SATISFIABLE
        && headers
            .get(header::CONTENT_RANGE)
            .is_some_and(|value| value == "bytes */0")
    {
        return Ok(HttpPrefixResponse::Empty);
    }
    let length = if status == StatusCode::PARTIAL_CONTENT {
        let range =
            opendal::raw::parse_content_range(headers)?.ok_or_else(error::invalid_response)?;
        let length = range.size().ok_or_else(error::invalid_response)?;
        if range.range() != Some(0..length.min(size)) {
            return Err(error::invalid_response());
        }
        Some(length)
    } else if status == StatusCode::OK {
        // Accept an ignored Range only if the whole object fits the prefix.
        let length = opendal::raw::parse_content_length(headers)?;
        if !length.is_some_and(|length| length <= size) {
            return Err(error::ignored_bootstrap_range());
        }
        length
    } else {
        // Leave other statuses to the backend's normal error handling.
        None
    };
    Ok(match length.filter(|length| *length < size) {
        Some(length) => HttpPrefixResponse::Short(length),
        None => HttpPrefixResponse::Unchanged,
    })
}

pub(crate) fn validate_prefix_chunk_length(actual: u64, remaining: u64) -> opendal::Result<()> {
    if actual > remaining {
        return Err(error::invalid_response());
    }
    Ok(())
}

pub(crate) fn validate_prefix_read_length(actual: u64, expected: u64) -> opendal::Result<()> {
    if actual != expected {
        return Err(error::invalid_response());
    }
    Ok(())
}
