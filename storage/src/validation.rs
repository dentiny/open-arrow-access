use indexmap::{IndexMap, map::Entry};

use crate::error;
use open_arrow_access_core::Result;

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
