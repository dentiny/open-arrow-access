use indexmap::{IndexMap, map::Entry};

use open_arrow_access_core::{Error, ErrorKind, Result};

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
            return Err(invalid_names());
        }
        match indexed.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(object);
            }
            Entry::Occupied(_) => return Err(invalid_names()),
        }
    }
    Ok(indexed)
}

fn invalid_names() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_named_files",
        "empty or duplicate filename",
    )
}
