use open_arrow_access_core::{ByteReadOptions, ReadOptions, ReadOptionsProvider, Result};

/// One or more complete `ocr/item-<id>/` or `ocr/part-<id>/` directories.
/// Original spelling and zero padding are retained for storage access.
#[derive(Clone, Debug)]
pub struct BhlShardReadRequest {
    pub(super) prefixes: Vec<String>,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}

impl BhlShardReadRequest {
    /// Validate a natural shard prefix without performing I/O.
    pub fn new(prefix: impl Into<String>) -> Result<Self> {
        Self::from_prefixes([prefix.into()])
    }

    /// Select complete directories in caller order, sharing one metadata scan.
    /// Reject empty lists, duplicate paths and malformed prefixes before I/O.
    /// Original spelling and zero padding are preserved; numeric aliases are
    /// distinct storage paths and are not silently deduplicated.
    pub fn from_prefixes(prefixes: impl IntoIterator<Item = impl Into<String>>) -> Result<Self> {
        let prefixes = prefixes.into_iter().map(Into::into).collect::<Vec<_>>();
        super::validation::validate_prefixes(&prefixes)?;
        Ok(Self {
            prefixes,
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        })
    }

    /// First selected directory, retained for existing single-directory callers.
    /// Use `prefixes()` to inspect the complete selection.
    pub fn prefix(&self) -> &str {
        &self.prefixes[0]
    }

    pub fn prefixes(&self) -> &[String] {
        &self.prefixes
    }

    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }

    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.byte_options = options;
        self
    }
}

impl ReadOptionsProvider for BhlShardReadRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }

    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }

    fn byte_read_options_mut(&mut self) -> Option<&mut ByteReadOptions> {
        Some(&mut self.byte_options)
    }
}
