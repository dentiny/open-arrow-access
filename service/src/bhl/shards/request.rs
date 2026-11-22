use open_arrow_access_core::{ByteReadOptions, ReadOptions, ReadOptionsProvider, Result};

/// One complete `ocr/item-<id>/` or `ocr/part-<id>/` directory.
/// Original spelling and zero padding are retained for storage access.
#[derive(Clone, Debug)]
pub struct BhlShardReadRequest {
    pub(super) prefix: String,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}

impl BhlShardReadRequest {
    /// Validate a natural shard prefix without performing I/O.
    pub fn new(prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        super::validation::group(&prefix)?;
        Ok(Self {
            prefix,
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        })
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
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
