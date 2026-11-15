use super::{AttachmentFormat, validation};
use crate::{OuterCompression, Result};

/// Format selection and compression surrounding the entire attachment.
#[derive(Clone, Debug)]
pub struct AttachmentOptions {
    pub format: AttachmentFormat,
    pub outer_compression: OuterCompression,
}

impl AttachmentOptions {
    /// Binary and Parquet default to unchanged bytes/ranges; text formats use
    /// suffix-based outer decompression. Enable the required format/codec features.
    pub fn new(format: AttachmentFormat) -> Self {
        let outer_compression = match &format {
            AttachmentFormat::Binary | AttachmentFormat::Parquet => OuterCompression::None,
            _ => OuterCompression::Auto,
        };
        Self {
            format,
            outer_compression,
        }
    }

    pub fn with_outer_compression(mut self, compression: OuterCompression) -> Self {
        self.outer_compression = compression;
        self
    }

    /// Validate configuration and format availability without storage/provider I/O.
    /// Decoder-specific options and projections are validated during binding.
    pub fn validate(&self) -> Result<()> {
        validation::validate(self)
    }
}

impl Default for AttachmentOptions {
    fn default() -> Self {
        Self::new(AttachmentFormat::Binary)
    }
}
