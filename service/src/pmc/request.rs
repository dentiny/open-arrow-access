use open_arrow_access_core::{
    ByteReadOptions, ListRequest, ObjectRef, ReadOptions, ReadOptionsProvider,
};
use std::num::NonZeroUsize;

/// Explicit metadata objects or streaming discovery beneath a directory prefix.
#[derive(Clone, Debug)]
pub enum PmcInput {
    /// Caller order and duplicate entries are retained. Empty lists are valid.
    Files(Vec<ObjectRef>),
    /// Only `.json` files are read, in backend listing order.
    Prefix(ListRequest),
}

/// Limits apply to bytes exposed by the supplied accessor, after any decoding.
#[derive(Clone, Copy, Debug)]
pub struct PmcLimits {
    /// Hard metadata-object limit; default 4 MiB.
    pub metadata_bytes: NonZeroUsize,
    /// Soft batch payload target (raw + normalized metadata); default 64 MiB.
    /// Checked after each row. A batch can exceed it by one bounded article.
    /// This is not a process-memory limit: input chunks and Arrow conversion
    /// buffers require additional memory.
    pub batch_bytes: NonZeroUsize,
}

impl Default for PmcLimits {
    fn default() -> Self {
        Self {
            metadata_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
            batch_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
        }
    }
}

/// Select PMC metadata and configure the final Arrow output.
#[derive(Clone, Debug)]
pub struct PmcRequest {
    pub input: PmcInput,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
    pub limits: PmcLimits,
}

impl PmcRequest {
    pub fn new(metadata: impl Into<ObjectRef>) -> Self {
        Self::files([metadata.into()])
    }

    pub fn files(objects: impl IntoIterator<Item = impl Into<ObjectRef>>) -> Self {
        Self {
            input: PmcInput::Files(objects.into_iter().map(Into::into).collect()),
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
            limits: PmcLimits::default(),
        }
    }

    /// Recursively discover metadata under a bucket-relative directory.
    /// Use `"metadata/"` for the public bucket's metadata collection.
    pub fn prefix(prefix: impl Into<ObjectRef>) -> Self {
        Self {
            input: PmcInput::Prefix(ListRequest::new(prefix)),
            ..Self::files(Vec::<ObjectRef>::new())
        }
    }

    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }

    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.byte_options = options;
        self
    }

    pub fn with_limits(mut self, limits: PmcLimits) -> Self {
        self.limits = limits;
        self
    }
}

impl ReadOptionsProvider for PmcRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}
