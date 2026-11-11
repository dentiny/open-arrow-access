use open_arrow_access_core::{ByteReadOptions, ReadOptions, ReadOptionsProvider};

/// Read the Parquet export of one Hugging Face dataset config/split.
/// The revision selects the Parquet representation, not the original data branch.
#[derive(Clone, Debug)]
pub struct HuggingFaceDatasetRequest {
    pub repository: String,
    pub revision: String,
    pub config: String,
    pub split: String,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
    /// Permit fallback to `partial-<split>` when the complete export is absent.
    pub allow_partial: bool,
}

impl HuggingFaceDatasetRequest {
    pub fn new(
        repository: impl Into<String>,
        config: impl Into<String>,
        split: impl Into<String>,
    ) -> Self {
        Self {
            repository: repository.into(),
            revision: "refs/convert/parquet".into(),
            config: config.into(),
            split: split.into(),
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
            allow_partial: false,
        }
    }

    /// Select a revision of the Parquet export; commit SHAs avoid a metadata request.
    pub fn with_revision(mut self, revision: impl Into<String>) -> Self {
        self.revision = revision.into();
        self
    }

    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }

    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.byte_options = options;
        self
    }

    /// Explicitly accept an incomplete export if the full split is unavailable.
    pub fn with_partial_exports(mut self) -> Self {
        self.allow_partial = true;
        self
    }
}

impl ReadOptionsProvider for HuggingFaceDatasetRequest {
    fn clone_with_read_options(&self, options: ReadOptions) -> Option<Self> {
        let mut request = self.clone();
        *request.read_options_mut() = options;
        Some(request)
    }

    fn read_options(&self) -> &ReadOptions {
        &self.options
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}
