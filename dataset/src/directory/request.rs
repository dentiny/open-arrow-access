use open_arrow_access_core::{ByteReadOptions, ObjectRef, ReadOptions, ReadOptionsProvider};

/// Select files beneath an accessor-defined directory prefix.
///
/// Selected locators are sorted lexicographically and duplicates removed. This
/// is a declared scan order, not an atomic snapshot of a mutable directory.
/// Prefixes and listed locators are backend-defined and passed through unchanged.
#[derive(Clone, Debug)]
pub struct DirectoryRequest {
    pub prefix: ObjectRef,
    /// Traverse child directories during discovery. Defaults to true.
    pub recursive: bool,
    /// Case-sensitive locator suffix, for example `.parquet`. None selects all files.
    pub suffix: Option<String>,
    /// Glob relative to prefix; `*` stays within a component and `**` spans directories.
    /// Combines with suffix filtering and respects the recursive listing setting.
    pub glob: Option<String>,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}

impl DirectoryRequest {
    pub fn new(prefix: impl Into<ObjectRef>) -> Self {
        Self {
            prefix: prefix.into(),
            recursive: true,
            suffix: None,
            glob: None,
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        }
    }

    pub fn with_glob(mut self, pattern: impl Into<String>) -> Self {
        self.glob = Some(pattern.into());
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
}

impl ReadOptionsProvider for DirectoryRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}
