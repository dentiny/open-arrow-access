use open_arrow_access_core::{
    ByteReadOptions, ByteReadRequest, ObjectRef, ReadOptions, ReadOptionsProvider, ReadRequest,
};

/// Map an RDF file URI to a storage locator.
#[derive(Clone, Debug)]
pub struct GutenbergFile {
    pub uri: String,
    pub object: ObjectRef,
}

impl GutenbergFile {
    pub fn new(uri: impl Into<String>, object: impl Into<ObjectRef>) -> Self {
        Self {
            uri: uri.into(),
            object: object.into(),
        }
    }
}

/// Book RDF and files in output order. An empty selection embeds no files.
#[derive(Clone, Debug)]
pub struct GutenbergRequest {
    pub metadata: ReadRequest,
    pub files: Vec<GutenbergFile>,
}

impl GutenbergRequest {
    pub fn new(metadata: impl Into<ObjectRef>, files: Vec<GutenbergFile>) -> Self {
        Self {
            metadata: ReadRequest::new(metadata),
            files,
        }
    }

    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.metadata.options = options;
        self
    }

    /// Apply I/O settings to RDF and file reads.
    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.metadata.byte_options = options;
        self
    }
}

impl ReadOptionsProvider for GutenbergRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.metadata.options
    }

    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.metadata.options
    }
}

impl From<&GutenbergRequest> for ByteReadRequest {
    fn from(request: &GutenbergRequest) -> Self {
        Self::from(&request.metadata)
    }
}
