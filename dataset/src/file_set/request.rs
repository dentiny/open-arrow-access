use open_arrow_access_core::{ByteReadOptions, ObjectRef, ReadOptions, ReadOptionsProvider};

/// Files in the caller's declared order. Duplicate selections are retained.
#[derive(Clone, Debug)]
pub struct FileSetRequest {
    pub objects: Vec<ObjectRef>,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}

impl FileSetRequest {
    pub fn new(objects: impl IntoIterator<Item = impl Into<ObjectRef>>) -> Self {
        Self {
            objects: objects.into_iter().map(Into::into).collect(),
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
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
}

impl ReadOptionsProvider for FileSetRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }

    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}
