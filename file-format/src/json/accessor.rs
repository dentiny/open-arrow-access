use super::JsonFormat;
use crate::FileAccessor;
use open_arrow_access_core::SchemaRef;

/// Read JSON object streams through the shared file accessor. See [`JsonFormat`].
pub type JsonAccessor<B> = FileAccessor<B, JsonFormat>;

impl<B> FileAccessor<B, JsonFormat> {
    pub fn new(inner: B, schema: SchemaRef) -> Self {
        Self::from_format(inner, JsonFormat::new(schema))
    }
}
