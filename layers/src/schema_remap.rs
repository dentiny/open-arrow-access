use std::sync::Arc;

use arrow_schema::DataType;
use open_arrow_access_core::{Accessor, Layer, ReadOptionsProvider, RecordReader, Result};

use crate::{TypeConversionAccessor, validation};

/// Remap one top-level field; omitted fields remain unchanged.
#[derive(Clone, Debug)]
pub struct FieldRemap {
    pub source: String,
    pub target: String,
    /// None preserves the source type.
    pub data_type: Option<DataType>,
}

/// Rename top-level fields and optionally cast them with the shared conversion reader.
#[derive(Clone, Debug, Default)]
pub struct SchemaRemapLayer {
    specs: Arc<[FieldRemap]>,
}

impl SchemaRemapLayer {
    pub fn try_new(specs: Vec<FieldRemap>) -> Result<Self> {
        validation::validate_remap_specs(&specs)?;
        Ok(Self {
            specs: specs.into(),
        })
    }
}

pub type SchemaRemapAccessor<A> = TypeConversionAccessor<A>;

impl<A> Layer<A> for SchemaRemapLayer
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Layered = SchemaRemapAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        TypeConversionAccessor::with_remaps(inner, Arc::clone(&self.specs))
    }
}
