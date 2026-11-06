use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

use crate::{Error, ErrorKind, Result, SchemaRef};

/// An opaque object locator interpreted by an accessor, not by a format reader.
/// Filesystem accessors interpret this as a path relative to their configured root.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjectRef(String);

impl ObjectRef {
    pub fn new(locator: impl Into<String>) -> Self {
        Self(locator.into())
    }

    pub fn locator(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ObjectRef {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ObjectRef {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Default row-count target for [`ReadOptions`].
pub const DEFAULT_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(8192).unwrap();

/// Per-reader storage I/O settings, independent of Arrow batch size and ordering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteReadOptions {
    /// Maximum in-flight I/O chunks within a read operation; defaults to one.
    pub io_concurrency: NonZeroUsize,
    /// I/O splitting size. None keeps the backend default; concurrency alone
    /// does not guarantee that a read is split into multiple tasks.
    pub io_chunk_size: Option<NonZeroUsize>,
}

impl Default for ByteReadOptions {
    fn default() -> Self {
        Self {
            io_concurrency: NonZeroUsize::MIN,
            io_chunk_size: None,
        }
    }
}

/// A byte input together with the I/O settings for this open operation.
#[derive(Clone, Debug)]
pub struct ByteReadRequest {
    pub object: ObjectRef,
    pub options: ByteReadOptions,
}

impl ByteReadRequest {
    pub fn new(object: impl Into<ObjectRef>) -> Self {
        Self {
            object: object.into(),
            options: ByteReadOptions::default(),
        }
    }

    pub fn with_options(mut self, options: ByteReadOptions) -> Self {
        self.options = options;
        self
    }
}

impl From<ObjectRef> for ByteReadRequest {
    fn from(object: ObjectRef) -> Self {
        Self::new(object)
    }
}

impl From<&str> for ByteReadRequest {
    fn from(object: &str) -> Self {
        Self::new(object)
    }
}

impl From<String> for ByteReadRequest {
    fn from(object: String) -> Self {
        Self::new(object)
    }
}

/// Common record-reading settings. Providers can expose their own typed settings.
#[derive(Clone, Debug)]
pub struct ReadOptions {
    /// A row-count target, not a guarantee about the size of an individual record.
    pub batch_size: NonZeroUsize,
    /// Top-level fields in the requested output order. None selects every field.
    /// An empty projection preserves row counts with zero columns.
    pub projection: Option<Vec<String>>,
}

impl ReadOptions {
    /// Resolve the output schema and selected top-level indices in caller order.
    /// None retains the original schema; an empty projection retains only row counts.
    /// Unknown or duplicate field names return InvalidInput.
    pub fn resolve_projection(
        &self,
        schema: &SchemaRef,
    ) -> Result<(SchemaRef, Option<Vec<usize>>)> {
        let Some(columns) = &self.projection else {
            return Ok((schema.clone(), None));
        };
        let mut seen = HashSet::new();
        let mut indices = Vec::with_capacity(columns.len());
        for name in columns {
            if !seen.insert(name) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "project",
                    format!("duplicate field {name:?}"),
                ));
            }
            indices.push(schema.index_of(name).map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "project",
                    format!("unknown field {name:?}"),
                )
                .with_source(error)
            })?);
        }
        let projected = schema.project(&indices).map_err(|error| {
            Error::new(ErrorKind::InvalidInput, "project", error.to_string()).with_source(error)
        })?;
        Ok((Arc::new(projected), Some(indices)))
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            projection: None,
        }
    }
}

/// Access to common read settings on a provider-specific record request.
///
/// Record layers can use this contract with file, dataset, and service requests
/// instead of depending on one concrete request type. A layer that changes an
/// inner projection remains responsible for its own declared output selection.
pub trait ReadOptionsProvider: Send {
    /// Borrow the batch target and projection without performing I/O.
    fn read_options(&self) -> &ReadOptions;

    /// Mutably borrow settings used when the request is subsequently opened.
    ///
    /// For example, a generated-column layer can retain its caller's projection
    /// and widen the inner projection to read expression dependencies. Changes
    /// to this request do not reconfigure readers that have already been opened.
    fn read_options_mut(&mut self) -> &mut ReadOptions;
}

/// Request for a single logical file. Dataset and service accessors can define
/// different request types while implementing the same `Accessor` trait.
#[derive(Clone, Debug)]
pub struct ReadRequest {
    pub object: ObjectRef,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}

impl ReadRequest {
    pub fn new(object: impl Into<ObjectRef>) -> Self {
        Self {
            object: object.into(),
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

impl ReadOptionsProvider for ReadRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }

    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}
