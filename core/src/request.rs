use std::num::NonZeroUsize;

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
    /// Copy a borrowed locator into an owned object reference.
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ObjectRef {
    /// Take ownership of a locator without interpreting its backend-specific syntax.
    fn from(value: String) -> Self {
        Self::new(value)
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

impl Default for ReadOptions {
    /// Select all fields with a target batch size of 8192 rows.
    fn default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(8192).unwrap(),
            projection: None,
        }
    }
}

/// Access to common read settings on a provider-specific record request.
///
/// Record layers can use this contract with file, dataset, and service requests
/// instead of depending on one concrete request type. A layer that changes an
/// inner projection remains responsible for its own declared output selection.
#[deny(missing_docs)]
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
}

impl ReadRequest {
    pub fn new(object: impl Into<ObjectRef>) -> Self {
        Self {
            object: object.into(),
            options: ReadOptions::default(),
        }
    }

    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }
}

impl ReadOptionsProvider for ReadRequest {
    /// Borrow the single-file request's common read settings.
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }

    /// Expose common settings for a layer to adjust before opening the file.
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}
