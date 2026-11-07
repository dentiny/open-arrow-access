use open_arrow_access_core::{
    ByteReader, Capability, ReadOptions, ReadRequest, RecordReader, Result, async_trait,
};

/// A single-file Arrow decoder, independent of its byte storage.
///
/// [`crate::FileAccessor`] calls [`Self::prepare`] before opening bytes, then
/// passes the owned input and prepared state to [`Self::open`]. Fixed-schema
/// projection and decoder configuration errors therefore need no storage I/O.
/// Formats that inspect their input resolve their schema during `open`.
#[async_trait]
pub trait FileFormat: Send + Sync {
    /// Per-open configuration or decoder state, owned by this operation.
    type Prepared: Send;

    /// Derive record-accessor capabilities from the upstream byte capabilities.
    /// This query performs no I/O and does not validate an individual request.
    /// The default exposes sequential record reading. Overrides must describe
    /// the record output, without advertising byte ranges or accessor listing.
    /// Validate the actual input capabilities and metadata in [`Self::open`].
    fn capability(&self, input: Capability) -> Capability {
        Capability {
            read: input.read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    /// Validate options and prepare decoder state without input I/O.
    ///
    /// # Errors
    /// Return configuration/projection errors here when the schema is known.
    fn prepare(&self, options: &ReadOptions) -> Result<Self::Prepared>;

    /// Inspect or decode the owned byte input into an independently owned reader.
    /// The reader must expose its schema and the request's source identity when
    /// this completes, preserve logical order, and obey the terminal reader
    /// contract. Format-specific projection, batching and error mapping belong
    /// here. The returned reader cannot borrow this format or its prepared state.
    ///
    /// # Errors
    /// Return unsupported input requirements and inspection/initialization errors.
    async fn open(
        &self,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        prepared: Self::Prepared,
    ) -> Result<Box<dyn RecordReader>>;
}
