//! One book with every advertised original file, without format conversion.

use open_arrow_access_core::blob_bytes_field;
use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{Array, ArrayRef, LargeBinaryArray, ListArray, StructArray, cast::AsArray};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, DocumentAccessor, DocumentDecoder,
    ReadRequest, RecordBatch, RecordCapability, Result, SchemaRef, async_trait, read_object,
    record_batch_with_rows,
};

use super::{GutenbergDecoder, error::decode_source, metadata};

/// Metadata schema plus `raw_rdf: LargeBinary` and `files[].data: LargeBinary`.
/// All original files retain their advertised URI, MIME types, extent and order.
/// Both payload leaves carry `open-arrow-sink:blob=bytes`. Their Arrow type stays
/// LargeBinary; the sink owns conversion to its native blob representation.
pub fn book_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let mut fields = super::schema().fields().to_vec();
            let files = fields.iter_mut().find(|f| f.name() == "files").unwrap();
            let DataType::List(item) = files.data_type() else {
                unreachable!()
            };
            let DataType::Struct(children) = item.data_type() else {
                unreachable!()
            };
            let mut children = children.to_vec();
            children.push(Arc::new(blob_bytes_field(Field::new(
                "data",
                DataType::LargeBinary,
                false,
            ))));
            *files = Arc::new(Field::new_list(
                "files",
                Field::new_struct("item", children, false),
                false,
            ));
            fields.push(Arc::new(blob_bytes_field(Field::new(
                "raw_rdf",
                DataType::LargeBinary,
                false,
            ))));
            Arc::new(Schema::new(fields))
        })
        .clone()
}

/// Whole-book view using the shared document reader's lazy I/O and scan plan.
/// Construct with `GutenbergBooks::new(rdf, GutenbergBookDecoder::new(files))`.
pub type GutenbergBooks<R, F> = DocumentAccessor<R, GutenbergBookDecoder<F>>;

/// Decode one RDF document and retain all its advertised original files.
/// File storage interprets each exact URI; use a URL-capable accessor or an
/// explicit mirror mapping. No formats are filtered or decoded. Projecting out
/// `files` skips payload I/O. The complete book is buffered before emission.
pub struct GutenbergBookDecoder<F> {
    files: F,
    file_concurrency: NonZeroUsize,
}

impl<F> GutenbergBookDecoder<F> {
    pub fn new(files: F) -> Self {
        Self {
            files,
            file_concurrency: NonZeroUsize::MIN,
        }
    }

    /// Bound simultaneous whole-file reads within each book (default: one).
    ///
    /// Advertised order, duplicate entries, bytes and strict errors are retained.
    /// This limit multiplies with the caller's concurrent book reads and does
    /// not limit retained bytes: the complete book still fits in one Arrow row.
    pub fn with_file_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.file_concurrency = concurrency;
        self
    }
}

#[async_trait]
impl<F> DocumentDecoder for GutenbergBookDecoder<F>
where
    F: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    type Request = ReadRequest;

    fn capability(&self, input: Capability) -> RecordCapability {
        let mut capability = GutenbergDecoder.capability(input);
        capability.read &= self.files.capability().read;
        capability
    }

    fn prepare(&self, _request: &ReadRequest) -> Result<SchemaRef> {
        Ok(book_schema())
    }

    async fn decode(
        &self,
        raw: &[u8],
        request: ReadRequest,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        // Validate all mapped fields, even when projected away. Exact RDF retains
        // properties outside the typed mapping.
        let batch =
            metadata::read_metadata(raw, ReadRequest::new(request.object), super::schema()).await?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            columns.push(match field.name().as_str() {
                "raw_rdf" => Arc::new(LargeBinaryArray::from_iter_values([raw])),
                "files" => {
                    let list = batch["files"].as_list::<i32>();
                    let entries = list.values().as_struct();
                    let uris = entries["uri"].as_string::<i32>();
                    // Boxing lets async_trait prove the borrowed read stream is Send.
                    let mut reads = stream::iter(uris.iter().flatten())
                        .map(|uri| {
                            read_object(
                                &self.files,
                                ByteReadRequest::new(uri).with_options(request.byte_options),
                                "read_gutenberg_file",
                            )
                        })
                        .buffered(self.file_concurrency.get())
                        .boxed();
                    let mut payloads = LargeBinaryBuilder::new();
                    while let Some(payload) = reads.try_next().await? {
                        // Release each source buffer after appending instead of
                        // retaining a second full-book copy until array creation.
                        payloads.append_value(payload);
                    }
                    let mut fields = entries.fields().to_vec();
                    fields.push(Arc::new(blob_bytes_field(Field::new(
                        "data",
                        DataType::LargeBinary,
                        false,
                    ))));
                    let mut values = entries.columns().to_vec();
                    values.push(Arc::new(payloads.finish()));
                    let values =
                        StructArray::try_new(fields.into(), values, None).map_err(decode_source)?;
                    let item = Arc::new(Field::new("item", values.data_type().clone(), false));
                    Arc::new(
                        ListArray::try_new(item, list.offsets().clone(), Arc::new(values), None)
                            .map_err(decode_source)?,
                    )
                }
                name => batch[name].clone(),
            });
        }
        record_batch_with_rows(schema, columns, 1).map_err(decode_source)
    }
}
