//! One book with every advertised original file, without format conversion.

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, ArrayRef, LargeBinaryArray, ListArray, StructArray, cast::AsArray};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, OrderRequirement, ReadRequest,
    RecordCapability, RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, SchemaRef,
    StreamReader, async_trait, read_object, record_batch_with_rows,
};

use super::{error::decode_source, metadata};

/// Metadata schema plus `raw_rdf: LargeBinary` and `files[].data: LargeBinary`.
/// All original files retain their advertised URI, MIME types, extent and order.
/// Binary values are ordinary Arrow values, not Lance blob extension markers.
pub fn book_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let mut fields: Vec<Field> = super::schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            let files = fields.iter_mut().find(|f| f.name() == "files").unwrap();
            let DataType::List(item) = files.data_type() else {
                unreachable!()
            };
            let DataType::Struct(children) = item.data_type() else {
                unreachable!()
            };
            let mut children: Vec<Field> = children.iter().map(|f| f.as_ref().clone()).collect();
            children.push(Field::new("data", DataType::LargeBinary, false));
            *files = Field::new(
                "files",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(children.into()),
                    false,
                ))),
                false,
            );
            fields.push(Field::new("raw_rdf", DataType::LargeBinary, false));
            Arc::new(Schema::new(fields))
        })
        .clone()
}

/// Read one RDF object and all of its advertised files as one Arrow book row.
///
/// `rdf` interprets the request locator. `files` interprets the exact RDF file URI:
/// supply a URL-capable accessor or an explicit mirror mapping. No filename is
/// guessed, MIME type filtered, archive expanded or text encoding changed.
/// Opening is lazy. Projecting out `files` avoids all payload I/O. Each book is
/// one whole-input partition; use file sets for parallel independent books.
pub struct GutenbergBooks<R, F> {
    rdf: Arc<R>,
    files: Arc<F>,
}

impl<R, F> GutenbergBooks<R, F> {
    pub fn new(rdf: R, files: F) -> Self {
        Self {
            rdf: Arc::new(rdf),
            files: Arc::new(files),
        }
    }
}

impl<R, F> Clone for GutenbergBooks<R, F> {
    fn clone(&self) -> Self {
        Self {
            rdf: self.rdf.clone(),
            files: self.files.clone(),
        }
    }
}

#[async_trait]
impl<R, F> Accessor for GutenbergBooks<R, F>
where
    R: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
    F: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        Capability {
            read: self.rdf.capability().read && self.files.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    async fn metadata(&self, request: ReadRequest) -> Result<RecordMetadata> {
        Ok(RecordMetadata {
            schema: request.options.resolve_projection(&book_schema())?.schema,
            source: Some(request.object),
        })
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let output = self.metadata(request.clone()).await?;
        let schema = output.schema.clone();
        let rdf = self.rdf.clone();
        let files = self.files.clone();
        let batches = stream::once(async move {
            let raw = read_object(
                rdf.as_ref(),
                ByteReadRequest::from(&request),
                "read_gutenberg_rdf",
            )
            .await?;
            // Validate every mapped field even when projected away, matching the
            // metadata-only decoder. Preserve unknown fields in the exact RDF.
            let batch =
                metadata::read_metadata(&raw, ReadRequest::new(request.object), super::schema())
                    .await?;
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
            for field in schema.fields() {
                match field.name().as_str() {
                    "raw_rdf" => columns.push(Arc::new(LargeBinaryArray::from_iter_values([
                        raw.as_slice()
                    ]))),
                    "files" => {
                        let list = batch["files"].as_list::<i32>();
                        let entries = list.values().as_struct();
                        let mut payloads = Vec::with_capacity(entries.len());
                        for uri in entries["uri"].as_string::<i32>().iter().flatten() {
                            payloads.push(
                                read_object(
                                    files.as_ref(),
                                    ByteReadRequest::new(uri).with_options(request.byte_options),
                                    "read_gutenberg_file",
                                )
                                .await?,
                            );
                        }
                        let DataType::List(item) = field.data_type() else {
                            unreachable!()
                        };
                        let DataType::Struct(children) = item.data_type() else {
                            unreachable!()
                        };
                        let mut values = entries.columns().to_vec();
                        values.push(Arc::new(LargeBinaryArray::from_iter_values(
                            payloads.iter(),
                        )));
                        let values = StructArray::try_new(children.clone(), values, None)
                            .map_err(decode_source)?;
                        columns.push(Arc::new(
                            ListArray::try_new(
                                item.clone(),
                                list.offsets().clone(),
                                Arc::new(values),
                                None,
                            )
                            .map_err(decode_source)?,
                        ));
                    }
                    name => columns.push(batch[name].clone()),
                }
            }
            record_batch_with_rows(schema, columns, 1).map_err(decode_source)
        });
        Ok(Box::new(StreamReader::records(
            batches,
            output,
            OrderRequirement::Reorderable,
        )))
    }
}

#[async_trait]
impl<R, F> ScanPlanner for GutenbergBooks<R, F>
where
    R: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
    F: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        Ok(ScanPlan::whole_input(self.open(request).await?))
    }
}
