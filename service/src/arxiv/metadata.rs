//! Join raw PDFs and common content rows to a previously harvested metadata snapshot.
use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, RecordBatch, StructArray, UInt64Array, cast::AsArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, RecordCapability, RecordReader, Result, ScanPlan,
    ScanPlanner, async_trait, map_record_reader, project_batch, scan_partition,
};

use super::{ArxivReadRequest, ArxivRecords, error, id, record_schema, validation};

/// Reusable in-memory metadata snapshot, indexed by the article ID column.
///
/// All input columns, types and values are retained inside the output `metadata`
/// struct. IDs may be canonical arXiv IDs or OAI identifiers. Duplicate IDs are
/// rejected instead of choosing an arbitrary record. Harvest/persist the snapshot
/// separately under a single rate-limited coordinator, then share this index
/// between shard readers. For large snapshots, load a month at a time.
pub struct ArxivMetadataIndex {
    // Metadata records; each struct row retains all columns from the snapshot.
    pub(super) rows: StructArray,
    // Maps from normalized, unversioned arXiv article IDs (e.g. "2410.01234"
    // or "hep-th9901001", without the OAI prefix or legacy slash) to
    // zero-based row indices in `rows`.
    ids: HashMap<String, u64>,
}

impl ArxivMetadataIndex {
    pub fn try_new(batch: RecordBatch, id_column: &str) -> Result<Self> {
        let column = batch
            .column_by_name(id_column)
            .ok_or_else(error::missing_metadata_id_column)?;
        validation::validate_metadata_id_column(column.as_ref())?;
        let mut ids = HashMap::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let id = match column.data_type() {
                DataType::Utf8 => column.as_string::<i32>().value(row),
                DataType::LargeUtf8 => column.as_string::<i64>().value(row),
                DataType::Utf8View => column.as_string_view().value(row),
                _ => unreachable!(),
            };
            validation::validate_unique_metadata_id(ids.insert(id::metadata_id(id)?, row as u64))?;
        }
        Ok(Self {
            rows: StructArray::from(batch),
            ids,
        })
    }

    /// Build an index directly from the fixed arXivRaw OAI record schema.
    /// Deleted records are retained and join by their OAI header identifier.
    /// Reconcile repeated updates before constructing this immutable snapshot.
    pub fn from_oai_records(batch: RecordBatch) -> Result<Self> {
        validation::validate_oai_metadata(&batch)?;
        let identifiers = batch["oai:header"].as_struct()["oai:identifier"].as_string::<i32>();
        let mut ids = HashMap::with_capacity(batch.num_rows());
        for (row, identifier) in identifiers.iter().enumerate() {
            validation::validate_unique_metadata_id(
                ids.insert(id::metadata_id(identifier.unwrap())?, row as u64),
            )?;
        }
        Ok(Self {
            rows: StructArray::from(batch),
            ids,
        })
    }

    pub(super) fn attach_content(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let indices = batch["article_id"]
            .as_string::<i64>()
            .iter()
            .map(|article| {
                let key = id::metadata_id(article.ok_or_else(error::null_metadata_id)?)?;
                validation::validate_content_metadata_match(self.ids.get(&key).copied(), &key)
            })
            .collect::<Result<Vec<_>>>()?;
        let metadata = arrow_select::take::take(&self.rows, &UInt64Array::from(indices), None)
            .map_err(error::arrow_error)?;
        let mut columns = batch.columns().to_vec();
        let position = batch
            .schema()
            .index_of("metadata")
            .map_err(error::arrow_error)?;
        columns[position] = metadata;
        RecordBatch::try_new(batch.schema(), columns).map_err(error::arrow_error)
    }

    fn attach(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let names = batch
            .column_by_name("entry_name")
            .ok_or_else(error::missing_pdf_entry_column)?
            .as_binary::<i64>();
        let indices = names
            .iter()
            .map(|name| pdf_metadata_row(name, &self.ids))
            .collect::<Result<Vec<_>>>()?;
        let metadata = arrow_select::take::take(&self.rows, &UInt64Array::from(indices), None)
            .map_err(error::arrow_error)?;
        let mut columns = batch.columns().to_vec();
        columns.push(metadata);
        RecordBatch::try_new(self.schema(), columns).map_err(error::arrow_error)
    }

    fn schema(&self) -> Arc<Schema> {
        let mut fields = record_schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            "metadata",
            self.rows.data_type().clone(),
            false,
        )));
        Arc::new(Schema::new(fields))
    }
}

/// PDF rows with a required matching metadata struct in the same batch/table.
pub struct ArxivPdfRecords<A> {
    records: ArxivRecords<A>,
    metadata: Arc<ArxivMetadataIndex>,
}

impl<A> ArxivRecords<A> {
    /// Attach a cached snapshot without making metadata network requests.
    /// This accessor accepts PDF requests only; missing matches fail the scan.
    pub fn with_metadata(self, metadata: Arc<ArxivMetadataIndex>) -> ArxivPdfRecords<A> {
        ArxivPdfRecords {
            records: self,
            metadata,
        }
    }
}

#[async_trait]
impl<A> Accessor for ArxivPdfRecords<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    type Request = ArxivReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.records.capability()
    }

    async fn open(&self, request: ArxivReadRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}

#[async_trait]
impl<A> ScanPlanner for ArxivPdfRecords<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    async fn plan_scan(&self, mut request: ArxivReadRequest) -> Result<ScanPlan> {
        validation::validate_pdf_kind(request.kind)?;
        let projection = request
            .options
            .resolve_projection(&self.metadata.schema())?;
        // Matching requires entry_name, even if callers project it away.
        request.options.projection = None;
        let mut plan = self.records.plan_scan(request).await?;
        plan.metadata.schema = projection.schema.clone();
        let index = self.metadata.clone();
        plan.partitions = plan
            .partitions
            .map(move |partition| {
                let index = index.clone();
                let schema = projection.schema.clone();
                let projection = projection.clone();
                partition.map(|partition| {
                    scan_partition(async move {
                        let reader = partition.await?;
                        let capability = reader.capability();
                        Ok(map_record_reader(
                            reader,
                            schema,
                            capability,
                            move |batch| {
                                let batch = index.attach(batch)?;
                                project_batch(
                                    batch,
                                    projection.indices.as_deref(),
                                    projection.schema.clone(),
                                )
                            },
                        ))
                    })
                })
            })
            .boxed();
        Ok(plan)
    }
}

fn pdf_metadata_row(name: Option<&[u8]>, ids: &HashMap<String, u64>) -> Result<u64> {
    let id = id::pdf_id(name.ok_or_else(error::null_pdf_entry_name)?)?;
    ids.get(&id)
        .copied()
        .ok_or_else(error::missing_pdf_metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arxiv::ArxivContentFormat;
    use arrow_array::{ArrayRef, LargeBinaryArray, StringArray};

    fn snapshot(ids: Vec<&str>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "identifier",
                Arc::new(StringArray::from(ids.clone())) as ArrayRef,
            ),
            (
                "title",
                Arc::new(StringArray::from(vec!["original title"; ids.len()])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn joins_by_id_and_keeps_pdf_bytes_and_all_metadata() {
        let index = ArxivMetadataIndex::try_new(
            snapshot(vec!["oai:arXiv.org:2410.01234", "hep-th/9901001"]),
            "identifier",
        )
        .unwrap();
        let input = RecordBatch::try_from_iter(vec![
            (
                "shard_uri",
                Arc::new(arrow_array::LargeStringArray::from(vec!["shard"; 2])) as ArrayRef,
            ),
            (
                "entry_index",
                Arc::new(UInt64Array::from(vec![0, 1])) as ArrayRef,
            ),
            (
                "entry_name",
                Arc::new(LargeBinaryArray::from(vec![
                    b"2410/hep-th9901001.pdf".as_slice(),
                    b"2410/2410.01234v2.pdf".as_slice(),
                ])) as ArrayRef,
            ),
            (
                "data",
                Arc::new(LargeBinaryArray::from(vec![
                    b"%PDF-one".as_slice(),
                    b"%PDF-two".as_slice(),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        let output = index.attach(input.clone()).unwrap();
        assert_eq!(output.column(3), input.column(3));
        let metadata = output.column(4).as_struct();
        assert_eq!(
            metadata.column(0).as_string::<i32>().value(0),
            "hep-th/9901001"
        );
        assert_eq!(
            metadata.column(1).as_string::<i32>().value(1),
            "original title"
        );
        let missing =
            ArxivMetadataIndex::try_new(snapshot(vec!["2410.99999"]), "identifier").unwrap();
        assert!(missing.attach(input).is_err());
    }

    #[test]
    fn rejects_duplicate_ids_and_invalid_identifiers() {
        assert!(
            ArxivMetadataIndex::try_new(
                snapshot(vec!["hep-th/9901001", "oai:arXiv.org:hep-th/9901001"]),
                "identifier"
            )
            .is_err()
        );
        for id in [
            "",
            "2410.01234v2",
            "ééééé",
            "not-an-id",
            "24/10.01234",
            "2410.01/234",
            "hep-/th9901001",
            "hep-th99/01001",
        ] {
            assert!(ArxivMetadataIndex::try_new(snapshot(vec![id]), "identifier").is_err());
        }
        assert_eq!(id::pdf_id(b"hep-th/9901001.pdf").unwrap(), "hep-th9901001");
        assert_eq!(
            id::pdf_id(b"0001/math.GT0001001v1.pdf").unwrap(),
            "math.GT0001001"
        );
        for name in [
            "2610/2610.01234.pdf",
            "2610/2610.01234.pdf.gz",
            "2610/2610.01234v2.ps.gz",
            "2610/2610.01234.tex.gz",
            "2610/2610.01234.tar.gz",
        ] {
            assert_eq!(
                validation::content_article_id(name.as_bytes(), ArxivContentFormat::Latex,)
                    .unwrap(),
                "2610.01234",
            );
        }
        assert!(
            validation::content_article_id(b"2610/not-an-id.pdf.gz", ArxivContentFormat::Latex,)
                .is_err()
        );
    }
}
