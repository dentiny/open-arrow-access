use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ReadRequest, RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan,
    SchemaRef, StreamReader, into_stream,
};

use super::{
    FileSetRequest,
    accessor::FilePlanner,
    error::{empty_selection, schema_mismatch},
    schema,
    validation::{validate_file_ordering, validate_file_policy},
};

pub(super) async fn plan<A>(
    inner: Arc<A>,
    file_planner: Option<FilePlanner<A>>,
    mut request: FileSetRequest,
) -> Result<ScanPlan>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    let options = request.options.clone();
    // A requested column may be absent in one file and present in another.
    request.options.projection = None;
    let mut files = Vec::new();
    let mut union_schema = None;
    let mut ordering = inner.capability().ordering;
    // Drop each inspected reader/plan before opening the next file, so JSON
    // samples and native partitions are not retained across the entire dataset.
    for request in request.into_requests() {
        let inspected = open_plan(inner.clone(), file_planner, request.clone()).await?;
        validate_file_policy(&inspected)?;
        ordering = ordering.combine(inspected.ordering);
        let source = inspected.metadata.schema;
        schema::validate(source.fields())?;
        union_schema = Some(match union_schema {
            None => source.as_ref().clone(),
            Some(ref previous) => schema::union_by_name(previous, source.as_ref())?,
        });
        files.push((request, source));
    }
    let union_schema = Arc::new(union_schema.ok_or_else(|| empty_selection("union_by_name"))?);
    let (target, _) = options.resolve_projection(&union_schema)?;
    let metadata = RecordMetadata {
        schema: target.clone(),
        source: None,
    };
    let partitions = stream::iter(files)
        .then(move |(request, source)| {
            let inner = inner.clone();
            let target = target.clone();
            async move {
                let plan = open_plan(inner, file_planner, request).await?;
                validate_file_policy(&plan)?;
                validate_file_ordering(ordering, plan.ordering)?;
                if plan.metadata.schema != source {
                    return Err(schema_mismatch());
                }
                Ok(plan.partitions.map(move |partition| {
                    let partition = partition?;
                    let source = source.clone();
                    let target = target.clone();
                    let aligned: ScanPartition =
                        Box::pin(async move { align_reader(partition.await?, source, target) });
                    Ok(aligned)
                }))
            }
        })
        .try_flatten();
    Ok(ScanPlan::from_stream(metadata, partitions).with_ordering(ordering))
}

async fn open_plan<A>(
    inner: Arc<A>,
    file_planner: Option<FilePlanner<A>>,
    request: ReadRequest,
) -> Result<ScanPlan>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    match file_planner {
        Some(plan) => plan(inner, request).await,
        None => Ok(ScanPlan::whole_input(inner.open(request).await?)),
    }
}

fn align_reader(
    reader: Box<dyn RecordReader>,
    source: SchemaRef,
    target: SchemaRef,
) -> Result<Box<dyn RecordReader>> {
    if reader.metadata().schema != source {
        return Err(schema_mismatch());
    }
    let ordering = reader.capability().ordering;
    let metadata = RecordMetadata {
        schema: target.clone(),
        source: reader.metadata().source.clone(),
    };
    let batches = into_stream(reader).map(move |batch| {
        let batch = batch?;
        if batch.schema() != source {
            return Err(schema_mismatch());
        }
        schema::align(batch, target.clone())
    });
    Ok(Box::new(StreamReader::records(batches, metadata, ordering)))
}
