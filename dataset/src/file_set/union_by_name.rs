use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, FieldNameIndex, ReadRequest, RecordCapability, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, map_record_reader,
};

use super::{
    FileSetRequest,
    accessor::FilePlanner,
    alignment::AlignmentPlan,
    error::empty_selection,
    schema,
    validation::{validate_fields, validate_file_ordering, validate_file_policy},
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
    let mut union_schema = schema::SchemaUnion::default();
    let mut ordering = inner.capability().ordering;
    // Drop each inspected reader/plan before opening the next file, so JSON
    // samples and native partitions are not retained across the entire dataset.
    for request in request.into_requests() {
        let inspected = open_plan(inner.clone(), file_planner, request.clone()).await?;
        validate_file_policy(&inspected)?;
        ordering = ordering.combine(inspected.ordering);
        let source = inspected.metadata.schema;
        validate_fields(source.fields())?;
        union_schema.merge(source.as_ref())?;
        files.push((request, source));
    }
    if files.is_empty() {
        return Err(empty_selection("union_by_name"));
    }
    let union_schema = Arc::new(union_schema.finish());
    let (target, _) = options.resolve_projection(&union_schema)?;
    let metadata = RecordMetadata {
        schema: target.clone(),
        source: None,
    };
    let selected = options
        .projection
        .as_ref()
        .map(|_| FieldNameIndex::new(target.fields()));
    let files = files
        .into_iter()
        .map(|(mut request, source)| {
            // Preflight checks every field, but execution reads only selected
            // fields present in this file. Alignment supplies missing columns
            // and restores the requested output order.
            let source = if let Some(selected) = &selected {
                request.options.projection = Some(
                    source
                        .fields()
                        .iter()
                        .filter(|field| selected.get(field.name()).is_some())
                        .map(|field| field.name().clone())
                        .collect(),
                );
                request.options.resolve_projection(&source)?.0
            } else {
                source
            };
            Ok((
                request,
                Arc::new(AlignmentPlan::new(source, target.clone())?),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let partitions = stream::iter(files)
        .then(move |(request, alignment)| {
            let inner = inner.clone();
            async move {
                let plan = open_plan(inner, file_planner, request).await?;
                validate_file_policy(&plan)?;
                validate_file_ordering(ordering, plan.ordering)?;
                super::validation::validate_schema(&plan.metadata.schema, &alignment.source)?;
                Ok(plan.partitions.map(move |partition| {
                    let partition = partition?;
                    let alignment = alignment.clone();
                    let aligned: ScanPartition =
                        Box::pin(async move { align_reader(partition.await?, alignment) });
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
    alignment: Arc<AlignmentPlan>,
) -> Result<Box<dyn RecordReader>> {
    super::validation::validate_schema(&reader.metadata().schema, &alignment.source)?;
    let capability = RecordCapability::sequential(reader.capability().ordering);
    let schema = alignment.target.clone();
    Ok(Box::new(map_record_reader(
        reader,
        schema,
        capability,
        move |batch| alignment.apply(batch),
    )))
}
