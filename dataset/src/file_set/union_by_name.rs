use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ReadRequest, RecordCapability, RecordMetadata, RecordReader, Result, ScanPartition,
    ScanPlan, map_record_reader,
};

use super::{
    FileSetRequest,
    accessor::{FilePlanner, METADATA_CONCURRENCY, open_plan},
    alignment::AlignmentPlan,
    error::empty_selection,
    schema,
    validation::validate_fields,
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
    let mut selected = false;
    let mut union_schema = schema::SchemaUnion::default();
    let mut ordering = inner.capability().ordering;
    // Buffer opening/inference, but merge in input order so field order and
    // output ordering do not depend on which file finishes first. Keep each
    // plan: inferred readers own the sample that must be replayed during reads.
    let mut inspected = stream::iter(request.into_requests())
        .map(|request| {
            let inner = inner.clone();
            async move {
                let plan = open_plan(inner.clone(), file_planner, request.clone()).await?;
                let empty = plan.metadata.schema.fields().is_empty()
                    && inner.inspect_metadata(request).await?.is_none();
                Ok::<_, open_arrow_access_core::Error>((plan, empty))
            }
        })
        .buffered(METADATA_CONCURRENCY);
    while let Some((plan, empty)) = inspected.try_next().await? {
        selected = true;
        if empty {
            continue;
        }
        let file_ordering = plan.ordering;
        let source = plan.metadata.schema.clone();
        ordering = ordering.combine(file_ordering);
        validate_fields(source.fields())?;
        union_schema.merge(source.as_ref())?;
        files.push(plan);
    }
    if files.is_empty() {
        return Err(if selected {
            super::error::no_inferred_schema("union_by_name")
        } else {
            empty_selection("union_by_name")
        });
    }
    let union_schema = Arc::new(union_schema.finish());
    let target = options.resolve_projection(&union_schema)?.schema;
    let metadata = RecordMetadata {
        schema: target.clone(),
        source: None,
    };
    // Keep whole streaming-file discovery inside executor workers. Retained
    // plans replay their inference samples through the same per-file execution
    // path without reopening or re-inferring any selected file.
    let can_flatten_native_partitions =
        file_planner.is_some() && files.iter().all(|plan| !plan.streaming_discovery);
    if files.len() > 1 && !can_flatten_native_partitions {
        let partitions = files.into_iter().map(move |plan| {
            let alignment = Arc::new(AlignmentPlan::new(
                plan.metadata.schema.clone(),
                target.clone(),
            )?);
            let partition: ScanPartition =
                Box::pin(async move { align_reader(Box::new(plan.into_reader()), alignment) });
            Ok(partition)
        });
        return Ok(ScanPlan::from_stream(metadata, stream::iter(partitions))
            .with_file_streams()
            .with_ordering(ordering));
    }
    let partitions = stream::iter(files)
        .map(move |plan| {
            // The retained reader was opened with the full source schema.
            // Alignment also projects columns and preserves zero-column rows.
            let alignment = Arc::new(AlignmentPlan::new(
                plan.metadata.schema.clone(),
                target.clone(),
            )?);
            Ok(plan.partitions.map(move |partition| {
                let partition = partition?;
                let alignment = alignment.clone();
                let aligned: ScanPartition =
                    Box::pin(async move { align_reader(partition.await?, alignment) });
                Ok(aligned)
            }))
        })
        .try_flatten();
    Ok(ScanPlan::from_stream(metadata, partitions).with_ordering(ordering))
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
