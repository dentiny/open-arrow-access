use open_arrow_access_core::{
    Bytes, FieldNameIndex, ReadOptions, RecordBatch, RecordBatchBuilder, RecordBatchRow, Result,
    SchemaRef, read_all,
};
use open_arrow_access_decompression::tar::{TarEntry, TarStream};

/// Batch regular TAR members; callers supply only their schema-specific fields.
/// Projection without `data` drains member bodies without materializing them.
pub(super) async fn read_tar_batch(
    mut tar: TarStream,
    schema: SchemaRef,
    options: ReadOptions,
    target_bytes: usize,
    mut write: impl FnMut(&mut RecordBatchRow<'_>, &TarEntry, &[u8]) -> Result<()>,
) -> Result<Option<(RecordBatch, TarStream)>> {
    let mut builder = RecordBatchBuilder::new(schema, options.projection)?;
    let include_data = FieldNameIndex::new(builder.schema().fields())
        .get("data")
        .is_some();
    let mut payload_bytes = 0_u64;
    while builder.len() < options.batch_size.get()
        && (builder.is_empty() || payload_bytes < target_bytes as u64)
    {
        let Some(member) = tar.next_entry().await? else {
            break;
        };
        if !member.entry.is_file {
            drop(member.reader);
            continue;
        }
        payload_bytes = payload_bytes.saturating_add(member.entry.size);
        let mut body = member.reader;
        let data = if include_data {
            Bytes::from(read_all(body, "read_tar_entry").await?)
        } else {
            while body.next().await?.is_some() {}
            Bytes::new()
        };
        builder.push(|row| write(row, &member.entry, data.as_ref()))?;
    }
    if builder.is_empty() {
        Ok(None)
    } else {
        Ok(Some((builder.finish()?, tar)))
    }
}
