//! Preserve each caller-supplied XML member as one binary content row.
use super::{ArxivContentFormat, content::document_batch};
use futures::{StreamExt, stream};
use open_arrow_access_core::{RecordBatch, RecordStream, Result};

/// Use the common content schema while keeping format handling independent.
pub(super) fn read(batch: RecordBatch, article_id: &str) -> Result<RecordStream> {
    let batch = document_batch(&batch, ArxivContentFormat::Xml, article_id)?;
    Ok(stream::iter([Ok(batch)]).boxed())
}
