use std::{ops::Range, sync::Arc};

use futures::future::BoxFuture;
use open_arrow_access_core::{ByteReader, Bytes};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
};

/// Bridge shared byte access to the Parquet decoder.
pub(super) struct ParquetInput {
    inner: Box<dyn ByteReader>,
}

impl ParquetInput {
    /// Wrap an opened byte reader whose exact ranges and length were validated.
    pub(super) fn new(inner: Box<dyn ByteReader>) -> Self {
        Self { inner }
    }

    /// Fetch an exact range through the backend-independent async byte reader.
    async fn read_bytes(&mut self, range: Range<u64>) -> ParquetResult<Bytes> {
        self.inner
            .read_range(range)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))
    }

    /// Fetch footer/schema metadata through range reads, honoring metadata/index options.
    async fn read_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> ParquetResult<Arc<ParquetMetaData>> {
        let length = self
            .inner
            .metadata()
            .content_length
            .ok_or_else(|| ParquetError::General("input length is unknown".to_owned()))?;
        let metadata = ParquetMetaDataReader::new()
            .with_metadata_options(options.map(|o| o.metadata_options().clone()))
            .with_column_index_policy(
                options.map_or(PageIndexPolicy::Skip, |o| o.column_index_policy()),
            )
            .with_offset_index_policy(
                options.map_or(PageIndexPolicy::Skip, |o| o.offset_index_policy()),
            )
            .load_and_finish(self, length)
            .await?;
        Ok(Arc::new(metadata))
    }
}

impl AsyncFileReader for ParquetInput {
    /// Adapt async range reads to the boxed-future signature required by Parquet.
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(self.read_bytes(range))
    }

    /// Adapt async inspection to the boxed-future signature required by Parquet.
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(self.read_metadata(options))
    }
}
