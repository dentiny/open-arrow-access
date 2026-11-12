use std::{ops::Range, sync::Arc};

use futures::future::BoxFuture;
use open_arrow_access_core::{ByteReader, Bytes};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::Result as ParquetResult,
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
};

/// Bridge shared byte access to the Parquet decoder.
pub(super) struct ParquetInput {
    inner: Box<dyn ByteReader>,
}

impl ParquetInput {
    pub(super) fn new(inner: Box<dyn ByteReader>) -> Self {
        Self { inner }
    }

    async fn read_bytes(&mut self, range: Range<u64>) -> ParquetResult<Bytes> {
        self.inner
            .read_range(range)
            .await
            .map_err(super::error::external_error)
    }

    async fn read_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> ParquetResult<Vec<Bytes>> {
        self.inner
            .read_ranges(ranges)
            .await
            .map_err(super::error::external_error)
    }

    async fn read_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> ParquetResult<Arc<ParquetMetaData>> {
        let length = self
            .inner
            .metadata()
            .content_length
            .ok_or_else(super::error::unknown_input_length)?;
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
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(self.read_bytes(range))
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        Box::pin(self.read_byte_ranges(ranges))
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(self.read_metadata(options))
    }
}
