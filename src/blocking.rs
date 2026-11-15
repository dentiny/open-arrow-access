use std::{
    iter::FusedIterator,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use tokio::runtime::{Handle, Runtime};

use crate::RecordReader;

/// Adapt an asynchronous record reader to Arrow's synchronous reader interface.
///
/// Available with the `blocking` feature. The caller supplies a Tokio runtime
/// configured for the reader's I/O; this adapter creates no runtime, background
/// thread, producer task, or prefetch queue. Each `next()` blocks until one batch,
/// EOF, or an error. Schema inspection never polls the reader. Upstream errors
/// retain their source and category inside [`ArrowError::ExternalError`]. EOF,
/// errors, and unwinding reader panics are terminal and release the reader while
/// entered into the supplied runtime.
///
/// # Runtime and thread requirements
///
/// Call `next()` from a synchronous thread outside any Tokio runtime context.
/// A call from a Tokio context returns a terminal error without polling the
/// reader. Use a separate synchronous thread when consuming from an async
/// application; Tokio's `spawn_blocking` threads also carry a runtime context
/// and are rejected. The adapter keeps the runtime alive through an `Arc`, so
/// it can be moved to a synchronous consumer such as an Arrow C stream. Drop the
/// adapter on a synchronous thread too, or retain another runtime owner until
/// after leaving the async context: dropping the last runtime owner in an async
/// context is prohibited by Tokio. Panicking reader destructors are not caught.
///
/// ```no_run
/// use std::sync::Arc;
/// use arrow_array::RecordBatchReader;
/// use open_arrow_access::{BlockingRecordBatchReader, RecordReader};
/// use tokio::runtime::Runtime;
///
/// fn consume(reader: Box<dyn RecordReader>, runtime: Arc<Runtime>) {
///     let mut reader = BlockingRecordBatchReader::new(reader, runtime);
///     println!("{:?}", reader.schema());
///     for batch in &mut reader {
///         println!("{} rows", batch.unwrap().num_rows());
///     }
/// }
/// ```
pub struct BlockingRecordBatchReader {
    reader: Option<Box<dyn RecordReader>>,
    schema: SchemaRef,
    runtime: Arc<Runtime>,
}

impl BlockingRecordBatchReader {
    /// Own an opened reader and its runtime without polling or opening input.
    pub fn new(reader: Box<dyn RecordReader>, runtime: Arc<Runtime>) -> Self {
        let schema = reader.metadata().schema.clone();
        Self {
            reader: Some(reader),
            schema,
            runtime,
        }
    }

    fn finish(&mut self) {
        let _guard = self.runtime.enter();
        self.reader.take();
    }
}

impl Iterator for BlockingRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        if Handle::try_current().is_ok() {
            self.finish();
            return Some(Err(ArrowError::ComputeError(
                "blocking record reader must be polled outside a Tokio runtime context".into(),
            )));
        }
        let result = catch_unwind(AssertUnwindSafe(|| self.runtime.block_on(reader.next())))
            .map_err(|_| ArrowError::ComputeError("source reader panicked".into()))
            .and_then(|result| result.map_err(|error| ArrowError::ExternalError(Box::new(error))));
        match result {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.finish();
                None
            }
            Err(error) => {
                self.finish();
                Some(Err(error))
            }
        }
    }
}

impl FusedIterator for BlockingRecordBatchReader {}

impl RecordBatchReader for BlockingRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for BlockingRecordBatchReader {
    fn drop(&mut self) {
        self.finish();
    }
}
