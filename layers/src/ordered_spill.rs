//! A bounded append-only IPC spool for one resident file. Publication follows
//! completed writes; the reader never observes a partial frame. One spool per
//! file caps both disk usage and open descriptors, independently of batch count.

use std::{io::Cursor, num::NonZeroU64};

use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use open_arrow_access_core::{Error, ErrorKind, RecordBatch, Result};
use tempfile::NamedTempFile;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};

#[derive(Clone, Copy, Default)]
struct Progress {
    written: u64,
    finished: bool,
}

pub(crate) struct SpillWriter {
    file: File,
    progress: watch::Sender<Progress>,
    written: u64,
    limit: u64,
}

pub(crate) struct SpillReader {
    file: File,
    progress: watch::Receiver<Progress>,
    offset: u64,
    // Own the unlink guard until the consumer has drained or dropped this spool.
    _temporary: NamedTempFile,
}

fn io_error(error: std::io::Error) -> Error {
    Error::new(
        ErrorKind::Io,
        "ordered_file_spill",
        "temporary file I/O failed",
    )
    .with_source(error)
}

fn ipc_error(error: arrow_schema::ArrowError) -> Error {
    Error::new(
        ErrorKind::Decode,
        "ordered_file_spill",
        "temporary Arrow IPC failed",
    )
    .with_source(error)
}

impl SpillWriter {
    pub(crate) async fn new(limit: NonZeroU64) -> Result<(Self, SpillReader)> {
        let (temporary, input, output) = tokio::task::spawn_blocking(|| {
            let temporary = NamedTempFile::new()?;
            // Reopen, rather than clone: producer and consumer need independent
            // file positions on platforms where cloned descriptors share them.
            let input = temporary.reopen()?;
            let output = temporary.reopen()?;
            Ok::<_, std::io::Error>((temporary, input, output))
        })
        .await
        .map_err(crate::error::partition_task_error)?
        .map_err(io_error)?;
        let (sender, receiver) = watch::channel(Progress::default());
        Ok((
            Self {
                file: File::from_std(output),
                progress: sender,
                written: 0,
                limit: limit.get(),
            },
            SpillReader {
                file: File::from_std(input),
                progress: receiver,
                offset: 0,
                _temporary: temporary,
            },
        ))
    }

    /// False means the remaining quota cannot fit this batch. The caller keeps
    /// the batch and returns to bounded in-memory delivery after this spool.
    pub(crate) async fn append(&mut self, batch: &RecordBatch) -> Result<bool> {
        let mut bytes = Vec::new();
        {
            // A complete stream per frame retains dictionaries and schema metadata
            // without retaining a dictionary tracker for the whole file.
            let mut writer =
                StreamWriter::try_new(&mut bytes, &batch.schema()).map_err(ipc_error)?;
            writer.write(batch).map_err(ipc_error)?;
            writer.finish().map_err(ipc_error)?;
        }
        let length = bytes.len() as u64;
        if length.saturating_add(8) > self.limit - self.written {
            return Ok(false);
        }
        self.file.write_u64_le(length).await.map_err(io_error)?;
        self.file.write_all(&bytes).await.map_err(io_error)?;
        // Tokio may leave the last blocking write pending after write_all returns.
        // Flush completes it before publishing bytes to the independent reader.
        self.file.flush().await.map_err(io_error)?;
        self.written += length + 8;
        self.progress.send_replace(Progress {
            written: self.written,
            finished: false,
        });
        Ok(true)
    }

    pub(crate) fn finish(self) {
        self.progress.send_replace(Progress {
            written: self.written,
            finished: true,
        });
    }
}

impl SpillReader {
    pub(crate) async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            let progress = *self.progress.borrow_and_update();
            if self.offset < progress.written {
                let length = self.file.read_u64_le().await.map_err(io_error)?;
                if length > progress.written.saturating_sub(self.offset + 8) {
                    return Err(Error::new(
                        ErrorKind::Internal,
                        "ordered_file_spill",
                        "incomplete IPC frame",
                    ));
                }
                let length = usize::try_from(length).map_err(|error| {
                    Error::new(
                        ErrorKind::ResourceExhausted,
                        "ordered_file_spill",
                        "IPC frame exceeds address space",
                    )
                    .with_source(error)
                })?;
                let mut bytes = vec![0; length];
                self.file.read_exact(&mut bytes).await.map_err(io_error)?;
                self.offset += length as u64 + 8;
                let mut reader =
                    StreamReader::try_new(Cursor::new(bytes), None).map_err(ipc_error)?;
                return reader.next().transpose().map_err(ipc_error);
            }
            if progress.finished || self.progress.changed().await.is_err() {
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/ordered_spill.rs"]
mod tests;
