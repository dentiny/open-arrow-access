//! Sequential package entries over asynchronous storage, without producer tasks.
use futures::{StreamExt, TryStreamExt, io::AsyncRead};
use open_arrow_access_core::{
    ByteMetadata, ByteReader, Result, StreamReader, into_async_read, read_chunks,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

use super::{TarEntry, error, validation};
use crate::layer::DEFAULT_CHUNK_SIZE;

type Input = Compat<Box<dyn AsyncRead + Send + Unpin>>;

/// A sequential TAR entry and its bounded body reader.
pub struct TarEntryReader {
    pub entry: TarEntry,
    pub reader: Box<dyn ByteReader>,
}

/// Consume or drop the current body before advancing. Package parsing retains
/// partial header/skip progress across cancellation; cancelled body reads must be
/// dropped. Completion drains decoder trailers. Bodies may outlive the archive.
pub struct TarStream {
    archive: Option<tokio_tar::Archive<Input>>,
    entries: Option<tokio_tar::Entries<Input>>,
    metadata: ByteMetadata,
    index: usize,
    remaining: Arc<AtomicU64>,
    failed: Arc<AtomicBool>,
    ended: bool,
}

impl TarStream {
    pub fn new(input: Box<dyn ByteReader>) -> Self {
        let metadata = input.metadata().clone();
        let input: Box<dyn AsyncRead + Send + Unpin> = Box::new(into_async_read(input, "read_tar"));
        let mut archive = tokio_tar::Archive::new(input.compat());
        let entries = archive
            .entries()
            .expect("new archive starts at position zero");
        Self {
            archive: Some(archive),
            entries: Some(entries),
            metadata,
            index: 0,
            remaining: Arc::new(AtomicU64::new(0)),
            failed: Arc::new(AtomicBool::new(false)),
            ended: false,
        }
    }

    pub async fn next_entry(&mut self) -> Result<Option<TarEntryReader>> {
        if self.entries.is_none() {
            return Ok(None);
        }
        if self.failed.load(Ordering::Acquire) {
            self.entries = None;
            self.archive = None;
            return Ok(None);
        }
        // The body stream holds the other reference until EOF or drop.
        if Arc::strong_count(&self.remaining) > 1 && self.remaining.load(Ordering::Acquire) != 0 {
            return Err(error::active_entry());
        }
        let result = self.read_entry().await;
        if !matches!(&result, Ok(Some(_))) {
            self.entries = None;
            self.archive = None;
        }
        result.map_err(|error| error.with_object(self.metadata.source.as_ref()))
    }

    async fn read_entry(&mut self) -> Result<Option<TarEntryReader>> {
        if !self.ended {
            if let Some(entry) = self.entries.as_mut().unwrap().next().await {
                let entry = entry.map_err(error::header_error)?;
                let selected = validation::entry(
                    self.index,
                    entry.path_bytes().map_err(error::header_error)?,
                    entry.effective_size(),
                    entry.header().entry_type().is_file(),
                    entry.header().entry_type().is_dir(),
                )?;
                self.index += 1;
                self.remaining = Arc::new(AtomicU64::new(selected.size));
                let remaining = self.remaining.clone();
                let failed = self.failed.clone();
                let mut metadata = self.metadata.clone();
                metadata.content_length = Some(selected.size);
                let chunks = read_chunks(entry.compat(), DEFAULT_CHUNK_SIZE)
                    .inspect_ok(move |bytes| {
                        remaining.fetch_sub(bytes.len() as u64, Ordering::Release);
                    })
                    .map_err(move |error| {
                        failed.store(true, Ordering::Release);
                        super::error::header_error(error)
                    });
                return Ok(Some(TarEntryReader {
                    entry: selected,
                    reader: Box::new(StreamReader::bytes(chunks, metadata)),
                }));
            }
            self.ended = true;
        }
        // Retain ended/drain progress if this call is cancelled while waiting
        // for a decoder trailer or source EOF.
        futures::io::copy(
            &mut self.archive.as_mut().unwrap().compat(),
            &mut futures::io::sink(),
        )
        .await
        .map_err(error::header_error)?;
        Ok(None)
    }
}
