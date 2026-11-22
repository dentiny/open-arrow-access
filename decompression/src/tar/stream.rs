//! Stream decoded package entries through a bounded async/synchronous bridge.
use futures::{TryFutureExt, stream};
use open_arrow_access_core::{
    ByteMetadata, ByteReader, Bytes, Error, ErrorKind, Result, StreamReader, into_async_read,
};
use std::{
    io::{self, Read},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc as commands,
    },
};
use tokio::{sync::mpsc, task::JoinHandle};

use super::{
    TarEntry, TarLimits,
    bridge::{self, Lease, METADATA_BUDGET},
    error, validation,
};
use crate::layer::DEFAULT_CHUNK_SIZE;

/// A sequential TAR entry and its bounded body reader.
pub struct TarEntryReader {
    pub entry: TarEntry,
    pub reader: Box<dyn ByteReader>,
}

struct PendingEntry {
    entry: TarEntryReader,
    remaining: Weak<AtomicU64>,
}

type EntryResponse = Result<Option<PendingEntry>>;

/// Sequential tar::Archive entries over one asynchronous source. Requires Tokio.
/// Consume or drop the current body before advancing. The package skips dropped
/// bodies and decodes GNU/PAX metadata. Header requests survive cancellation;
/// cancelled body reads must be dropped. Archive completion drains the source
/// through EOF so decoder trailer/source failures remain observable.
/// A body may outlive the archive. Dropping both cancels the bounded worker.
pub struct TarStream {
    input: Option<Box<dyn ByteReader>>,
    metadata: ByteMetadata,
    limits: TarLimits,
    lease: Arc<Lease>,
    active: Weak<AtomicU64>,
    commands: Option<commands::Sender<()>>,
    entries: Option<mpsc::Receiver<EntryResponse>>,
    worker: Option<JoinHandle<()>>,
    pending: bool,
    finished: bool,
    body_failed: Arc<AtomicBool>,
}

impl TarStream {
    pub fn new(input: Box<dyn ByteReader>, limits: TarLimits) -> Self {
        Self {
            metadata: input.metadata().clone(),
            input: Some(input),
            limits,
            lease: Arc::new(Lease(tokio_util::sync::CancellationToken::new())),
            active: Weak::new(),
            commands: None,
            entries: None,
            worker: None,
            pending: false,
            finished: false,
            body_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn start(&mut self) -> Result<()> {
        let handle = tokio::runtime::Handle::try_current().map_err(error::runtime)?;
        let input = into_async_read(self.input.take().expect("unstarted archive"), "read_tar");
        let (input, budget) = bridge::reader(input, self.lease.0.clone(), handle.clone());
        let (commands, requests) = commands::channel();
        let (entries, receiver) = mpsc::channel(1);
        let limits = self.limits.clone();
        let metadata = self.metadata.clone();
        let lease = Arc::downgrade(&self.lease);
        let failed = self.body_failed.clone();
        self.worker = Some(handle.spawn_blocking(move || {
            let worker = Worker {
                requests,
                output: &entries,
                limits,
                metadata,
                lease,
                failed,
            };
            if let Err(error) = worker.read(input, budget) {
                let _ = entries.blocking_send(Err(error));
            }
        }));
        self.commands = Some(commands);
        self.entries = Some(receiver);
        Ok(())
    }

    pub async fn next_entry(&mut self) -> Result<Option<TarEntryReader>> {
        if self.finished {
            return Ok(None);
        }
        if self.body_failed.load(Ordering::Acquire) {
            self.finished = true;
            self.commands = None;
            self.entries = None;
            self.lease.0.cancel();
            if let Some(worker) = self.worker.take() {
                worker.await.map_err(error::worker)?;
            }
            return Ok(None);
        }
        if self
            .active
            .upgrade()
            .is_some_and(|remaining| remaining.load(Ordering::Acquire) != 0)
        {
            return Err(error::active_entry());
        }
        if self.entries.is_none() {
            self.start()?;
        }
        if !self.pending {
            // A disconnected worker is handled below, including its panic.
            let _ = self.commands.as_ref().unwrap().send(());
            self.pending = true;
        }
        let response = self.entries.as_mut().unwrap().recv().await;
        self.pending = false;
        match response {
            Some(Ok(Some(entry))) => {
                self.active = entry.remaining;
                Ok(Some(entry.entry))
            }
            response => {
                self.finished = true;
                self.commands = None;
                self.entries = None;
                self.lease.0.cancel();
                if let Some(worker) = self.worker.take() {
                    worker.await.map_err(error::worker)?;
                }
                match response {
                    Some(Err(error)) => Err(error.with_object(self.metadata.source.as_ref())),
                    _ => Ok(None),
                }
            }
        }
    }
}

struct Worker<'a> {
    requests: commands::Receiver<()>,
    output: &'a mpsc::Sender<EntryResponse>,
    limits: TarLimits,
    metadata: ByteMetadata,
    lease: Weak<Lease>,
    failed: Arc<AtomicBool>,
}

impl Worker<'_> {
    fn read<R: Read>(self, input: R, budget: Arc<AtomicU64>) -> Result<()> {
        let Self {
            requests,
            output,
            limits,
            metadata,
            lease,
            failed,
        } = self;
        let mut archive = tar::Archive::new(input);
        let mut entries = archive.entries().map_err(error::header_error)?;
        let mut index = 0;
        while requests.recv().is_ok() {
            budget.store(METADATA_BUDGET, Ordering::Relaxed);
            let Some(entry) = entries.next() else {
                budget.store(u64::MAX, Ordering::Relaxed);
                io::copy(&mut archive.into_inner(), &mut io::sink())
                    .map_err(error::header_error)?;
                let _ = output.blocking_send(Ok(None));
                return Ok(());
            };
            let mut entry = entry.map_err(error::header_error)?;
            let selected = validation::entry(&entry, index, &limits)?;
            index += 1;
            budget.store(u64::MAX, Ordering::Relaxed);
            let Some(lease) = lease.upgrade() else {
                return Ok(());
            };
            let remaining = Arc::new(AtomicU64::new(selected.size));
            let active = Arc::downgrade(&remaining);
            let mut body_metadata = metadata.clone();
            body_metadata.content_length = Some(selected.size);
            let (body, receiver) = mpsc::channel::<Result<Bytes>>(1);
            let source = metadata.source.clone();
            let failed = failed.clone();
            let chunks = stream::try_unfold(
                (receiver, remaining, lease),
                move |(mut receiver, remaining, lease)| {
                    let source = source.clone();
                    let failed = failed.clone();
                    async move {
                        if remaining.load(Ordering::Acquire) == 0 {
                            return Ok(None);
                        }
                        let bytes = receiver.recv().await.ok_or_else(|| {
                            error::decode("TAR worker ended before the entry body completed")
                        })??;
                        remaining.fetch_sub(bytes.len() as u64, Ordering::Release);
                        Ok(Some((bytes, (receiver, remaining, lease))))
                    }
                    .map_err(move |error: Error| {
                        failed.store(true, Ordering::Release);
                        error.with_object(source.as_ref())
                    })
                },
            );
            let reader = StreamReader::bytes(chunks, body_metadata);
            if output
                .blocking_send(Ok(Some(PendingEntry {
                    entry: TarEntryReader {
                        entry: selected,
                        reader: Box::new(reader),
                    },
                    remaining: active,
                })))
                .is_err()
            {
                return Ok(());
            }
            loop {
                if body.is_closed() {
                    // Drain with the package entry reader before resetting the
                    // metadata budget, without buffering the discarded payload.
                    io::copy(&mut entry, &mut io::sink()).map_err(error::header_error)?;
                    break;
                }
                let mut bytes = vec![0; DEFAULT_CHUNK_SIZE.get()];
                let size = match entry.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(size) => size,
                    Err(io_error) => {
                        let kind = ErrorKind::from_source(&io_error).unwrap_or(ErrorKind::Decode);
                        let message = io_error.to_string();
                        let _ = body.blocking_send(Err(Error::new(kind, "read_tar", message)
                            .with_object(metadata.source.as_ref())));
                        return Err(error::header_error(io_error));
                    }
                };
                bytes.truncate(size);
                if body.blocking_send(Ok(Bytes::from(bytes))).is_err() {
                    io::copy(&mut entry, &mut io::sink()).map_err(error::header_error)?;
                    break;
                }
            }
        }
        Ok(())
    }
}
