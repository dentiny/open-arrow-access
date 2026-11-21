use std::sync::{Arc, Weak};

use futures::{
    StreamExt,
    lock::Mutex,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{
    ByteMetadata, ByteReader, Bytes, Result, StreamReader, empty_chunk, into_stream,
};

use super::{
    TarEntry, TarLimits,
    error::{active_entry, decode},
    validation::{BLOCK_SIZE, is_long_name, long_name, parse_entry},
};

/// A sequential TAR entry and its lazy, bounded body reader.
pub struct TarEntryReader {
    pub entry: TarEntry,
    pub reader: Box<dyn ByteReader>,
}

/// Incremental TAR consumption over one sequential source. Entry bodies must be
/// consumed or dropped before advancing. Dropped bodies are drained on the next
/// call. No range requests or upfront header index are required. Partial header
/// and skip progress survive cancellation; a cancelled body read must be dropped.
/// After TAR end markers, input is drained through EOF to surface underlying
/// decoder checksum/trailer and source errors before reporting archive completion.
/// Dropping the archive early does not drain the remaining input.
pub struct TarStream {
    state: Arc<Mutex<State>>,
    active: Weak<()>,
    limits: TarLimits,
    metadata: ByteMetadata,
}

struct State {
    input: BoxStream<'static, Result<Bytes>>,
    pending: Bytes,
    header: Vec<u8>,
    payload: u64,
    padding: u64,
    index: usize,
    headers: usize,
    reading_name: bool,
    name_bytes: Vec<u8>,
    pending_name: Option<Vec<u8>>,
    zero_block: bool,
    finished: bool,
}

impl TarStream {
    pub fn new(input: Box<dyn ByteReader>, limits: TarLimits) -> Self {
        let metadata = input.metadata().clone();
        Self {
            state: Arc::new(Mutex::new(State {
                input: into_stream(input),
                pending: Bytes::new(),
                header: Vec::with_capacity(BLOCK_SIZE as usize),
                payload: 0,
                padding: 0,
                index: 0,
                headers: 0,
                reading_name: false,
                name_bytes: Vec::new(),
                pending_name: None,
                zero_block: false,
                finished: false,
            })),
            active: Weak::new(),
            limits,
            metadata,
        }
    }

    /// Advance by one header and return a lazy, bounded sequential body reader.
    /// Once a body is consumed, its reader remains at EOF even after advancement.
    pub async fn next_entry(&mut self) -> Result<Option<TarEntryReader>> {
        let mut state = self.state.try_lock().ok_or_else(active_entry)?;
        if state.finished {
            return Ok(None);
        }
        if state.payload != 0 && self.active.upgrade().is_some() {
            return Err(active_entry());
        }
        let entry = match state.next_header(&self.limits).await {
            Ok(entry) => entry,
            Err(error) => {
                state.finish();
                return Err(error.with_object(self.metadata.source.as_ref()));
            }
        };
        let Some(entry) = entry else {
            return Ok(None);
        };
        let mut metadata = self.metadata.clone();
        metadata.content_length = Some(entry.size);
        let token = Arc::new(());
        self.active = Arc::downgrade(&token);
        let stream = stream::try_unfold(
            (self.state.clone(), entry.size, token),
            |(state, remaining, token)| async move {
                if remaining == 0 {
                    return Ok(None);
                }
                let mut input = state.lock().await;
                if input.finished {
                    return Ok(None);
                }
                let bytes = match input.take(remaining.min(usize::MAX as u64) as usize).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        input.finish();
                        return Err(error);
                    }
                };
                input.payload -= bytes.len() as u64;
                let remaining = remaining - bytes.len() as u64;
                drop(input);
                Ok(Some((bytes, (state, remaining, token))))
            },
        );
        let reader = StreamReader::bytes(stream, metadata);
        Ok(Some(TarEntryReader {
            entry,
            reader: Box::new(reader),
        }))
    }
}

impl State {
    fn finish(&mut self) {
        self.finished = true;
        self.pending = Bytes::new();
        self.header.clear();
        self.name_bytes.clear();
        self.pending_name = None;
        self.input = Box::pin(stream::empty());
    }

    async fn take(&mut self, limit: usize) -> Result<Bytes> {
        if self.pending.is_empty() {
            self.pending = self
                .input
                .next()
                .await
                .transpose()?
                .ok_or_else(|| decode("truncated TAR stream"))?;
            if self.pending.is_empty() {
                return Err(empty_chunk("read_tar"));
            }
        }
        Ok(self.pending.split_to(limit.min(self.pending.len())))
    }

    async fn next_header(&mut self, limits: &TarLimits) -> Result<Option<TarEntry>> {
        loop {
            while self.payload != 0 {
                let bytes = self
                    .take(self.payload.min(usize::MAX as u64) as usize)
                    .await?;
                self.payload -= bytes.len() as u64;
                if self.reading_name {
                    self.name_bytes.extend_from_slice(&bytes);
                }
            }
            while self.padding != 0 {
                let bytes = self.take(self.padding as usize).await?;
                self.padding -= bytes.len() as u64;
            }
            if self.reading_name {
                self.pending_name = Some(long_name(std::mem::take(&mut self.name_bytes))?);
                self.reading_name = false;
            }
            while self.header.len() != BLOCK_SIZE as usize {
                let bytes = self.take(BLOCK_SIZE as usize - self.header.len()).await?;
                self.header.extend_from_slice(&bytes);
            }
            let zero = self.header.iter().all(|byte| *byte == 0);
            if self.zero_block {
                if !zero {
                    return Err(decode("TAR archive requires two zero end blocks"));
                }
                // Finish the underlying decoder too: its checksum/trailer may
                // follow the TAR end markers or trailing archive padding.
                while let Some(bytes) = self.input.next().await {
                    if bytes?.is_empty() {
                        return Err(empty_chunk("read_tar"));
                    }
                }
                self.finish();
                return Ok(None);
            }
            if zero {
                if self.pending_name.is_some() {
                    return Err(decode("GNU long name has no following entry"));
                }
                self.zero_block = true;
                self.header.clear();
                continue;
            }
            let mut entry = parse_entry(&self.header, self.headers, limits)?;
            self.headers += 1;
            let is_name = is_long_name(&self.header);
            self.header.clear();
            self.payload = entry.size;
            self.padding = (BLOCK_SIZE - entry.size % BLOCK_SIZE) % BLOCK_SIZE;
            if is_name {
                if self.pending_name.is_some() {
                    return Err(decode("consecutive GNU long names"));
                }
                self.reading_name = true;
                continue;
            }
            if let Some(name) = self.pending_name.take() {
                entry.name = name;
            }
            entry.index = self.index;
            self.index += 1;
            return Ok(Some(entry));
        }
    }
}
