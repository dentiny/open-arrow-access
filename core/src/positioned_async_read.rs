use crate::{BytePosition, ByteReader, Bytes};
use futures::{
    future::BoxFuture,
    io::{AsyncBufRead, AsyncRead},
};
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

type Fetch = BoxFuture<'static, (Box<dyn ByteReader>, io::Result<Option<Bytes>>)>;
enum InputState {
    Ready(Box<dyn ByteReader>),
    Reading(Fetch),
    Finished,
}

/// A position observer. Marking records the first subsequently consumed byte,
/// including when a buffered chunk or a compression frame ends at the mark.
#[derive(Clone, Default)]
pub struct PositionObserver(Arc<Mutex<Observed>>);
#[derive(Default)]
struct Observed {
    current: Option<BytePosition>,
    marked: Option<BytePosition>,
    armed: bool,
}
impl PositionObserver {
    pub fn position(&self) -> Option<BytePosition> {
        self.0.lock().unwrap().current
    }
    pub fn mark(&self) {
        let mut state = self.0.lock().unwrap();
        state.marked = None;
        state.armed = true;
    }
    pub fn marked_position(&self) -> Option<BytePosition> {
        self.0.lock().unwrap().marked
    }
}
/// Buffered input whose position advances only when bytes are consumed.
pub struct PositionedAsyncRead {
    input: InputState,
    bytes: Bytes,
    observer: PositionObserver,
    operation: &'static str,
}
/// Construct without I/O. Each chunk retains its own coordinate origin.
pub fn into_positioned_async_read(
    reader: Box<dyn ByteReader>,
    operation: &'static str,
) -> PositionedAsyncRead {
    let observer = PositionObserver::default();
    observer.0.lock().unwrap().current = reader.position();
    PositionedAsyncRead {
        input: InputState::Ready(reader),
        bytes: Bytes::new(),
        observer,
        operation,
    }
}
impl PositionedAsyncRead {
    pub fn position(&self) -> Option<BytePosition> {
        self.observer.position()
    }
    pub fn observer(&self) -> PositionObserver {
        self.observer.clone()
    }
}
impl AsyncBufRead for PositionedAsyncRead {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        if this.bytes.is_empty() {
            let mut pending = match std::mem::replace(&mut this.input, InputState::Finished) {
                InputState::Ready(mut reader) => {
                    this.observer.0.lock().unwrap().current = reader.position();
                    let operation = this.operation;
                    Box::pin(async move {
                        let result =
                            crate::next_nonempty(reader.as_mut(), || crate::empty_chunk(operation))
                                .await
                                .map_err(crate::error::byte_input_error);
                        (reader, result)
                    }) as Fetch
                }
                InputState::Reading(pending) => pending,
                InputState::Finished => return Poll::Ready(Ok(&this.bytes)),
            };
            let (reader, result) = match pending.as_mut().poll(cx) {
                Poll::Pending => {
                    this.input = InputState::Reading(pending);
                    return Poll::Pending;
                }
                Poll::Ready(result) => result,
            };
            let Some(bytes) = result? else {
                return Poll::Ready(Ok(&this.bytes));
            };
            this.bytes = bytes;
            this.input = InputState::Ready(reader);
        }
        Poll::Ready(Ok(&this.bytes))
    }
    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        crate::validation::validate_buffer_consumption(amount, this.bytes.len());
        if amount > 0 {
            let mut state = this.observer.0.lock().unwrap();
            if state.armed {
                state.marked = state.current;
                state.armed = false;
            }
            state.current = state.current.map(|p| p.advance(amount as u64));
            this.bytes = this.bytes.slice(amount..);
        }
    }
}
impl AsyncRead for PositionedAsyncRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if output.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let bytes = std::task::ready!(self.as_mut().poll_fill_buf(cx))?;
        let amount = bytes.len().min(output.len());
        output[..amount].copy_from_slice(&bytes[..amount]);
        self.consume(amount);
        Poll::Ready(Ok(amount))
    }
}
