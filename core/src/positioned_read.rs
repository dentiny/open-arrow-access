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
    reader: Option<Box<dyn ByteReader>>,
    pending: Option<Fetch>,
    bytes: Bytes,
    observer: PositionObserver,
    ended: bool,
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
        reader: Some(reader),
        pending: None,
        bytes: Bytes::new(),
        observer,
        ended: false,
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
        if this.bytes.is_empty() && !this.ended {
            if this.pending.is_none() {
                let mut reader = this.reader.take().unwrap();
                this.observer.0.lock().unwrap().current = reader.position();
                this.pending = Some(Box::pin(async move {
                    let result = reader.next().await.map_err(io::Error::other);
                    (reader, result)
                }));
            }
            let (reader, result) =
                std::task::ready!(this.pending.as_mut().unwrap().as_mut().poll(cx));
            this.pending = None;
            this.reader = Some(reader);
            match result {
                Ok(Some(bytes)) if !bytes.is_empty() => this.bytes = bytes,
                Ok(Some(_)) => {
                    this.ended = true;
                    this.reader = None;
                    return Poll::Ready(Err(io::Error::other(crate::error::empty_chunk(
                        this.operation,
                    ))));
                }
                Ok(None) => {
                    this.ended = true;
                    this.reader = None;
                }
                Err(error) => {
                    this.ended = true;
                    this.reader = None;
                    return Poll::Ready(Err(error));
                }
            }
        }
        Poll::Ready(Ok(&this.bytes))
    }
    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        assert!(amount <= this.bytes.len());
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
