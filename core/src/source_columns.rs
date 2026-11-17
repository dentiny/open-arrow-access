use crate::{Accessor, RecordReader, Result, ScanPlan, ScanPlanner, async_trait};
use std::sync::Arc;

/// Optional parser extension, keeping provenance out of ordinary request structs.
#[async_trait]
pub trait SourceColumnAccessor: Accessor<Reader = dyn RecordReader> {
    async fn open_source_columns(
        &self,
        request: Self::Request,
        columns: &[SourceColumn],
    ) -> Result<Box<dyn RecordReader>>;
}
#[async_trait]
pub trait SourceColumnPlanner: SourceColumnAccessor + ScanPlanner {
    async fn plan_source_columns(
        &self,
        request: Self::Request,
        columns: &[SourceColumn],
    ) -> Result<ScanPlan>;
}
#[async_trait]
impl<A: SourceColumnAccessor + ?Sized> SourceColumnAccessor for Arc<A> {
    async fn open_source_columns(
        &self,
        request: Self::Request,
        columns: &[SourceColumn],
    ) -> Result<Box<dyn RecordReader>> {
        (**self).open_source_columns(request, columns).await
    }
}
#[async_trait]
impl<A: SourceColumnPlanner + ?Sized> SourceColumnPlanner for Arc<A> {
    async fn plan_source_columns(
        &self,
        request: Self::Request,
        columns: &[SourceColumn],
    ) -> Result<ScanPlan> {
        (**self).plan_source_columns(request, columns).await
    }
}
/// Bind source columns while retaining the provider's existing request type.
/// Used by datasets to forward the same selection to independently opened files.
pub struct SourceColumnRequestAccessor<A> {
    inner: A,
    columns: Vec<SourceColumn>,
}
impl<A> SourceColumnRequestAccessor<A> {
    pub fn new(inner: A, columns: Vec<SourceColumn>) -> Self {
        Self { inner, columns }
    }
}
#[async_trait]
impl<A: SourceColumnAccessor> Accessor for SourceColumnRequestAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;
    fn capability(&self) -> crate::RecordCapability {
        self.inner.capability()
    }
    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        self.inner.open_source_columns(request, &self.columns).await
    }
}
#[async_trait]
impl<A: SourceColumnPlanner> ScanPlanner for SourceColumnRequestAccessor<A> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        self.inner.plan_source_columns(request, &self.columns).await
    }
}

mod position {
    // Byte coordinates and opt-in record provenance.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum BytePosition {
        Stored(u64),
        Frame { frame: u64, decoded: u64 },
    }
    impl BytePosition {
        pub fn advance(self, n: u64) -> Self {
            match self {
                Self::Stored(offset) => Self::Stored(offset + n),
                Self::Frame { frame, decoded } => Self::Frame {
                    frame,
                    decoded: decoded + n,
                },
            }
        }
        pub fn offset(self) -> u64 {
            match self {
                Self::Stored(offset) => offset,
                Self::Frame { frame, .. } => frame,
            }
        }
        pub fn offset_in_frame(self) -> u64 {
            match self {
                Self::Stored(_) => 0,
                Self::Frame { decoded, .. } => decoded,
            }
        }
    }
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    pub enum SourceColumn {
        Object,
        Offset,
        OffsetInFrame,
    }
    impl SourceColumn {
        pub fn name(self) -> &'static str {
            match self {
                Self::Object => "_source_object",
                Self::Offset => "_source_offset",
                Self::OffsetInFrame => "_source_offset_in_frame",
            }
        }
        pub fn field(self) -> arrow_schema::Field {
            arrow_schema::Field::new(
                self.name(),
                match self {
                    Self::Object => arrow_schema::DataType::Utf8,
                    _ => arrow_schema::DataType::UInt64,
                },
                false,
            )
            .with_metadata(
                [(
                    "open_arrow_access:source_column".to_owned(),
                    self.name().to_owned(),
                )]
                .into(),
            )
        }
    }
}
pub use position::{BytePosition, SourceColumn};
mod positioned {
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
                        let result = reader.next().await.map_err(crate::error::byte_input_error);
                        (reader, result)
                    }));
                }
                let (reader, result) =
                    std::task::ready!(this.pending.as_mut().unwrap().as_mut().poll(cx));
                this.pending = None;
                this.reader = Some(reader);
                match result {
                    Ok(Some(bytes)) => {
                        if let Err(error) =
                            crate::validation::validate_positioned_chunk(&bytes, this.operation)
                        {
                            this.ended = true;
                            this.reader = None;
                            return Poll::Ready(Err(error));
                        }
                        this.bytes = bytes;
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
}
pub use positioned::{PositionObserver, PositionedAsyncRead, into_positioned_async_read};
