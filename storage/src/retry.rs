//! Honor HTTP Retry-After with OpenDAL's released retry and timeout layers.
//!
//! `RetryAfterLayer` sits between `RetryLayer` and `TimeoutLayer`. Its transport
//! records a throttled response's server delay, and every attempt through the
//! layer, including attempts made by retried readers and writers, first waits
//! for that delay. The wait runs outside the attempt's timeout and overlaps
//! RetryLayer backoff, so each retry waits for the larger of the two.
use std::{
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use opendal::{
    Buffer, BytesRange, Capability, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter,
    Metadata, OperationContext, Result,
    raw::{
        Layer, OpCompose, OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename,
        OpRestore, OpStat, OpWrite, RpCreateDir, RpPresign, RpRead, RpRename, RpRestore, RpStat,
        Service, ServiceInfo, Servicer, oio,
    },
};
use tokio::time::Instant;

/// Must be applied after `TimeoutLayer` and before `RetryLayer`. A server delay
/// pauses every new attempt through the same operator, not only the throttled one.
#[derive(Debug)]
pub(crate) struct RetryAfterLayer {
    max_delay: Duration,
    gate: Arc<Gate>,
}

impl RetryAfterLayer {
    pub(crate) fn new(max_delay: Duration) -> Self {
        Self {
            max_delay,
            gate: Arc::default(),
        }
    }
}

impl Layer for RetryAfterLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(GateService {
            inner,
            gate: self.gate.clone(),
        })
    }

    fn apply_context(&self, _: Servicer, context: OperationContext) -> OperationContext {
        let transport = RetryAfterTransport {
            inner: context.http_transport().clone(),
            max_delay: self.max_delay,
            gate: self.gate.clone(),
        };
        context.with_http_transport(HttpTransporter::new(transport))
    }
}

/// Earliest instant at which the operator may start another attempt.
#[derive(Debug, Default)]
struct Gate(Mutex<Option<Instant>>);

impl Gate {
    fn delay(&self, delay: Duration) {
        let Some(until) = Instant::now().checked_add(delay) else {
            return;
        };
        let mut next = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        *next = Some(next.map_or(until, |next| next.max(until)));
    }

    async fn wait(&self) {
        let next = *self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(next) = next {
            tokio::time::sleep_until(next).await;
        }
    }
}

struct RetryAfterTransport {
    inner: HttpTransporter,
    max_delay: Duration,
    gate: Arc<Gate>,
}

impl HttpTransport for RetryAfterTransport {
    async fn fetch(&self, request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let response = self.inner.fetch(request).await?;
        let status = response.status();
        if !matches!(status.as_u16(), 429 | 503) {
            return Ok(response);
        }
        let header = response
            .headers()
            .get(http::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        // Drop failed bodies and their HTTP permits before any retry waits.
        drop(response);
        let error = Error::new(ErrorKind::Unexpected, "storage HTTP request was throttled")
            .with_context("status", status);
        let Some(header) = header else {
            return Err(error.set_temporary());
        };
        let error = error.with_context("retry_after", &header);
        match server_delay(&header) {
            // Retrying before the server allows it would only be throttled again.
            Some(delay) if delay > self.max_delay => Err(error),
            Some(delay) => {
                self.gate.delay(delay);
                Err(error.set_temporary())
            }
            None => Err(error.set_temporary()),
        }
    }
}

fn server_delay(header: &str) -> Option<Duration> {
    let header = header.trim();
    if !header.is_empty() && header.bytes().all(|character| character.is_ascii_digit()) {
        return Some(Duration::from_secs(header.parse().unwrap_or(u64::MAX)));
    }
    httpdate::parse_http_date(header)
        .ok()
        .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
}

#[derive(Debug)]
struct GateService {
    inner: Servicer,
    gate: Arc<Gate>,
}

impl GateService {
    fn gated<T>(&self, inner: T) -> Gated<T> {
        Gated {
            inner,
            gate: self.gate.clone(),
        }
    }
}

impl Service for GateService {
    type Reader = Gated<oio::Reader>;
    type Writer = Gated<oio::Writer>;
    type Lister = Gated<oio::Lister>;
    type Deleter = Gated<oio::Deleter>;
    type Copier = Gated<oio::Copier>;
    type Composer = Gated<oio::Composer>;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.gate.wait().await;
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.gate.wait().await;
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner
            .read(ctx, path, args)
            .map(|reader| self.gated(reader))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner
            .write(ctx, path, args)
            .map(|writer| self.gated(writer))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx).map(|deleter| self.gated(deleter))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner
            .list(ctx, path, args)
            .map(|lister| self.gated(lister))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner
            .copy(ctx, from, to, args)
            .map(|copier| self.gated(copier))
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner
            .compose(ctx, to, args)
            .map(|composer| self.gated(composer))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.gate.wait().await;
        self.inner.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.gate.wait().await;
        self.inner.restore(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

/// Retried readers and writers issue requests through these methods, not the service.
struct Gated<T> {
    inner: T,
    gate: Arc<Gate>,
}

impl<R: oio::Read> oio::Read for Gated<R> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.gate.wait().await;
        self.inner.open(range).await
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        self.gate.wait().await;
        self.inner.read(range).await
    }
}

impl<W: oio::Write> oio::Write for Gated<W> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.gate.wait().await;
        self.inner.write(bs).await
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        self.gate.wait().await;
        self.inner.copy_from(path, args, range).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.gate.wait().await;
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.gate.wait().await;
        self.inner.abort().await
    }
}

impl<L: oio::List> oio::List for Gated<L> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.gate.wait().await;
        self.inner.next().await
    }
}

impl<D: oio::Delete> oio::Delete for Gated<D> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.gate.wait().await;
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        self.gate.wait().await;
        self.inner.close().await
    }
}

impl<C: oio::Copy> oio::Copy for Gated<C> {
    async fn next(&mut self) -> Result<Option<usize>> {
        self.gate.wait().await;
        self.inner.next().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.gate.wait().await;
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.gate.wait().await;
        self.inner.abort().await
    }
}

impl<C: oio::Compose> oio::Compose for Gated<C> {
    async fn compose(&mut self, path: &str, args: OpRead) -> Result<()> {
        self.gate.wait().await;
        self.inner.compose(path, args).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.gate.wait().await;
        self.inner.close().await
    }
}
