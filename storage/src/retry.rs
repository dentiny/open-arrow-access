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
#[derive(Debug, Clone)]
pub(crate) struct RetryAfterLayer {
    max_delay: Duration,
    gate: Arc<Gate>,
    retry_forbidden: bool,
}

impl RetryAfterLayer {
    pub(crate) fn new(max_delay: Duration) -> Self {
        Self {
            max_delay,
            gate: Arc::default(),
            retry_forbidden: false,
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
            retry_forbidden: self.retry_forbidden,
        };
        context.with_http_transport(HttpTransporter::new(transport))
    }
}

/// Shared HTTP object-read/stat pacing and cooldown for cloned operators. Install outside
/// TimeoutLayer and inside RetryLayer so server cooldowns precede attempt timeouts.
/// This is process-local coordination; share clones per origin and divide the
/// desired origin rate across independent worker processes.
///
/// Each object read/range open or stat consumes an admission; streaming body
/// chunks do not. This requires an HTTP-backed operator. Create one layer per
/// origin and clone it across that origin's operators; unrelated origins should
/// use separate layers. The layer does not inspect or partition by URL.
///
/// Apply layers in this order: `operator.layer(timeout).layer(rate).layer(retry)`.
/// The default classifies 429/503 as temporary errors, preserves Retry-After,
/// and caps accepted server cooldowns at ten minutes.
#[derive(Clone, Debug)]
pub struct HttpReadRateLimitLayer {
    inner: RetryAfterLayer,
}
impl HttpReadRateLimitLayer {
    pub fn new(min_request_interval: Duration) -> open_arrow_access_core::Result<Self> {
        if min_request_interval.is_zero()
            || Instant::now().checked_add(min_request_interval).is_none()
        {
            return Err(open_arrow_access_core::Error::new(
                open_arrow_access_core::ErrorKind::InvalidInput,
                "configure_http_rate_limit",
                "request interval must be positive and representable",
            ));
        }
        let mut inner = RetryAfterLayer::new(Duration::from_secs(600));
        inner.gate = Arc::new(Gate {
            interval: Some(min_request_interval),
            ..Default::default()
        });
        Ok(Self { inner })
    }
    /// Opt in only for origins that document HTTP 403 as a throttle response.
    /// Permission failures are not retryable by default.
    pub fn with_forbidden_throttling(mut self) -> Self {
        self.inner.retry_forbidden = true;
        self
    }
}
impl Layer for HttpReadRateLimitLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        self.inner.apply_service(inner)
    }
    fn apply_context(&self, inner: Servicer, context: OperationContext) -> OperationContext {
        self.inner.apply_context(inner, context)
    }
}
#[derive(Debug, Default)]
struct Gate {
    cooldown: Mutex<Option<Instant>>,
    interval: Option<Duration>,
    next_request: tokio::sync::Mutex<Option<Instant>>,
}
impl Gate {
    fn delay(&self, delay: Duration) {
        let Some(until) = Instant::now().checked_add(delay) else {
            return;
        };
        let mut next = self.cooldown.lock().unwrap_or_else(PoisonError::into_inner);
        *next = Some(next.map_or(until, |next| next.max(until)));
    }
    async fn wait(&self) {
        loop {
            let next = *self.cooldown.lock().unwrap_or_else(PoisonError::into_inner);
            match next.filter(|next| *next > Instant::now()) {
                Some(next) => tokio::time::sleep_until(next).await,
                None => return,
            }
        }
    }
    async fn admit_request(&self) {
        let Some(interval) = self.interval else {
            self.wait().await;
            return;
        };
        let mut next = self.next_request.lock().await;
        if let Some(when) = *next {
            tokio::time::sleep_until(when).await;
        }
        self.wait().await;
        *next = Instant::now().checked_add(interval);
    }
}

struct RetryAfterTransport {
    inner: HttpTransporter,
    max_delay: Duration,
    gate: Arc<Gate>,
    retry_forbidden: bool,
}

impl HttpTransport for RetryAfterTransport {
    async fn fetch(&self, request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let response = self.inner.fetch(request).await?;
        let status = response.status();
        if !matches!(status.as_u16(), 429 | 503)
            && !(self.retry_forbidden && status.as_u16() == 403)
        {
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
            if let Some(interval) = self.gate.interval {
                self.gate.delay(interval);
            }
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
        self.gate.admit_request().await;
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
        self.gate.admit_request().await;
        self.inner.open(range).await
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        self.gate.admit_request().await;
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
