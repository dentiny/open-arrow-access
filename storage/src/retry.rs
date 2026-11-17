//! Retry storage operations without including backoff in attempt timeouts.
use backon::{BackoffBuilder, BlockingRetryable, ExponentialBackoff, ExponentialBuilder};
use opendal::raw::oio::Read as _;
use opendal::{raw::*, *};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug)]
pub(crate) struct HttpRetryLayer(crate::RetryConfig);

impl HttpRetryLayer {
    pub(crate) fn new(config: &crate::RetryConfig) -> Self {
        Self(config.clone())
    }

    fn builder(&self) -> ExponentialBuilder {
        let config = &self.0;
        let builder = ExponentialBuilder::default()
            .with_max_times(config.max_retries)
            .with_min_delay(config.min_delay)
            .with_max_delay(config.max_delay)
            .with_factor(config.factor);
        if config.jitter {
            builder.with_jitter()
        } else {
            builder
        }
    }

    fn budget(&self) -> RetryBudget {
        RetryBudget {
            delays: self.builder().build(),
            max_server_delay: self.0.max_delay,
        }
    }

    fn prepare<T>(&self, action: impl FnMut() -> Result<T>) -> Result<T> {
        action
            .retry(self.builder())
            .when(Error::is_temporary)
            .call()
            .map_err(Error::set_persistent)
    }
}

struct RetryBudget {
    delays: ExponentialBackoff,
    max_server_delay: Duration,
}
impl RetryBudget {
    async fn wait(&mut self, error: Error) -> Result<()> {
        if !error.is_temporary() {
            return Err(error.set_persistent());
        }
        let Some(mut delay) = self.delays.next() else {
            return Err(error.set_persistent());
        };
        if let Some(server) = response_delay(&error) {
            if server > self.max_server_delay {
                return Err(error.set_persistent());
            }
            delay = delay.max(server);
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }
}

// Retry loops borrow their operation/body directly, so cancellation leaves
// ownership with the caller. Each invocation has an independent retry budget.
macro_rules! retry_call {
    ($policy:expr, $call:expr) => {{
        let mut budget = $policy.budget();
        loop {
            match $call.await {
                Ok(value) => break Ok(value),
                Err(error) => budget.wait(error).await?,
            }
        }
    }};
}

impl Layer for HttpRetryLayer {
    fn apply_service(&self, backend: Servicer) -> Servicer {
        Arc::new(RetryStorage {
            backend,
            policy: self.clone(),
        })
    }
}

#[derive(Debug)]
struct RetryStorage {
    backend: Servicer,
    policy: HttpRetryLayer,
}

macro_rules! control_operation {
    ($name:ident($($arg:ident: $kind:ty),*) -> $output:ty) => {
        async fn $name(&self, context: &OperationContext, $($arg: $kind),*) -> Result<$output> {
            let context = response_context(context);
            retry_call!(self.policy, self.backend.$name(&context, $($arg.clone()),*))
        }
    };
}
macro_rules! open_body {
    ($name:ident($($arg:ident: $kind:ty),*) -> $output:ident) => {
        fn $name(&self, context: &OperationContext, $($arg: $kind),*) -> Result<Self::$output> {
            let context = response_context(context);
            let body = self.policy.prepare(|| self.backend.$name(&context, $($arg.clone()),*))?;
            Ok(RetryBody { body, policy: self.policy.clone() })
        }
    };
}
impl Service for RetryStorage {
    type Reader = RetryReader;
    type Writer = RetryBody<oio::Writer>;
    type Lister = RetryBody<oio::Lister>;
    type Deleter = RetryBody<oio::Deleter>;
    type Copier = RetryBody<oio::Copier>;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.backend.info()
    }
    fn capability(&self) -> Capability {
        self.backend.capability()
    }

    control_operation!(create_dir(path: &str, args: OpCreateDir) -> RpCreateDir);
    control_operation!(stat(path: &str, args: OpStat) -> RpStat);
    control_operation!(rename(from: &str, to: &str, args: OpRename) -> RpRename);
    control_operation!(restore(path: &str, args: OpRestore) -> RpRestore);
    control_operation!(presign(path: &str, args: OpPresign) -> RpPresign);
    open_body!(write(path: &str, args: OpWrite) -> Writer);
    open_body!(list(path: &str, args: OpList) -> Lister);
    open_body!(delete() -> Deleter);
    open_body!(copy(from: &str, to: &str, args: OpCopy) -> Copier);

    fn compose(
        &self,
        context: &OperationContext,
        to: &str,
        args: OpCompose,
    ) -> Result<Self::Composer> {
        self.backend.compose(&response_context(context), to, args)
    }
    fn read(&self, context: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let context = response_context(context);
        let reader = self
            .policy
            .prepare(|| self.backend.read(&context, path, args.clone()))?;
        Ok(RetryReader {
            reader: Arc::new(reader),
            policy: self.policy.clone(),
        })
    }
}

struct RetryReader {
    reader: Arc<oio::Reader>,
    policy: HttpRetryLayer,
}
impl oio::Read for RetryReader {
    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        retry_call!(self.policy, self.reader.read(range))
    }
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (metadata, stream) = retry_call!(self.policy, self.reader.open(range))?;
        Ok((
            metadata,
            Box::new(ResumingStream {
                source: self.reader.clone(),
                policy: self.policy.clone(),
                remaining: range,
                active: Some(stream),
                finished: false,
            }),
        ))
    }
}

struct ResumingStream {
    source: Arc<oio::Reader>,
    policy: HttpRetryLayer,
    remaining: BytesRange,
    active: Option<Box<dyn oio::ReadStreamDyn>>,
    finished: bool,
}
impl oio::ReadStream for ResumingStream {
    async fn read(&mut self) -> Result<Buffer> {
        if self.finished {
            return Ok(Buffer::new());
        }
        let mut budget = self.policy.budget();
        loop {
            if self.active.is_none() {
                match self.source.open(self.remaining).await {
                    Ok((_, stream)) => self.active = Some(stream),
                    Err(error) => {
                        budget.wait(error).await?;
                        continue;
                    }
                }
            }
            match self
                .active
                .as_mut()
                .expect("stream was opened")
                .read()
                .await
            {
                Ok(bytes) => {
                    self.remaining.advance(bytes.len() as u64);
                    self.finished = bytes.is_empty();
                    return Ok(bytes);
                }
                Err(error) => {
                    // Only bytes returned to the caller advance the next range.
                    self.active = None;
                    budget.wait(error).await?;
                }
            }
        }
    }
}

struct RetryBody<T> {
    body: T,
    policy: HttpRetryLayer,
}
macro_rules! body_operation {
    ($name:ident($($arg:ident: $kind:ty),*) -> $output:ty) => {
        async fn $name(&mut self, $($arg: $kind),*) -> Result<$output> {
            retry_call!(self.policy, self.body.$name($($arg.clone()),*))
        }
    };
}
impl<T: oio::Write> oio::Write for RetryBody<T> {
    body_operation!(write(bytes: Buffer) -> ());
    body_operation!(copy_from(path: &str, args: OpRead, range: BytesRange) -> ());
    body_operation!(close() -> Metadata);
    body_operation!(abort() -> ());
}
impl<T: oio::List> oio::List for RetryBody<T> {
    body_operation!(next() -> Option<oio::Entry>);
}
impl<T: oio::Delete> oio::Delete for RetryBody<T> {
    body_operation!(delete(path: &str, args: OpDelete) -> ());
    body_operation!(close() -> ());
}
impl<T: oio::Copy> oio::Copy for RetryBody<T> {
    body_operation!(next() -> Option<usize>);
    body_operation!(close() -> Metadata);
    body_operation!(abort() -> ());
}

#[derive(Debug)]
struct RateLimitResponse {
    status: http::StatusCode,
    header: Option<String>,
    delay: Option<Duration>,
}
impl std::fmt::Display for RateLimitResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP {}; Retry-After: {:?}", self.status, self.header)
    }
}
impl std::error::Error for RateLimitResponse {}

fn response_delay(error: &Error) -> Option<Duration> {
    let mut cause: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(response) = cause.downcast_ref::<RateLimitResponse>() {
            return response.delay;
        }
        cause = cause.source()?;
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

fn response_context(context: &OperationContext) -> OperationContext {
    let transport = ResponseTransport(context.http_transport().clone());
    context
        .clone()
        .with_http_transport(HttpTransporter::new(transport))
}
struct ResponseTransport(HttpTransporter);
impl HttpTransport for ResponseTransport {
    async fn fetch(&self, request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let response = self.0.fetch(request).await?;
        if response.status() != http::StatusCode::TOO_MANY_REQUESTS
            && response.status() != http::StatusCode::SERVICE_UNAVAILABLE
        {
            return Ok(response);
        }
        let header = response
            .headers()
            .get(http::header::RETRY_AFTER)
            .and_then(|header| header.to_str().ok())
            .map(str::to_owned);
        let details = RateLimitResponse {
            status: response.status(),
            delay: header.as_deref().and_then(server_delay),
            header,
        };
        // Release response bodies and their request permits before backoff.
        drop(response);
        Err(
            Error::new(ErrorKind::Unexpected, "storage HTTP request was throttled")
                .set_source(details)
                .set_temporary(),
        )
    }
}
