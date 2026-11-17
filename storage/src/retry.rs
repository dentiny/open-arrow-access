// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! OpenDAL operation retries with HTTP Retry-After-aware backoff.
//! Modified from upstream to retain HTTP status/delay and adjust operation backoff.
//! Operation/body state handling is derived from opendal-layer-retry 0.59.5.
//! Keep timeout inside this layer: only attempts, never backoff, have deadlines.
use backon::{
    BackoffBuilder, BlockingRetryable, ExponentialBackoff, ExponentialBuilder, Retryable,
};
use opendal::{raw::*, *};
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Policy {
    builder: ExponentialBuilder,
    max_delay: Duration,
}

impl Policy {
    pub(crate) fn new(config: &crate::RetryConfig) -> Self {
        let mut builder = ExponentialBuilder::default()
            .with_max_times(config.max_retries)
            .with_min_delay(config.min_delay)
            .with_max_delay(config.max_delay)
            .with_factor(config.factor);
        if config.jitter {
            builder = builder.with_jitter();
        }
        Self {
            builder,
            max_delay: config.max_delay,
        }
    }

    fn backoff(
        &self,
    ) -> (
        ServerBackoff,
        impl FnMut(&Error) -> bool + Send + Sync + 'static,
    ) {
        // Each retry loop owns its hint; concurrent operations cannot affect it.
        let hint = Arc::new(Mutex::new(None));
        let state = hint.clone();
        let retryable = move |error: &Error| {
            *state.lock().unwrap() = http_failure(error).and_then(|failure| failure.delay);
            error.is_temporary()
        };
        (
            ServerBackoff {
                inner: self.builder.build(),
                hint,
                max_delay: self.max_delay,
            },
            retryable,
        )
    }
}

impl Layer for Policy {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(RetryService {
            inner,
            policy: *self,
        })
    }
}

struct ServerBackoff {
    inner: ExponentialBackoff,
    hint: Arc<Mutex<Option<Duration>>>,
    max_delay: Duration,
}
impl Iterator for ServerBackoff {
    type Item = Duration;
    fn next(&mut self) -> Option<Duration> {
        // Never revive an exhausted budget, or shorten a server's minimum wait.
        let delay = self.inner.next()?;
        match *self.hint.lock().unwrap() {
            Some(server) if server > self.max_delay => None,
            Some(server) => Some(delay.max(server)),
            None => Some(delay),
        }
    }
}

#[derive(Debug)]
struct HttpFailure {
    status: u16,
    retry_after: Option<String>,
    delay: Option<Duration>,
}
impl std::fmt::Display for HttpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HTTP {} (Retry-After: {:?})",
            self.status, self.retry_after
        )
    }
}
impl std::error::Error for HttpFailure {}
fn http_failure(error: &Error) -> Option<&HttpFailure> {
    let mut source: &dyn std::error::Error = error;
    loop {
        if let Some(failure) = source.downcast_ref::<HttpFailure>() {
            return Some(failure);
        }
        source = source.source()?;
    }
}

fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    Some(
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(now)
            .unwrap_or_default(),
    )
}

fn capture_context(ctx: &OperationContext) -> OperationContext {
    ctx.clone()
        .with_http_transport(HttpTransporter::new(CaptureTransport(
            ctx.http_transport().clone(),
        )))
}
struct CaptureTransport(HttpTransporter);
impl HttpTransport for CaptureTransport {
    async fn fetch(&self, request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let response = self.0.fetch(request).await?;
        if !matches!(response.status().as_u16(), 429 | 503) {
            return Ok(response);
        }
        let retry_after = response
            .headers()
            .get(http::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let failure = HttpFailure {
            status: response.status().as_u16(),
            delay: retry_after
                .as_deref()
                .and_then(|value| parse_retry_after(value, SystemTime::now())),
            retry_after,
        };
        // Drop error bodies immediately, releasing transport permits before sleep.
        Err(Error::new(
            ErrorKind::Unexpected,
            "HTTP storage temporarily unavailable",
        )
        .set_source(failure)
        .set_temporary())
    }
}
#[doc(hidden)]
pub(crate) struct RetryService {
    inner: Servicer,

    policy: Policy,
}

impl Debug for RetryService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Service for RetryService {
    type Reader = RetryReader<oio::Reader>;
    type Writer = RetryWrapper<oio::Writer>;
    type Lister = RetryWrapper<oio::Lister>;
    type Deleter = RetryWrapper<oio::Deleter>;
    type Copier = RetryWrapper<oio::Copier>;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        let ctx = &capture_context(ctx);
        self.inner.compose(ctx, to, args)
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        { || self.inner.create_dir(ctx, path, args.clone()) }
            .retry(backoff)
            .when(retryable)
            .await
            .map_err(|err| err.set_persistent())
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        let reader = { || self.inner.read(ctx, path, args.clone()) }
            .retry(backoff)
            .when(retryable)
            .call()
            .map_err(|err| err.set_persistent())?;

        Ok(RetryReader::new(reader, self.policy))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        let writer = { || self.inner.write(ctx, path, args.clone()) }
            .retry(backoff)
            .when(retryable)
            .call()
            .map_err(|err| err.set_persistent())?;

        Ok(RetryWrapper::new(writer, self.policy))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        { || self.inner.stat(ctx, path, args.clone()) }
            .retry(backoff)
            .when(retryable)
            .await
            .map_err(|err| err.set_persistent())
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        let deleter = { || self.inner.delete(ctx) }
            .retry(backoff)
            .when(retryable)
            .call()
            .map_err(|err| err.set_persistent())?;

        Ok(RetryWrapper::new(deleter, self.policy))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        let copier = { || self.inner.copy(ctx, from, to, args.clone()) }
            .retry(backoff)
            .when(retryable)
            .call()
            .map_err(|err| err.set_persistent())?;

        Ok(RetryWrapper::new(copier, self.policy))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        { || self.inner.rename(ctx, from, to, args.clone()) }
            .retry(backoff)
            .when(retryable)
            .await
            .map_err(|err| err.set_persistent())
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        { || self.inner.restore(ctx, path, args.clone()) }
            .retry(backoff)
            .when(retryable)
            .await
            .map_err(|err| err.set_persistent())
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        let lister = { || self.inner.list(ctx, path, args.clone()) }
            .retry(backoff)
            .when(retryable)
            .call()
            .map_err(|err| err.set_persistent())?;

        Ok(RetryWrapper::new(lister, self.policy))
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        let ctx = &capture_context(ctx);
        let (backoff, retryable) = self.policy.backoff();
        { || self.inner.presign(ctx, path, args.clone()) }
            .retry(backoff)
            .when(retryable)
            .await
            .map_err(|err| err.set_persistent())
    }
}

#[doc(hidden)]
pub(crate) struct RetryReader<R> {
    inner: Arc<R>,
    policy: Policy,
}

impl<R> RetryReader<R> {
    fn new(inner: R, policy: Policy) -> Self {
        Self {
            inner: Arc::new(inner),
            policy,
        }
    }
}

impl<R: oio::Read + 'static> oio::Read for RetryReader<R> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        use backon::Retryable;

        let (backoff, retryable) = self.policy.backoff();
        let (rp, stream) = { || self.inner.open(range) }
            .retry(backoff)
            .when(retryable)
            .await
            .map_err(|e| e.set_persistent())?;

        Ok((
            rp,
            Box::new(RetryReadStream::new(
                self.inner.clone(),
                stream,
                range,
                self.policy,
            )) as Box<dyn oio::ReadStreamDyn>,
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        use backon::Retryable;

        let (backoff, retryable) = self.policy.backoff();
        { || self.inner.read(range) }
            .retry(backoff)
            .when(retryable)
            .await
            .map_err(|e| e.set_persistent())
    }
}

#[doc(hidden)]
pub(crate) struct RetryReadStream<R> {
    reader: Arc<R>,
    stream: Option<Box<dyn oio::ReadStreamDyn>>,
    range: BytesRange,
    read: u64,
    policy: Policy,
}

impl<R> RetryReadStream<R> {
    fn new(
        reader: Arc<R>,
        stream: Box<dyn oio::ReadStreamDyn>,
        range: BytesRange,
        policy: Policy,
    ) -> Self {
        Self {
            reader,
            stream: Some(stream),
            range,
            read: 0,
            policy,
        }
    }
}

impl<R: oio::Read> oio::ReadStream for RetryReadStream<R> {
    async fn read(&mut self) -> Result<Buffer> {
        use backon::RetryableWithContext;

        let reader = self.reader.clone();
        let stream = self.stream.take();
        let range = self.range;
        let read = self.read;
        let (backoff, retryable) = self.policy.backoff();

        let ((stream, range, read), res) = {
            |(stream, mut range, mut read): (
                Option<Box<dyn oio::ReadStreamDyn>>,
                BytesRange,
                u64,
            )| {
                let reader = reader.clone();
                async move {
                    let mut stream = match stream {
                        Some(stream) => stream,
                        None => {
                            range.advance(read);
                            read = 0;

                            match reader.open(range).await {
                                Ok((_, stream)) => stream,
                                Err(err) => return ((None, range, read), Err(err)),
                            }
                        }
                    };

                    let res = match stream.read().await {
                        Ok(buf) => {
                            if !buf.is_empty() {
                                read += buf.len() as u64;
                            }
                            (Some(stream), Ok(buf))
                        }
                        Err(err) => (None, Err(err)),
                    };

                    ((res.0, range, read), res.1)
                }
            }
        }
        .retry(backoff)
        .when(retryable)
        .context((stream, range, read))
        .await;

        self.stream = stream;
        self.range = range;
        self.read = read;

        res.map_err(|err| err.set_persistent())
    }
}

#[doc(hidden)]
pub(crate) struct RetryWrapper<R> {
    inner: Option<R>,
    policy: Policy,
}

impl<R> RetryWrapper<R> {
    fn new(inner: R, policy: Policy) -> Self {
        Self {
            inner: Some(inner),
            policy,
        }
    }

    fn take_inner(&mut self) -> Result<R> {
        self.inner.take().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "retry layer is in bad state, please make sure future not dropped before ready",
            )
        })
    }
}

impl<R: oio::ReadStream> oio::ReadStream for RetryWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let (backoff, retryable) = self.policy.backoff();

        let (inner, res) = {
            |mut r: R| async move {
                let res = r.read().await;

                (r, res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context(inner)
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }
}

impl<R: oio::Write> oio::Write for RetryWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let (backoff, retryable) = self.policy.backoff();

        let ((inner, _), res) = {
            |(mut r, bs): (R, Buffer)| async move {
                let res = r.write(bs.clone()).await;

                ((r, bs), res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context((inner, bs))
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let path = path.to_string();
        let (backoff, retryable) = self.policy.backoff();

        let ((inner, _, _, _), res) = {
            |(mut r, path, args, range): (R, String, OpRead, BytesRange)| async move {
                let res = r.copy_from(&path, args.clone(), range).await;

                ((r, path, args, range), res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context((inner, path, args, range))
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }

    async fn abort(&mut self) -> Result<()> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let (backoff, retryable) = self.policy.backoff();

        let (inner, res) = {
            |mut r: R| async move {
                let res = r.abort().await;

                (r, res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context(inner)
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }

    async fn close(&mut self) -> Result<Metadata> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let (backoff, retryable) = self.policy.backoff();

        let (inner, res) = {
            |mut r: R| async move {
                let res = r.close().await;

                (r, res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context(inner)
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }
}

impl<P: oio::List> oio::List for RetryWrapper<P> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let (backoff, retryable) = self.policy.backoff();

        let (inner, res) = {
            |mut p: P| async move {
                let res = p.next().await;

                (p, res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context(inner)
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }
}

impl<P: oio::Delete> oio::Delete for RetryWrapper<P> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let path = path.to_string();
        let args_cloned = args.clone();
        let (backoff, retryable) = self.policy.backoff();

        let (inner, res) = {
            |mut p: P| {
                let path = path.clone();
                let args = args_cloned.clone();
                async move {
                    let res = p.delete(&path, args).await;
                    (p, res)
                }
            }
        }
        .retry(backoff)
        .when(retryable)
        .context(inner)
        .await;

        self.inner = Some(inner);
        res.map_err(|e| e.set_persistent())
    }

    async fn close(&mut self) -> Result<()> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let (backoff, retryable) = self.policy.backoff();

        let (inner, res) = {
            |mut p: P| async move {
                let res = p.close().await;

                (p, res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context(inner)
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }
}

impl<C: oio::Copy> oio::Copy for RetryWrapper<C> {
    async fn next(&mut self) -> Result<Option<usize>> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let (backoff, retryable) = self.policy.backoff();

        let (inner, res) = {
            |mut c: C| async move {
                let res = c.next().await;

                (c, res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context(inner)
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }

    async fn close(&mut self) -> Result<Metadata> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let (backoff, retryable) = self.policy.backoff();

        let (inner, res) = {
            |mut c: C| async move {
                let res = c.close().await;

                (c, res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context(inner)
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }

    async fn abort(&mut self) -> Result<()> {
        use backon::RetryableWithContext;

        let inner = self.take_inner()?;
        let (backoff, retryable) = self.policy.backoff();

        let (inner, res) = {
            |mut c: C| async move {
                let res = c.abort().await;

                (c, res)
            }
        }
        .retry(backoff)
        .when(retryable)
        .context(inner)
        .await;

        self.inner = Some(inner);
        res.map_err(|err| err.set_persistent())
    }
}
