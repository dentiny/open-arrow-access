# Provider HTTP connections

## Service and data

`common::Connection` supplies origin-scoped HTTP access for provider metadata
and original downloads. It carries endpoint, header/query authentication and
transport settings; it does not itself contain a corpus catalogue.

Available without a provider feature. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Request | Response |
| --- | --- |
| `Connection::new(origin)` | Connection configuration; no HTTP request |
| `with_header(name, value)` / `with_query(name, value)` | Explicit origin-scoped credentials/settings |
| `json(relative_path, query)` / `json_url(same_origin_url)` | `ApiDocument` |
| `bytes(url, accept)` | Fully collected `Bytes` for small metadata |
| `open(absolute_url, ByteReadOptions)` | Sequential `ByteReader` |
| `files(links, metadata)` | Lazy named `RemoteFiles<M>` |

Metadata/sequential GETs require HTTP 200. Responses with failure status retain
`Error::http_status()` and textual `retry_after()` when available; retries are opt-in as described below. Downloads follow redirects with origin-scoped authentication.
Listing on a bare Connection is unsupported; a resolved file manifest supplies
listing. A Connection implements `ByteAccessor` for direct byte-layer composition.

## Usage and results

```rust
use open_arrow_access::{Result, service::common::Connection};

async fn read_one_metadata_page() -> Result<()> {
    let api = Connection::new("https://api.figshare.com")?;
    let page = api
        .json("v2/articles", &[("page", "1"), ("page_size", "1")])
        .await?;
    println!("first public article page: {}", page.value());
    Ok(())
}
```

The example returns a parsed JSON page. It does not use a Figshare-specific
feature or resolve attachment membership. For provider identity/file validation,
prefer the corresponding typed provider service.

### Retry and timeout policy

`with_http_config(StorageConfig)` configures metadata and sequential downloads
through the same OpenDAL `HttpTransporter` used by the connection. The same
method is available on Zenodo, Dataverse, Internet Archive and Hugging Face
services for their metadata and provider-managed file requests. Other services
accept the configured connection through `with_connection` / `with_connections`.

```rust
use open_arrow_access::{
    Result, service::common::Connection,
    storage::{RetryConfig, StorageConfig, TimeoutConfig},
};

fn configured() -> Result<Connection> {
    Connection::new("https://api.figshare.com")?.with_http_config(StorageConfig {
        retry: Some(RetryConfig::default()),
        timeout: Some(TimeoutConfig::default()),
    })
}
```

Policies are opt-in; defaults preserve one attempt and no added timeout. Only
GET/HEAD requests are retried, on temporary transport errors or HTTP
408/429/500/502/503/504. `max_retries` excludes the first attempt. Exponential
backoff, factor and jitter use the same `backon` library as OpenDAL's RetryLayer.
A valid `Retry-After` delta-seconds or HTTP-date supplies a minimum wait. Invalid
values use normal backoff. A server wait above `max_delay` ends retries and
returns the response instead of retrying earlier than requested. The final
metadata error retains its HTTP status and original header.

`operation_timeout` bounds each fetch attempt through response headers, including
redirects; `io_timeout` bounds each next response-body chunk. Backoff sleeps are
outside the attempt timeout. Once headers succeed, body failures propagate
without replaying the request, including for buffered metadata. Dropping the
request/reader cancels its active wait; no background retries are spawned.
`with_context` keeps these policies regardless of builder order.

This is a policy adapter on OpenDAL's transport plane, not a second HTTP client.
OpenDAL's HTTP file backend constructs file paths, HEAD and Range requests;
provider APIs need exact URLs, scoped credentials and original response headers.
Native `RetryLayer`/`TimeoutLayer` apply to operators, not direct transport calls.
Caller-supplied operators still use `StorageConfig::apply`. Avoid adding native
retry layers to operators already using this HTTP retry policy, which would
multiply attempts. Hugging Face's separate Xet transfer path, when selected by
OpenDAL, is outside this HTTP transport policy.
