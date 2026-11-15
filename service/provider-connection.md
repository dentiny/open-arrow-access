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
| `open(absolute_url, ByteReadOptions)` | Sequential `ByteReader` with conditional range support |
| `files(links, metadata)` | Lazy named `RemoteFiles<M>` |

Metadata and initial download GETs require HTTP 200. Responses with failure status retain
`Error::http_status()` and textual `retry_after()` when available; retries are opt-in as described below. Downloads follow redirects with origin-scoped authentication.
Listing on a bare Connection is unsupported; a resolved file manifest supplies
listing. A Connection implements `ByteAccessor` for direct byte-layer composition.

## Attachment range reads

`open` retains the initial GET body for lazy sequential consumption; it does not
issue a HEAD request or buffer the object. The accessor advertises potential
range support. Each opened reader advertises `read_with_range` only when the
response supplies `Accept-Ranges: bytes`, a known Content-Length and no
non-identity Content-Encoding. Missing/`none` range advertisements and unknown
lengths remain sequential. File requests use `Accept-Encoding: identity`.

`read_range(start..end)` uses a separate GET and leaves the sequential cursor
unchanged, including after EOF. Empty valid ranges perform no I/O; invalid ranges
fail locally. `read_ranges` retains request order and duplicates through the
standard sequential fallback. A partial response must have status 206, an exact
Content-Range (including the original total length), identity encoding and the
exact requested body length. A server ignoring Range with HTTP 200 returns
Unsupported; other HTTP failures preserve status and Retry-After metadata.

The initial ETag is retained in `ByteMetadata::version`. A strong ETag becomes
an `If-Match` precondition on every range request; changed/missing response validators are
rejected. Weak or absent ETags provide no snapshot guarantee, even though range
bounds and total length are still checked. Reopening a source is independent.
Download-owned Range, If-Match, If-Range and Accept-Encoding settings take
precedence over connection headers; provider authentication keeps its existing
origin scope across redirects. Retry and timeout policies apply to range GETs
and their bodies as well as the initial response.

These readers allow native Parquet decoding directly through `RemoteFiles` on
range-capable servers. Outer compression still removes native random access;
there is no implicit whole-object materialization or seek emulation.

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

Policies are opt-in. GET/HEAD retry temporary transport errors and HTTP
408/429/500/502/503/504 with the configured backoff. `Retry-After` seconds/dates
set a minimum wait; a wait above `max_delay` returns the original response.
`operation_timeout` bounds each fetch through headers (including redirects), and
`io_timeout` bounds each body chunk. Body failures never replay a response.
Custom contexts preserve the policy regardless of builder order.

This adapter uses OpenDAL's transport and the same backon library as RetryLayer.
Native operator layers do not cover direct API calls. Caller-owned operators
still use `StorageConfig::apply`; avoid stacking retries on the same requests.
Hugging Face's separate Xet transfer path is outside this HTTP policy.
