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
`Error::http_status()` and textual `retry_after()` when available; callers decide
retry/pacing. Downloads follow redirects with origin-scoped authentication.
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
