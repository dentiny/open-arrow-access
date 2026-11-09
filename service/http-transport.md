# Custom HTTP transport

## Service and data

Providers use OpenDAL HTTP transport contexts for metadata requests and file
reads. This guide covers the public injection seam for custom TLS/proxy clients
or controlled transports; it is not a corpus provider.

Available without a provider feature. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`Connection::with_context(opendal::OperationContext)` supplies the transport for
subsequent metadata/download calls. The supplied inner transport must return
single-hop responses: Connection adds its own redirect/authentication layer.
`with_connection` / `with_connections` on provider services accept these configured
connections. Requests remain the provider's existing GETs and responses remain
`ApiDocument`/byte readers; injection does not change their schemas.

OpenDAL ranged backends use their own HEAD/206 behavior. Do not wrap raw OpenDAL
requests in Connection's origin-scope policy or add another redirect follower.

## Usage and results

```rust
use open_arrow_access::{Result, service::common::Connection, storage::opendal};

async fn read_with_transport(context: opendal::OperationContext) -> Result<()> {
    let api = Connection::new("https://api.figshare.com")?.with_context(context);
    let page = api.json("v2/articles", &[("page_size", "1")]).await?;
    println!("metadata through supplied transport: {}", page.value());
    Ok(())
}
```

The caller supplies an OperationContext configured with its single-hop
transport. The example obtains the same parsed metadata shape as the default
connection; only transport configuration changes.
