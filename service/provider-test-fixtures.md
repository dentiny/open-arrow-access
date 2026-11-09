# Provider HTTP fixtures

## Service and data

`open-arrow-access-test-support` provides synthetic HTTP metadata/file responses
for workspace development. It is unpublished and is not a production storage
service. Fixtures let examples/tests exercise provider calls without real data
access, credentials or network requests.

Available without a provider feature. In workspace development, add `open-arrow-access-test-support` as a path dev-dependency and `http = "1"`; it is not available as a published consumer dependency.

## Request and response

`HttpFixture::new()` creates an isolated endpoint. `respond(path, status, bytes)`
registers a metadata response, `file(path, bytes)` registers HEAD/GET/range file
behavior, and `requests()` exposes captured methods/paths/headers. Configure the
Connection with `OperationContext::new()` after installing a fixture so its
requests use the fixture transport. The resulting public API still returns
`ApiDocument` or byte readers, matching the real provider path.

## Usage and results

```rust
use open_arrow_access::{Result, service::common::Connection, storage::opendal::OperationContext};
use open_arrow_access_test_support::http::HttpFixture;

async fn inspect_fixture() -> Result<()> {
    let fixture = HttpFixture::new();
    fixture.respond("/metadata", http::StatusCode::OK, r#"{"id":42}"#);
    let api = Connection::new(fixture.endpoint())?.with_context(OperationContext::new());
    let document = api.json("metadata", &[]).await?;
    println!("fixture ID: {}", document.value()["id"]);
    println!("captured requests: {}", fixture.requests().len());
    Ok(())
}
```

The example obtains ID `42` and captures one request without contacting an
upstream provider. It is intended for this workspace's development environment.
