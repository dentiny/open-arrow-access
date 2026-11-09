# Parsed API metadata

## Service and data

`ApiDocument` is the common JSON metadata response used by many HTTP providers.
It is a library utility, not another dataset service. It retains parsed known and
unknown fields; file bytes are handled separately.

Available without a provider feature. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`ApiDocument::from_bytes(Bytes)` parses a JSON value and returns
`Result<ApiDocument>`. `value() -> &serde_json::Value` exposes its object, array
or scalar structure. `Connection::json(path, query)` fetches and parses one HTTP
response into the same type. Parsing releases the input buffer and does not
preserve original whitespace or duplicate-key spelling. It does not define a
provider-wide row schema or automatically produce Arrow.

## Usage and results

```rust
use open_arrow_access::{Bytes, Result, service::common::ApiDocument};

fn inspect_metadata() -> Result<()> {
    let document = ApiDocument::from_bytes(Bytes::from_static(
        br#"{"id":42,"title":"Example","extra":{"license":"CC0"}}"#,
    ))?;
    println!("title: {}", document.value()["title"]);
    println!("license: {}", document.value()["extra"]["license"]);
    Ok(())
}
```

This local example prints `Example` and `CC0` as JSON string values. No HTTP
request or attachment download occurs. To preserve exact XML bytes instead, see
[OAI-PMH responses](oai-protocol.md).
