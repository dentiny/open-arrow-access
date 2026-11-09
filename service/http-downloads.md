# Sequential HTTP downloads

## Service and data

The common HTTP download path reads original files advertised by providers,
including absolute signed URLs. It is a byte transport utility; returned data
can be PDFs, XML, gzip archives or any other representation.

Available without a provider feature. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`Connection::open(url, ByteReadOptions)` sends a GET and returns
`Box<dyn ByteReader>`. `next().await` yields nonempty `Bytes` chunks followed by
EOF. Chunks need not align with records, UTF-8 characters or complete files.

Byte metadata has optional HTTP Content-Length and ETag-derived version, with
`source = None`. Declared lengths are checked during consumption. Output is
sequential: range reads and listing are unsupported. `io_chunk_size` bounds
emitted chunks; this path does not implement concurrent HTTP range fetching.
Signed query values are preserved; configured provider credentials are not sent
to a different origin. Use ranged OpenDAL storage for Parquet/ZIP consumers.

## Usage and results

```rust
use open_arrow_access::{ByteReadOptions, Result, service::common::Connection};

async fn read_original_bytes(url: &str) -> Result<()> {
    let mut reader = Connection::new("https://example.org")?
        .open(url, ByteReadOptions::default())
        .await?;
    println!("declared length: {:?}", reader.metadata().content_length);
    if let Some(chunk) = reader.next().await? {
        println!("first original chunk: {} bytes", chunk.len());
    }
    Ok(())
}
```

Pass a real download URL. The example reads at most one chunk; continue calling
`next()` to consume the whole object. It does not parse/decompress the bytes or
write them to disk.
