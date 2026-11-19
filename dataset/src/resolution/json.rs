use std::num::NonZeroUsize;

use open_arrow_access_core::{
    ByteAccessor, ByteReadOptions, ByteReadRequest, Error, ErrorKind, ObjectRef, ReaderFactory,
    ReaderFactoryExt, Result,
};
use open_arrow_access_decompression::CompressionFactory;
use serde_json::Value;

use open_arrow_access_file_format::DetectedFormat;

pub(super) async fn detect<B: ByteAccessor>(
    storage: &B,
    object: &ObjectRef,
    mut options: ByteReadOptions,
    limit: NonZeroUsize,
) -> Result<Option<DetectedFormat>> {
    let limit = limit.get();
    options.io_chunk_size = NonZeroUsize::new(
        options
            .io_chunk_size
            .map_or(limit, |size| size.get().min(limit)),
    );
    let factory = CompressionFactory::default().with_chunk_size(NonZeroUsize::new(limit).unwrap());
    // Filename detection ignores URL suffixes, but storage must receive the
    // complete signed locator. Use the same path for codec selection only.
    let path = object
        .locator()
        .split(['?', '#'])
        .next()
        .unwrap_or_default();
    let codec_request = ByteReadRequest::new(path).with_options(options);
    factory.prepare(&codec_request)?;
    let input = storage
        .open(ByteReadRequest::new(object.clone()).with_options(options))
        .await?;
    let mut input = factory.open_input(input, codec_request).await?;
    let mut sample = Vec::new();
    let mut next_parse = 1;
    loop {
        let next = input.next().await?;
        let eof = next.is_none();
        if let Some(bytes) = next {
            if bytes.is_empty() {
                return Err(error(
                    object,
                    ErrorKind::Decode,
                    "byte reader returned an empty chunk",
                ));
            }
            sample.extend_from_slice(&bytes[..bytes.len().min(limit - sample.len())]);
        }
        let at_limit = sample.len() == limit;
        if eof || at_limit || sample.len() >= next_parse {
            // Geometric retry sizes bound total parsing work even with one-byte chunks.
            next_parse = sample.len().saturating_mul(2).max(1);
            let mut records = serde_json::Deserializer::from_slice(&sample).into_iter::<Value>();
            match records.next() {
                Some(Ok(value)) => {
                    return Ok(Some(
                        if value
                            .get("schema_version")
                            .and_then(Value::as_str)
                            .is_some_and(|version| version.starts_with("ATIF-"))
                        {
                            DetectedFormat::Atif
                        } else {
                            DetectedFormat::Json
                        },
                    ));
                }
                Some(Err(source)) if !source.is_eof() || eof => {
                    // JSON readers also accept extensions such as bare NaN.
                    // Only a successfully parsed ATIF marker selects ATIF; leave
                    // all other syntax and type validation to the chosen reader.
                    return Ok(Some(DetectedFormat::Json));
                }
                None if eof => return Ok(None),
                _ => {}
            }
        }
        if at_limit {
            return Err(error(
                object,
                ErrorKind::InvalidInput,
                "no complete JSON record within the probe byte limit",
            ));
        }
    }
}

fn error(object: &ObjectRef, kind: ErrorKind, message: &str) -> Error {
    Error::new(kind, "probe_json_format", message).with_object(Some(object))
}
