# Extension entry points

Choose the shared entry point for the input and output contract. Keep parser
configuration, source identity checks and domain mappings in the extension.

| Extension | Implement or configure | Shared access path |
| --- | --- | --- |
| Single-file format | `ReaderFactory` | `FileAccessor::from_format(storage, format)` |
| Incremental Arrow decoder | `BatchDecoder` inside its factory | `decode_batches(input, decoder, metadata)` |
| Native asynchronous parser | Map its items/errors inside its factory | `StreamReader::new(stream, metadata)` |
| One whole document to one Arrow row | `DocumentDecoder` | `DocumentAccessor::new(storage, decoder)` |
| Document provider with default decoder settings | `DocumentDecoder + Default` and a type alias | `DefaultDocumentAccessor::new(storage)` |
| OpenDAL storage backend | Configure an OpenDAL `Operator` | `OpenDalAccessor::new(operator)` |

## Document providers

Implement `DocumentDecoder::prepare` to validate settings and return the full
schema. Implement `decode` to validate the document and return one row using the
provided projected schema. `RecordBatchBuilder` supports named metadata fields,
including zero-column projections. See the PMC and Gutenberg decoder mappings.

For a provider with default settings, expose an alias:

```rust,ignore
pub type CatalogAccessor<B> = DefaultDocumentAccessor<B, CatalogDecoder>;

#[derive(Default)]
pub struct CatalogDecoder;

// Implement DocumentDecoder for CatalogDecoder: prepare + decode.
// Callers construct CatalogAccessor::new(storage).
```

The shared accessor validates projection before input I/O, reports the output
schema at `open`, and opens/reads bytes only on the first `next`. It forwards byte
options, owns the storage/decoder needed by each reader, and checks that decoding
returns exactly one row with the declared schema. Cloning either document
accessor shares storage and decoder without requiring either type to be `Clone`.
Each opened reader has independent terminal state.

`PmcAccessor::new(storage)` and `GutenbergAccessor::new(storage)` use this path.
Their decoders can also be composed explicitly:

```rust,no_run
# #[cfg(feature = "service-pmc")]
# fn example<B>(storage: B) {
use open_arrow_access::{DocumentAccessor, service::pmc::PmcDecoder};

let articles = DocumentAccessor::new(storage, PmcDecoder);
# }
```

## File formats

Bind a format directly with `FileAccessor::from_format(storage, format)`. The
factory owns `prepare` and `create_reader`; the shared protocol prepares each
request before opening bytes and forwards its locator and byte options. A
format-specific accessor name can be a thin alias with convenience constructors,
as the built-in CSV, JSON and Parquet accessors demonstrate.

Use `decode_batches` for incremental byte-to-Arrow decoders. Native streams can
use `StreamReader`, applying `project_batch` when needed to restore declared
schema metadata and caller column order. Schema inference, decoder finalization
and range requirements remain format-specific. See the
[format protocol](../README.md#adding-a-file-format) for the complete factory hooks.

Archive sessions also consume the existing `ByteReader` boundary. `TarStream`
keeps sequential entry ownership and cancellation progress, while `TarArchive`
uses `SeekableInput` for independent selected entries. Their entry readers reuse
`StreamReader` and compose with sequential format consumers; archive-specific
header inspection and active-body rules stay in the archive session. Use the
shared byte-layer output/error assertions alongside those lifetime tests.

Within decompression, `ArchiveSource` shares ranged source setup between TAR and
ZIP. `entry_metadata` retains archive identity while assigning decoded entry
lengths for both indexed archives and sequential TAR. Keep format-specific
parsing, limits and entry ownership in their respective modules.

## OpenDAL backends

Configure backend credentials, endpoint, root and transport on its operator,
then wrap that operator once:

```rust,no_run
use open_arrow_access::storage::{OpenDalAccessor, opendal};

# fn example(operator: opendal::Operator) {
let storage = OpenDalAccessor::new(operator);
# }
```

Enable the backend's features on the caller's OpenDAL dependency. The shared
accessor supplies sequential reads, exact ranges, listing, byte options and
reader ownership according to the operator's capabilities. Formats, compression
and dataset selection compose above it; a new OpenDAL backend needs no new byte
reader implementation in this workspace.

Use the existing [test-support contracts](../test-support/README.md) for the
common behavior, and keep parser values, domain validation and backend-specific
capability/scheduling tests with their owning suite.
