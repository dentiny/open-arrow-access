# JSON content resolution

`AutoFormatAccessor` selects a file format. `JsonResolver` independently decides
which content type exists within JSON. ATIF is a JSON content type, not a sibling
of CSV, Parquet, or WARC in the automatic file-format hierarchy.

| Stage | Owner | Result |
| --- | --- | --- |
| Discover objects and select a file format | `AutoFormatAccessor` | `ResolvedKind::Json`, `CsvOrTsv`, `Parquet`, or `Warc` |
| Identify content within JSON | `JsonResolver` | `JsonKind::Records` or `JsonKind::Atif(AtifFormat)` |
| Check consistency across selected files | `JsonResolver` | Ordered nonempty objects with one content kind |
| Decode and plan records | Native JSON/ATIF reader and `FileSetAccessor` | Arrow batches with dataset schema and ordering policies |

## Ownership and configuration

The automatic accessor retains storage, an optional file-format override, and an
optional schema. It delegates JSON content resolution using default settings;
it does not store JSON detection flags or sniff limits. The resolver owns schema
configuration and `SniffConfig` (policy, maximum consumed bytes per file, and probe
concurrency). Applications needing custom probing compose the resolver directly:

```rust,ignore
let decoded: Arc<dyn ByteAccessor> = Arc::new(
    AutoDecompressionLayer::new().layer(storage),
);
let resolver = JsonResolver::default()
    .with_schema_opt(schema)
    .with_sniff_config(SniffConfig {
        policy: SniffPolicy::EveryFile,
        max_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        concurrency: NonZeroUsize::new(8).unwrap(),
    });
let selected = resolver.resolve(decoded.clone(), &request).await?;
let reader = resolver.reader(decoded, selected.kind, selected.objects.first())?;
let plan = FileSetAccessor::new(reader)
    .with_union_by_name()
    .with_file_planning()
    .plan_scan(FileSetRequest { objects: selected.objects, ..request })
    .await?;
```

The resolver receives already selected objects and decoded storage. It neither
recognizes suffixes nor configures compression. Original object identities and
byte options reach storage unchanged. Probe readers are separate from execution
readers, so input bytes remain available to native decoders.

## Detection and validation

Probe the first complete nonblank JSON value within a consumed-byte budget.
A top-level `schema_version` beginning with `ATIF-` selects ATIF; multiline
objects select document framing, while one-line objects select JSONL framing.
Incomplete first lines alone are not evidence of ATIF. Ordinary pretty-printed
JSON remains ordinary JSON; this resolver does not broaden the native JSON
reader's supported framing. Malformed or truncated input reports a decode error
with the original object context. Native readers validate full records later.

The default policy checks every selected file, preserving order and duplicates,
and rejects mixed ordinary JSON, ATIF JSONL, and ATIF document inputs before
schema union. Whitespace-only files are omitted; an all-empty selection is
`NotFound`. `FirstFile` uses sequential probes to find the first nonempty file,
then trusts all later objects without opening them. It is an explicit opt-out
from cross-file content validation, not automatic behavior.

Type identifiers and content classification are available without reader features.
Creating a decoder requires the corresponding `format-json` or `format-atif`
feature and otherwise reports `Unsupported`. ATIF uses its fixed schema and
rejects explicit schema overrides.

## Extension boundary

Future content resolvers belong to their parent file format and can own their
own policies. They do not require adding subtypes or format-specific configuration
to `AutoFormatAccessor`. Registering entirely new file formats remains a separate
[planned registry](extending.md#custom-format-registration-proposed).
