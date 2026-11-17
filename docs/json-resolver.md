# JSON content resolution

`AutoFormatAccessor` selects a file format. `JsonResolver` independently decides
which content type exists within JSON. ATIF is a JSON content type, not a sibling
of CSV, Parquet, or WARC in the automatic file-format hierarchy.

| Stage | Owner | Result |
| --- | --- | --- |
| Discover objects and select a file format | `AutoFormatAccessor` | `ResolvedKind::Json`, `CsvOrTsv`, `Parquet`, or `Warc` |
| Identify content within JSON | `JsonResolver` | `JsonKind::Records` or `JsonKind::Atif(AtifFormat)` |
| Select JSON inputs and optionally check all content kinds | `JsonResolver` | Ordered objects; `EveryFile` validates all kinds and omits all empty files |
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

The default `FirstFile` policy uses sequential probes to find the first nonempty
file, then trusts all later objects without opening them. Empty inputs before
that file are omitted; later inputs remain unchecked, including empty files.
An all-empty selection is `NotFound`.

Opt into `EveryFile` to process bounded concurrent probes as they complete, then
restore input order by original index, preserving duplicates. This policy checks
every file, omits whitespace-only inputs, and rejects mixed ordinary JSON, ATIF
JSONL, and ATIF document inputs before schema union.

Type identifiers and content classification are available without reader features.
`open_arrow_access_file_format::AtifFormat` is independent of the feature-gated
`atif` reader module; the existing `atif::AtifFormat` path remains available when
that reader feature is enabled.
Creating a decoder requires the corresponding `format-json` or `format-atif`
feature and otherwise reports `Unsupported`. ATIF uses its fixed schema and
rejects explicit schema overrides.

## Extension boundary

Future content resolvers belong to their parent file format and can own their
own policies. They do not require adding subtypes or format-specific configuration
to `AutoFormatAccessor`. Registering entirely new file formats remains a separate
[planned registry](extending.md#custom-format-registration-proposed).
