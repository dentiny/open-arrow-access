# Documentation

open-arrow-access is for **pretraining data preparation**. It gives data engineers
and developers a shared source-access and decoding interface for turning raw
corpus inputs into Arrow batches for downstream preprocessing.

Start with the [usage guide](usage.md) to build a corpus ingestion reader, or use
the [support matrix](support.md) to check a particular source or representation.
The [README workflow](../README.md#where-it-fits-in-data-preparation) shows how the
library fits before cleaning, deduplication, tokenization, and sample packing.

## Guides

| Document | What it answers |
| --- | --- |
| [Architecture](architecture.md) | How do storage, formats, providers, datasets, and layers compose? |
| [Support matrix](support.md) | What is implemented, which features enable it, and what does it return? |
| [Usage](usage.md) | How do I read files, compressed records, multiple shards, or a provider dataset? |
| [Format behavior](formats.md) | Which layouts, schema policies, and limits apply to each decoder? |
| [Decompression and ZIP](../decompression/README.md) | How do byte layers and selected ZIP entries behave? |
| [Provider guide](../service/README.md) | How do I discover files and preserve provider metadata? |
| [Roadmap](roadmap.md) | Which capabilities are planned and what must precede them? |

## Contributing

| Document | Scope |
| --- | --- |
| [Extending the library](extending.md) | Format factories, document decoders, storage adapters, and layers |
| [API contracts](contracts.md) | Detailed reader lifecycle, capabilities, and factory orchestration |
| [Performance benchmarks](../benchmarks/README.md) | Local CPU, I/O, memory and parser optimization measurements |
| [Testing](testing.md) | Test ownership, fixtures, and commands |
| [Dependency alignment](dependencies.md) | Pinned upstream baseline and update procedure |

## Design proposals

These describe future APIs and behavior; they are not available features.

- [Generated columns](generated-columns.md): expressions, projection dependencies, and output schema.
- [Source attributes and scan planning](source-attributes.md): partition strategies, ordering requirements, and policy admission.
