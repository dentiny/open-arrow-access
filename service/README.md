# Dataset source providers

Provider-specific discovery and schema conventions live in this crate. The
workspace scaffold has no enabled providers or transport dependencies. Each
provider adds its own default-off feature and facade exports.

Providers use the shared object/reader contracts and existing byte, format,
and decompression accessors. Backend configuration stays caller-controlled.
The facade always exposes `storage::{OpenDalAccessor, opendal}`, so callers can
wrap an SDK operator without enabling a provider or a storage feature.
Generic object-store prefix listing lives in `storage`, not in a provider.

The [source-attribute proposal](../docs/source-attributes.md) defines provider
constraints and conditional planning. Sequential cursor-based file discovery
can coexist with independent file reads; dependent API page requests require
a different plan. Services retain their own ordering requirements while
delegating discovered representations to dataset and format accessors.
