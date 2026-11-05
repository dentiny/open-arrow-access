# Dataset services

Reserved for provider-specific discovery and schema conventions, starting with
Hugging Face. No service implementation or feature exists in the initial PR.
Use the shared object/reader contracts and existing format accessors; keep
repository revision, config and split semantics in the service adapter.

The [source-attribute proposal](../docs/source-attributes.md) defines provider
constraints and conditional planning. Sequential cursor-based file discovery
can coexist with independent file reads; dependent API page requests require
a different plan. Services retain their own ordering requirements while
delegating discovered representations to dataset and format accessors.
