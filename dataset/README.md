# Dataset extension point

Reserved for the directory/FileSet PR. There is no dataset implementation or
Cargo feature yet. Compose existing record accessors behind the shared
`Accessor` / `Reader` contracts; add optional object discovery via `ByteAccessor`.
Default to fixed schema and sequential, ordered emission. See `docs/roadmap.md`.

The [source-attribute proposal](../docs/source-attributes.md) separates file
independence from emission order. Resolve dataset-wide ordering constraints
before admitting unordered emission; an ATIF child requires global order for
the composed scan. Undefined listing order is not an authoritative file sequence.
