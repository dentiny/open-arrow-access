# Dataset extension point

Reserved for the directory/FileSet PR. There is no dataset implementation or
Cargo feature yet. Compose existing record accessors behind the shared
`Accessor` / `Reader` contracts; add optional object discovery via `ByteAccessor`.
Default to fixed schema and sequential, ordered emission. See `docs/roadmap.md`.
