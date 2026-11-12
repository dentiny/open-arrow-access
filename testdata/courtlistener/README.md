# CourtListener fixtures

Source: [official courts export](https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/bulk-data/courts-2026-09-30.csv.bz2), retrieved 2026-11-08.

`courts-2026-09-30.csv` preserves the original header and first three complete
records (`nc`, `minnag`, `arizsuper`), including multiline notes, line endings,
unquoted NULLs, and quoted empty strings. The `.csv.bz2` is this excerpt
recompressed with Python `bz2.compress(sample, compresslevel=9)`; it is not the
complete upstream dump.

Original download SHA-256:
`f33e4c12e5a1d73574f7ecaafa8676887321dccae3ea29cc91d6a5935508baf3`.
The [upstream documentation](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data)
marks bulk data as free of known copyright restrictions. Tests compare decoded
bytes exactly; discovery tests use synthetic filenames.

## Use these inputs

Facade feature: `service-courtlistener`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-service --no-default-features --features courtlistener
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
