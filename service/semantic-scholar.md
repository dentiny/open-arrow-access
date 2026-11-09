# Semantic Scholar bulk data

Enable `service-semantic-scholar`. Construct `SemanticScholarService::new(key)`
with an explicit API key. `dataset("latest","s2orc")` first pins the latest
release ID and then obtains signed partition URLs using that fixed ID. An explicit
release works too. The resolved metadata contains the release ID and both parsed
release/dataset API responses, including README and licensing fields.

Files have stable manifest-order names `part-00000`, `part-00001`, etc. These are
opaque names; the upstream signed URL remains the actual locator. Downloads are
lazy sequential GETs, retain the exact signature, and never send the API key to
another origin. Compose gzip/JSON readers according to the selected dataset.
Signed URLs can expire; resolve the same pinned release again to obtain fresh URLs.

`release(id)` exposes release metadata. `diffs(dataset,start,end)` returns the parsed
incremental manifest with update/delete lists unchanged; applying a diff remains
with the caller. No deletion or filtering is performed by this reader.
Pacing, retry timing, ingestion and Lance conversion remain caller-owned.

Source: [Datasets API](https://api.semanticscholar.org/api-docs/datasets).
