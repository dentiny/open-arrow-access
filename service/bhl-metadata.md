# Direct AWS TSV metadata joins

Enable `service-bhl-metadata`. `BhlMetadata::load(storage).await` reads the 13
`data/*.txt.gz` tables from bucket-root storage. Anonymous live storage can use
`OpenDalAccessor::new(open_data_bucket_operator()?)`; pinned test/mirror storage
retains the same AWS keys. No database, staging file or persisted index is used.

The gzip/TSV reader validates exact headers against the fixed 104-field schema.
Rows stream one Arrow batch at a time directly into in-memory lookup maps;
there is no complete decompressed-table buffer or collection of input batches.
All retained source rows must fit memory. Load once and share the result across
page selections/jobs rather than fetching the tables for every page or job.

`metadata.select(&page_keys)` returns populated metadata for selected OCR page
identities. Supply `move |identity| selected.get(identity)` to
`BhlPageAccessor::new` and attach `selected.schema_metadata()` using
`with_schema_metadata` before moving `selected` into the closure.

Item pages join page/item/title; part pages also join part and matching partpage.
The parent item resolves from part, partpage, then page; the part's page sequence
comes from partpage, not part.SequenceOrder or the item page order. Authors,
identifiers, subjects, names and DOI relations retain source order and duplicate
rows. DOI matching includes entity type and numeric ID. Item and part rights
remain separate original fields. Lexical IDs, dates, quotes, Unicode and empty
strings are preserved. Numeric singleton collisions such as 001 and 1 fail load.

Absent singleton rows become null children and absent relations become empty
lists; the outer metadata struct is populated. Missing required joins and
conflicting parent/sequence values appear in `BhlMetadataReport` and
`bhl.metadata.reconciliation`. They retain the OCR record instead of failing or
silently dropping it; callers decide whether a report permits publishing their
conversion. This is not a claim that every upstream page has complete metadata.
Invalid source headers/keys, missing tables and source/decoder failures fail load;
requests outside the selected page set fail the metadata provider.

`BhlMetadataInfo` records source paths, row counts and SHA-256 hashes of decoded
lexical cell rows, with a combined fingerprint. These are content hashes, not
compressed-object hashes or AWS version IDs. Acquisition spans a time window;
it does not promise an atomic AWS export. Preserve that fingerprint and the
source provenance with conversion jobs. Reuse a loaded value for consistent
selections, and acquire from caller-pinned storage when reproducibility requires
an immutable source snapshot.
