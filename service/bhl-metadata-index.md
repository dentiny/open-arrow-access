# AWS metadata index

Enable `service-bhl-metadata` to prepare one immutable SQLite snapshot from the
13 `data/*.txt.gz` TSV tables. `metadata::prepare(storage, source_root, path)`
streams existing gzip/TSV readers, validates headers against the fixed schema,
and preserves lexical cells. Completed tables resume from the adjacent
`.building.sqlite` file; run one preparer per output. Publication is exclusive.

`metadata::inspect(path)` reads source paths, row counts and content fingerprints.
`BhlMetadataIndex::open(path)` opens the completed file read-only;
`rows(table, Some(id))` queries one numeric source key in original row order.
Rows retain lexical IDs and duplicate relation rows. Singleton numeric key
collisions fail preparation; missing/incomplete snapshots fail opening. This
layer stores and reads table rows. Mapping those rows to page/item/title/rights
is a separate metadata-join change.

Paths must be absolute and identical on workers sharing the snapshot. Keep
published files immutable. Fingerprints hash canonical decoded JSON cell rows,
not compressed S3 bytes or AWS versions. Acquisition spans a time window,
including resumed tables. Local index I/O belongs on a blocking worker.
