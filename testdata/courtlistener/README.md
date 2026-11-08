# CourtListener bulk fixture

Source: [official courts export](https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/bulk-data/courts-2026-09-30.csv.bz2), retrieved 2026-11-08.

`courts-2026-09-30.csv` contains the original header and first three complete
records (`nc`, `minnag`, `arizsuper`), copied without rewriting CSV fields or
line endings. The first record includes a multiline notes field. Unquoted NULLs
and quoted empty strings remain distinct.

The `.csv.bz2` file is this excerpt recompressed once with Python
`bz2.compress(sample, compresslevel=9)`. It is not the complete upstream dump.
The original compressed download has SHA-256:
`f33e4c12e5a1d73574f7ecaafa8676887321dccae3ea29cc91d6a5935508baf3`.

Tests read these committed files offline and compare decoded bytes exactly.
Discovery tests use synthetic filenames for date/table filtering cases.
The [upstream documentation](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data)
marks bulk data as free of known copyright restrictions.
