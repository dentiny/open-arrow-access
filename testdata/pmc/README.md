# Synthetic PMC article metadata

`metadata.json` is an invented article-version object with upstream fields,
content URLs, a numeric PMID, Unicode, unknown large numbers, and duplicate keys.
There are no content fixtures: the article service reads only the supplied JSON.
Tests also cover missing/null PDF URLs, manuscript and non-open-access flags,
optional fields, arbitrary URL metadata, and exact raw JSON preservation.

`metadata.json.gz` is the same JSON compressed with `gzip.compress(data, mtime=0)`
for composition and decoded-limit tests. All fixtures are synthetic and local.
