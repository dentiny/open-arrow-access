# Synthetic PMC metadata fixtures

`metadata.json` is an invented PMC article-version object with a numeric PMID,
encoded PDF URL, non-ASCII title, and unknown fields. The URL is preserved as
metadata; there is deliberately no corresponding PDF fixture. Tests assert that
only metadata objects are opened.

No real article content or public network is required. Layout follows the
[PMC Article Datasets README](https://pmc-oa-opendata.s3.amazonaws.com/README.txt).

`metadata.json.gz` encodes the same metadata for composition tests of the existing
gzip byte layer. Regenerate with Python `gzip.compress(data, mtime=0)`.
