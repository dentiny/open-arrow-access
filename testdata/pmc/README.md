# Synthetic PMC fixtures

`metadata.json` is an invented PMC article-version object with a numeric PMID,
encoded PDF locator, non-ASCII title and unknown fields. `article.pdf` is a
synthetic PDF header followed by a comment, not a complete PDF. The source
validates PDF headers only and preserves the original bytes.

No real article content or public network is required. Layout follows the
[PMC Article Datasets README](https://pmc-oa-opendata.s3.amazonaws.com/README.txt).

The `.gz` files encode the same fixtures for composition tests of the existing
gzip byte layer. Regenerate with Python `gzip.compress(data, mtime=0)`.
