# semantic-scholar response fixtures

Project-authored API responses for offline provider tests. These documents preserve
the serialized inputs previously embedded in the tests; these API fixtures contain
no downloaded upstream content. The reader fixtures below include an official
public sample with its source, license and attribution.

| Fixture | Purpose |
| --- | --- |
| `bad-dataset-identity.json` | Dataset response does not match the selected dataset. |

Run the owning suite:

```sh
cargo test -p open-arrow-access-service --features semantic-scholar --test semantic_scholar
```

## S2ORC v2 reader inputs

`s2orc_v2-sample.jsonl` contains all 20 records from the official public
[Semantic Scholar v2 sample](https://s3-us-west-2.amazonaws.com/ai2-s2ag/samples/s2orc_v2/s2orc_v2-sample.jsonl.gz),
downloaded on 2026-11-19 and gzip-decompressed without changing the JSONL bytes.
The compressed source SHA-256 is
`4833173b3dea488e71cd8450c5c2e2a86fc8cd4b63afb35d78c51dbb517d4c47`.
This is a public sample, not an authenticated full-release shard.

The original upstream license and attribution are preserved verbatim in
[s2orc_v2-upstream-README.txt](s2orc_v2-upstream-README.txt), including the
ODC-BY license and citation for *The Semantic Scholar Open Data Platform*
(Kinney et al., 2023, https://doi.org/10.48550/arxiv.2301.10140).
Per-record open-access metadata and copyright disclaimers are unchanged.

The following additional fixtures are project-authored boundary cases, separate
from the unchanged public sample:

| Fixture | Purpose |
| --- | --- |
| `s2orc_v2-synthetic.json` | Decimal and numeric offsets, typed attributes, null and empty annotation lists. |
| `s2orc_v2-invalid-annotation.json` | Invalid annotation JSON, even when the column is unselected. |
| `s2orc_v2-empty.jsonl` | Empty shard. |
| `s2orc_v2-truncated.json` | Incomplete outer JSON object. |
| `s2orc_v2-minimal.json` | Missing optional structs. |

The reader suite checks the complete public sample, known paper metadata and
annotation counts, projection, zero-column row preservation, and equality of
streaming scan and sequential output. All inputs are local; tests use no network.

```sh
cargo test -p open-arrow-access-service --features semantic-scholar --test s2orc_v2 --test semantic_scholar --offline
```
