# uspto response fixtures

Project-authored API responses for offline provider tests. These documents preserve
the serialized inputs previously embedded in the tests; they contain no downloaded
upstream content. Other files in this directory cover successful metadata/content
resolution.

| Fixture | Purpose |
| --- | --- |
| `bad-empty-products.json` | Missing selected product in an empty product bag. |
| `bad-identity.json` | Mismatched response identity. |
| `bad-missing-files.json` | Selected record has no required file membership. |

Run the owning suite:

```sh
cargo test -p open-arrow-access-service --features uspto --test uspto
```
