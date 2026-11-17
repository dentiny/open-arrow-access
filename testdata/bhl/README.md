# bhl response fixtures

Project-authored API responses for offline provider tests. These documents preserve
the serialized inputs previously embedded in the tests; they contain no downloaded
upstream content. Other files in this directory cover successful metadata/content
resolution.

| Fixture | Purpose |
| --- | --- |
| `bad-api-denial.json` | API-level unauthorized response despite HTTP success. |

Run the owning suite:

```sh
cargo test -p open-arrow-access-service --features bhl --test bhl
```
