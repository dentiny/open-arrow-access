# regulations response fixtures

Project-authored API responses for offline provider tests. These documents preserve
the serialized inputs previously embedded in the tests; they contain no downloaded
upstream content. Other files in this directory cover successful metadata/content
resolution.

| Fixture | Purpose |
| --- | --- |
| `bad-record-identity.json` | Record identity does not match the request. |

Run the owning suite:

```sh
cargo test -p open-arrow-access-service --features regulations --test regulations
```
