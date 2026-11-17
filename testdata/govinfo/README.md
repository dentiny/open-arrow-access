# govinfo response fixtures

Project-authored API responses for offline provider tests. These documents preserve
the serialized inputs previously embedded in the tests; they contain no downloaded
upstream content. Other files in this directory cover successful metadata/content
resolution.

| Fixture | Purpose |
| --- | --- |
| `empty-packages.json` | Valid empty package page. |
| `bad-package-identity.json` | Package identity does not match the request. |

Run the owning suite:

```sh
cargo test -p open-arrow-access-service --features govinfo --test govinfo
```
