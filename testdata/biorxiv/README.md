# biorxiv response fixtures

Project-authored API responses for offline provider tests. These documents preserve
the serialized inputs previously embedded in the tests; they contain no downloaded
upstream content. Other files in this directory cover successful metadata/content
resolution.

| Fixture | Purpose |
| --- | --- |
| `details.json` | Paginated details with two records and a lazy JATS link; ORIGIN is replaced only with the local fixture host. |
| `empty-window.json` | A valid no-posts window. |
| `bad-cursor.json` | Response cursor does not match the requested cursor. |
| `bad-count.json` | Advertised remaining records with an empty collection. |
| `bad-status.json` | API error status. |

Run the owning suite:

```sh
cargo test -p open-arrow-access-service --features biorxiv --test biorxiv
```
