# library-of-congress response fixtures

Project-authored API responses for offline provider tests. These documents preserve
the serialized inputs previously embedded in the tests; they contain no downloaded
upstream content. Other files in this directory cover successful metadata/content
resolution.

| Fixture | Purpose |
| --- | --- |
| `legacy-item.json` | Valid metadata returned by a legacy item URL. |
| `bad-resource-files.json` | Resource file membership has the wrong JSON type. |

Run the owning suite:

```sh
cargo test -p open-arrow-access-service --features library-of-congress --test library_of_congress
```
