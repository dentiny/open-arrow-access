# openstax response fixtures

Project-authored API responses for offline provider tests. These documents preserve
the serialized inputs previously embedded in the tests; they contain no downloaded
upstream content. Other files in this directory cover successful metadata/content
resolution.

| Fixture | Purpose |
| --- | --- |
| `bad-unpinned-commit.json` | Branch name returned instead of a pinned commit SHA. |

Run the owning suite:

```sh
cargo test -p open-arrow-access-service --features openstax --test openstax
```
