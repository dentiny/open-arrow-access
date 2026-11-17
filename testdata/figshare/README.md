# figshare response fixtures

Project-authored API responses for offline provider tests. These documents preserve
the serialized inputs previously embedded in the tests; they contain no downloaded
upstream content. Other files in this directory cover successful metadata/content
resolution.

| Fixture | Purpose |
| --- | --- |
| `bad-identity.json` | Mismatched response identity. |
| `bad-embargo.json` | Embargoed response, expected to deny access. |
| `bad-missing-files.json` | Selected record has no required file membership. |

Run the owning suite:

```sh
cargo test -p open-arrow-access-service --features figshare --test figshare
```
