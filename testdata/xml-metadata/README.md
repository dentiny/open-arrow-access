# Service XML fixtures

Project-authored fictional metadata envelopes under the repository MIT license.
External DTD declarations must not trigger network fetches.

| Input | Case |
| --- | --- |
| `namespaced-header.xml` | Prefixed manifest, declaration, comment, external DTD. |
| `oai.xml` | OAI-PMH envelope with an empty ListRecords element. |
| `namespaces-entities.xml` | Namespaces, external DTD, and `&amp;` decoded as `A & B`. |
| `wrong-root.xml` | Valid XML with a root other than `manifest`. |
| `empty.xml`, `multiple-roots.xml`, `truncated.xml` | No root, multiple roots, or missing closing tag. |
| `trailing-entity.xml` | Unknown entity after the closed root. |
| `internal-dtd.xml` | Unsupported internal DTD subset. |

Several inputs intentionally have no final newline.

## Use these inputs

Facade feature: `format-xml`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-service --no-default-features --features format-xml
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
