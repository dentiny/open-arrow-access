# XML format fixtures

Project-authored fictional inputs under the repository MIT license.

| Input | Case |
| --- | --- |
| `catalog.xml` | Five products, inherited default namespace, UTF-8 `Café Mug`, `&amp;`, decimals, optional category; projection/batching. |
| `inventory.xml` | Two warehouses, two racks each, two devices each; inferred attributes, nested structs/lists, sampling/replay, limits and order. |
| `bad_integer.xml` | First warehouse ID changed from `0` to `invalid`; fails explicit integer conversion. |
| `bad_entity.xml` | ID changed to `&unknown;`. |
| `bad_truncated.xml` | Missing closing `</inventory>`. |
| `bad_dtd.xml` | Added `<!DOCTYPE inventory>` after the declaration. |

Invalid files are deliberate variations of `inventory.xml`. No external XML
snapshots or downloads are required.

## Use these inputs

Facade feature: `format-xml`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-file-format --no-default-features --features xml
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
