# XML fixtures

These XML files were written specifically for this project's adapter tests.
They use fictional data and are covered by the repository's MIT license.
No third-party XML snapshots, downloads or external fixture licenses are needed.

## Valid inputs

- `catalog.xml`: five products in an inherited default namespace. Covers
  namespace bindings, UTF-8 text (`Café Mug`), a built-in entity (`&amp;`),
  decimal values, an optional category, projections and multiple row batches.
- `inventory.xml`: two warehouses, each containing two racks with two devices
  each. Covers inferred integer attributes, nested structs/lists, bounded
  sampling and replay, byte limits, source errors and record ordering.

## Invalid inputs

Each `bad_*.xml` file is a small, intentional modification of `inventory.xml`:

- `bad_integer.xml`: replace the first warehouse's `id="0"` with `id="invalid"`.
  This is valid XML but fails conversion with an explicit integer mapping.
- `bad_entity.xml`: replace that same ID with `&unknown;`.
- `bad_truncated.xml`: remove the closing `</inventory>` tag.
- `bad_dtd.xml`: insert `<!DOCTYPE inventory>` after the XML declaration.

## Test fixture policy

Keep XML input in committed `.xml` files, including small, hand-crafted cases.
Use fictional, project-authored data with clear expected values. Add or adjust
fixtures for the behavior under test and document deliberate malformed inputs
here. Tests read these local files; they do not download data or construct XML
strings at runtime. The in-memory byte accessor only controls chunk boundaries
and injected I/O failures.
