# Real XML fixtures

These are complete, unmodified configuration/topology files from public projects. They are real project files rather than generated XML test
snippets or a large document corpus. The XML files are stored directly in Git.

## LanceDB Maven POM

- File: `lancedb-pom.xml` (15,205 bytes).
- Source: [lancedb/lancedb, java/pom.xml](https://github.com/lancedb/lancedb/blob/1ea47a50559f02a0251d9d43237259b559606abd/java/pom.xml).
- Git blob: `33446040b5e0532c945ee3f4c718563dca5250a3`.
- SHA-256: `b4f82c7efc8b448cd59b1fa4ed84473cc5658e716c30b9adc76f7f82cb592c8c`.
- License: [Apache-2.0](licenses/Apache-2.0.txt).
- Structure: default XML namespace, dotted element names, nested properties,
  dependency/module lists, optional fields, and literal Maven placeholders.

## Azure NDv5 hardware topology

- File: `azure-ndv5-topology.xml` (3,395 bytes).
- Source: [NVIDIA/NVSentinel, ndv5-topo.xml](https://github.com/NVIDIA/NVSentinel/blob/09cf75f8bc27dc8dcafa67354cf879f7d0ff1858/distros/kubernetes/nvsentinel/charts/preflight/files/ndv5-topo.xml).
- Git blob: `02fa383a75775f7b55a235a2926db6aedc5121dd`.
- SHA-256: `7d29707d077c654121988fad48b3fa70df7a3e3d36bd00abb2ba47696ffed5e1`.
- The source comment credits [Azure/azhpc-images](https://github.com/Azure/azhpc-images/blob/master/topology/ndv5-topo.xml) and is retained. NVSentinel uses
  [Apache-2.0](licenses/Apache-2.0.txt); Azure's original project uses
  [MIT, copyright Microsoft Corporation](licenses/Azure-MIT.txt).
- Structure: attribute-heavy CPU → PCI bridge → device hierarchy
  (2 CPUs, 8 bridges, 16 devices).

These snapshots match the corresponding fixtures in `dentiny/xml-arrow-rs`;
the hashes above were verified when copying. Refresh the source revision and hashes together
when deliberately updating a fixture.

## Adapter tests

The XML adapter tests use these unmodified snapshots for namespace matching,
fragmented byte streams, field projection, ordered batches, nested PCI lists,
fixed schemas, conversion errors and XML byte/depth limits.

The four `bad_*.xml` files are deliberate corruptions of the topology snapshot,
not additional real datasets: replace the first `numaid="0"` with `invalid` or
`&unknown;`, remove the closing system tag, or prepend `<!DOCTYPE system>`.
These exercise terminal conversion, syntax, and unsupported-DTD errors.
Tests read committed local files and make no network requests.

## Test fixture policy

Use committed, real XML files for XML adapter tests, including configuration,
projection, batching and limit tests. Prefer reusing these snapshots; when adding
a new source, record its pinned revision, SHA-256 hash and license here.

For malformed XML that cannot occur in an unmodified valid snapshot, make the
smallest corruption of a real fixture and document the exact change. Keep the
original snapshot unmodified. Reserve synthetic input for a narrowly targeted
case that cannot be covered by real files or their documented corruptions, and
explain why the exception is needed in the test.
