# Biodiversity Heritage Library

Enable `service-bhl`. `BhlService::new(None)?.bulk_export(Some(version))` reads
BHL OCR export article `21422193` through the shared Figshare service. This path
needs no BHL key and preserves the selected export version and original files.

`new(Some(key))` configures API v3 query authentication. `get(operation,parameters)`
exposes supported read-only title/item/page/part metadata, collections and statistics
calls as parsed JSON metadata. Add documented method parameters to include pages, OCR,
parts or other metadata. `item(id)` additionally resolves the advertised text,
PDF and image archive URLs into independent lazy sequential byte readers. Missing
PDFs leave the metadata intact and do not remove an item. API-level unauthorized
responses become permission errors, even when HTTP status is 200.

The monthly export, API v3 and file downloads share existing HTTP, named-file and
reader contracts. Pacing, additional discovery, retries and filtering are caller-owned.

Sources: [BHL API v3](https://www.biodiversitylibrary.org/docs/api3.html),
[GetItemMetadata](https://www.biodiversitylibrary.org/docs/api3/GetItemMetadata.html).
