# USPTO Open Data Portal

Enable `service-uspto`, then construct `UsptoService::new(key)` with an explicit
ODP key. `search(parameters)` exposes the bulk-product discovery GET endpoint.
`product(product_id,offset,limit)` fetches one product file page with
`includeFiles=true` and exposes each `fileName`/`fileDownloadURI` as a lazy byte
object. It validates product identity and required file membership, preserving
all upstream metadata, release dates, product totals and unknown fields in parsed JSON metadata. The caller advances offsets and records the observed product snapshot;
this service does not claim an atomic snapshot of a changing catalogue.

Downloads keep original ZIP/XML bytes, support GET-only signed URLs, and send the
ODP key only to the configured API origin. Cross-origin redirects strip it.
Compose format/archive readers according to the selected product representation.
No credentials are loaded implicitly. Offline fixtures cover the API contract;
a real ODP key is needed to verify account-specific product access online.

Sources: [ODP bulk search](https://data.uspto.gov/apis/bulk-data/search),
[ODP API access](https://data.uspto.gov/apis/getting-started).
