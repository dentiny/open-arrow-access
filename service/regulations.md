# Regulations.gov

Enable `service-regulations`. Construct `RegulationsService::new(key)` with an
explicit api.data.gov key. The API surface is read-only.
`search(resource,parameters,page,size)` reads one documents/comments/dockets page,
retaining parsed JSON and all pagination metadata. Page numbers are 1..=20 and page
sizes 5..=250, matching the official v4 specification. API results can exceed a
single query window: inspect `meta.totalElements`/`hasNextPage` and use upstream
lastModifiedDate filters and stable sorting to define subsequent windows. This
reader does not silently claim a capped query is a complete dataset.

`record(resource,id)` requests document/comment attachments and exposes original
file-format URLs as lazy bytes. Logical names are `document/file-000` and
`attachment-000/file-000`, preserving upstream representation order. Full record
metadata, withdrawal flags, restrictions and included resources stay unchanged.
Records without downloadable files still retain their metadata. Downloads never
forward API credentials to another origin. Caller controls pacing, windowing,
retries, joins and downstream conversion.

Sources: [GSA API documentation](https://open.gsa.gov/api/regulationsgov/),
[official OpenAPI schema](https://open.gsa.gov/api/regulationsgov/v4/openapi.yaml).
