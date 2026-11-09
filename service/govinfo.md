# GovInfo

Enable `service-govinfo` on the facade. `GovInfoService::new(None)` reads public
bulk directories; `new(Some(key))` also configures the GovInfo API. Keys are never
loaded from the environment implicitly.

`bulk_directory("FR/2026/01")` returns a lazy named-file accessor. Its parsed JSON
metadata includes folder entries for explicit traversal. ZIP bundles and their XML
members are exposed separately as upstream publishes them; choose the desired
representation to avoid counting the same documents twice. No file body is fetched
by resolution. Compose XML/ZIP readers on selected files.

`packages(collection,start,end,offset_mark,page_size)` fetches one last-modified
page, up to 1000 packages. Start with `"*"`, then use `next_page` with the returned
`nextPage`. `package(package_id)` resolves the package summary's content and
metadata links, including XML, PDF, MODS, and text. Logical names are the upstream
download-map keys. All upstream fields remain available in the parsed metadata. Readers use the shared sequential GET transport.

Callers own pacing and retry policy. HTTP 503 (including on-demand ZIP generation)
and 429 are surfaced as Unavailable and RateLimited errors. Listing pages are observations, not immutable
snapshots; record the requested interval and metadata with downstream output.

Sources: [bulk API](https://github.com/usgpo/bulk-data),
[GovInfo API](https://github.com/usgpo/api).
