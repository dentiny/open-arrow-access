# Figshare

Enable `service-figshare`. `FigshareService::new()?.article(id,Some(version))`
resolves a specific published article version into lazy sequential file access.
Pass `None` only when latest-version semantics are intended. Parsed metadata keeps
all original fields, license details, checksums, sizes and download URLs.
Files keep upstream names and original bytes. Resolution and cached listing
perform no file downloads. Embargoed articles fail rather than appearing empty.
`collection_articles(id,page,page_size)` returns one discovery metadata page; callers
own pagination, pacing and retry policy. No API key is needed for public articles.

For BHL's monthly OCR export, article ID is `21422193`; select and record a version.
The BHL API service composes with this shared provider instead of duplicating its
Figshare parsing or downloads. Gzip/text decoding remains separately composable.

Sources: [Figshare API](https://docs.figshare.com/v2/),
[BHL export announcement](https://blog.biodiversitylibrary.org/2023/01/bhl-technical-development-year-in-review).
