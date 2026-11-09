# bioRxiv and medRxiv

Enable `service-biorxiv`. `BiorxivService::new()` accesses the public metadata API without a key. `details(Server::Biorxiv, "2026-11-01", "2026-11-02", 0)` returns parsed JSON and `next_cursor`; request each next page explicitly. `Server::Medrxiv` uses the same contract. Counters accept the upstream string or integer representation; pagination uses actual returned record count. Empty `no posts found` windows are valid. Every version, license and unknown field remains available; no deduplication or license filtering is applied.

`jats(page)` resolves advertised JATS URLs into lazy sequential byte readers named `paper-00000.xml` by original row index. Missing XML links retain their full metadata in the bound page. Content fetches occur only on open/read.

For bulk MECA bundles, `Server::bulk_bucket()` identifies `biorxiv-src-monthly` or `medrxiv-src-monthly` in us-east-1. Use existing storage listing and a ranged S3 reader configured by the caller with requester-pays, then `MecaArchive::open(reader, limits)` (reexported here). It retains the manifest and opens referenced XML/PDF/supplement members on demand through the shared ZIP implementation. Bucket/month discovery and credentials stay in the existing storage layer; no paid requests are made automatically. API metadata and bulk bundles have different update schedules; callers record the selected interval/prefix and source version.

Sources: [official metadata API](https://api.biorxiv.org/), [bioRxiv TDM](https://www.biorxiv.org/tdm) and [medRxiv TDM](https://www.medrxiv.org/tdm).
