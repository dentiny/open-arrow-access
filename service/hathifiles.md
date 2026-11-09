# HathiTrust Hathifiles

Enable `service-hathifiles`, then call `HathifilesService::new()?.catalog().await?`.
This resolves the official `files/hathifiles/hathi_file_list.json` into a lazy
named-file accessor. It preserves every full/update entry and original listing
order; metadata exposes parsed JSON, including sizes and timestamps. Open a
selected filename to read the original gzip bytes, then compose gzip/TSV readers.
No rights, language, or date filtering is performed. These files are the catalogue,
not book text; use the separate Internet Archive service for IA-hosted volumes.
The upstream listing is mutable, so record its parsed metadata and selected filename
with downstream snapshot metadata. Mirrors can use `with_connection`.

Source: [HathiTrust's listing generator](https://github.com/hathitrust/hathifiles/blob/main/jobs/update_hathifile_listing.rb).
