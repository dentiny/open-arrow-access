# Dataset source providers

Enable `service-commoncrawl` on the facade for `CommonCrawl`, which validates a
crawl identity and HTTP(S) origin, and `create_http_accessor`, which configures
an origin-rooted OpenDAL HTTP reader without fetching data. Gzip and record
format decoding remain explicit caller-supplied layers.

Enable `service-commoncrawl-warc` for `CommonCrawlWarcRequest` and
`CommonCrawlWarcAccessor`: resolve all WARC paths from a crawl's decoded
`warc.paths.gz` inventory, preserving file order, then read through a supplied
single-file format accessor and the existing sequential FileSet. `resolve`
returns membership without opening record files. The caller supplies gzip for
compressed inventories and file decompression/format parsing separately.
Selected locators are held in memory; inventory lines have a 16 KiB limit.
There is no index, batch planner or parallel execution.

Enable `service-commoncrawl-segment` for `CommonCrawlSegmentRequest` and
`CommonCrawlSegmentAccessor`. Segment resolution reuses the WARC inventory
parser and retains only exact `segments/<segment>/warc/` matches. It preserves
manifest order and forwards existing read options to the same sequential
FileSet. A neighboring segment with a longer numeric ID is a different selection.
Construct a request from crawl/segment IDs, a segment URL, or a shared
`CommonCrawl` source. No index, batch scheduler or parallel executor is added.
