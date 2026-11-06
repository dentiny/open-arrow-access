# Dataset source providers

Enable `service-commoncrawl` on the facade for `CommonCrawl`, which validates a
crawl identity and HTTP(S) origin, and `create_http_accessor`, which configures
an origin-rooted OpenDAL HTTP reader without fetching data. Gzip and record
format decoding remain explicit caller-supplied layers.

Crawl-wide WARC datasets and segment selection are separate additions.
