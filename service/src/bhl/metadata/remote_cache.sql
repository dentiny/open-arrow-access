-- A bounded 128 MiB cache, instead of a private multi-GB disk copy per worker.
-- Set these before the first in-memory cache access.
SET cache_httpfs_cache_block_size = 524288;
SET cache_httpfs_max_in_mem_cache_block_count = 256;
SET cache_httpfs_max_fanout_subrequest = 4;
SET cache_httpfs_type = 'in_mem';
