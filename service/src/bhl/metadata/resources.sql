-- This budget belongs to the shared DuckDB instance, including query clones.
-- It excludes Arrow, the writer and extension allocations, so leave headroom.
SET memory_limit = '1GiB';
SET threads = 2;
SET max_temp_directory_size = '1GiB';
-- Relationship lists retain explicit source rowids and the result orders by
-- request_id; temporary-table insertion order is not part of the contract.
SET preserve_insertion_order = false;
