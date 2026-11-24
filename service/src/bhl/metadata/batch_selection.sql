CREATE OR REPLACE TEMP VIEW bhl_selection AS
SELECT selected.* FROM bhl_all_selection selected
WHERE EXISTS (
    SELECT 1 FROM bhl_all_selection batch
    WHERE batch.request_id >= {start} AND batch.request_id < {end}
      AND batch.entity_type = selected.entity_type
      AND batch.entity_id = selected.entity_id
      AND batch.page_id = selected.page_id
);
