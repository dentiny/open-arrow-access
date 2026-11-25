CREATE VIEW main."{table}" AS
SELECT *
FROM read_parquet(
    [{paths}],
    union_by_name = {union_by_name},
    hive_partitioning = {hive_partitioning}
);
