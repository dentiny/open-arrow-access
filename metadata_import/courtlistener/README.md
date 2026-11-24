# CourtListener database creation SQL

These files are an executable SQL reference for the prepared local database
layout. They do not change the runtime importer or automatically load CSV data.

| File | Purpose |
| --- | --- |
| [create_tables.sql](create_tables.sql) | Full default DDL for opinions, opinion_clusters, dockets and courts |
| [create_snapshot.sql](create_snapshot.sql) | Completion/provenance table, created after successful imports |
| [insert_snapshot.sql](insert_snapshot.sql) | Insert the single completed snapshot with four bound parameters |

The default DDL is pinned to the 2026-09-30 export: 22 opinion, 36 cluster,
54 docket and 20 court source fields. Every table also has
`__courtlistener_ordinal: UBIGINT NOT NULL`, the zero-based logical CSV row
number excluding the header. These columns match the default Arrow source
schema; historical schema variants require their own reviewed DDL.

## How the database is created

1. Rust opens a new staging DuckDB file on the destination filesystem.
   Opening the file creates the database; there is no separate SQL
   `CREATE DATABASE` statement.
2. Execute `create_tables.sql` to create four empty typed tables.
3. For each table, execute `BEGIN TRANSACTION`, parse the source PostgreSQL
   COPY CSV into strictly typed Arrow batches, append the batches and original
   ordinals using DuckDB's Arrow appender, flush, then `COMMIT`.
   These data writes use the appender API, not `read_csv` or SQL `INSERT`
   statements. Quoted empty strings and unquoted NULLs stay distinct.
4. Reject duplicate IDs in each imported table. For example:

   ```sql
   SELECT id, count(*)
   FROM opinions
   GROUP BY id
   HAVING count(*) > 1;
   ```

   The importer checks all four tables. Cross-table foreign keys are not
   enforced: upstream exports can have missing matches, which remain nullable
   LEFT JOINs at read time.
5. Execute `create_snapshot.sql`, then `insert_snapshot.sql` with parameters
   in this order: application format version (`1`), export date, source URI
   root, and the exact serialized source-schema contract.
6. Execute `CHECKPOINT`, close DuckDB, then publish the file atomically without
   replacing an existing destination. File publication is a filesystem action,
   not a SQL statement.

TEXT, HTML and XML are stored in their original columns. PDF paths/URLs are
stored as references; PDF downloading remains with the sink. No joined wide
table is materialized. Read-time joins follow the same opinion → cluster →
docket → court relationship as the provider.

## Inspect just the schema with the DuckDB CLI

```sh
duckdb -bail /path/to/empty.duckdb < metadata_import/courtlistener/create_tables.sql
duckdb -bail /path/to/empty.duckdb 'DESCRIBE opinions;'
```

This creates empty tables only. It does not import data or create a completed
snapshot row. Populate the tables using the typed CSV readers and DuckDB Arrow
appender, then write the bound snapshot row and checkpoint the file as described
above. Pass the completed file path to the generic `DuckDbDatabase::open(path)`
for SQL queries. The provider-specific prepared-file reader is implemented
separately in [PR #727](https://github.com/dentiny/open-arrow-access/pull/727).
