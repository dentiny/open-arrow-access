# DuckDB data processing

- Use `VARIANT` for dynamic or heterogeneous data processed in DuckDB. Do not
  introduce DuckDB `JSON` columns, casts, or JSON-valued query intermediates.
- Keep known schemas in native scalar, `STRUCT`, and `LIST` types, including the
  fixed BHL metadata schema. Do not serialize typed records into JSON for joins
  or aggregation.
- JSON source formats and Rust JSON serialization for fingerprints or Arrow
  schema provenance are separate from DuckDB's SQL `JSON` type.
