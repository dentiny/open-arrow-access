-- DuckDB v1.5.6. Run against a NEW database with -bail.
-- Download the input JSON (Cornell/Kaggle arXiv metadata snapshot):
-- 1. Open https://www.kaggle.com/datasets/Cornell-University/arxiv/versions/313
--    and click Download (sign in to Kaggle if prompted). Version 313 is the
--    snapshot used for the original import; keep this version for reproducibility.
-- 2. Extract arxiv-metadata-oai-snapshot.json from the downloaded ZIP:
--      unzip /path/to/arxiv.zip arxiv-metadata-oai-snapshot.json -d /path/to/data
--    Replace arxiv.zip with the downloaded archive's name. The extracted file
--    is JSON Lines (one article per line), not a JSON array.
-- 3. Replace /path/to/arxiv-metadata-oai-snapshot.json below with the extracted
--    file's absolute path, then run from the repository root:
--      duckdb -bail /path/to/arxiv.duckdb < metadata_import/arxiv/ingest-arxiv-metadata.sql
-- This script performs no OAI requests.
SET preserve_insertion_order = false;

-- Preserve every source field. Explicit string extraction protects IDs/leading zeros.
CREATE TABLE arxiv_metadata_raw (
    article_id VARCHAR PRIMARY KEY CHECK (length(article_id) > 0),
    source_id VARCHAR NOT NULL,
    source_metadata JSON NOT NULL
);

INSERT INTO arxiv_metadata_raw
SELECT
    replace(json_extract_string(json, '$.id'), '/', ''),
    json_extract_string(json, '$.id'),
    json
FROM read_json_objects(
    '/path/to/arxiv-metadata-oai-snapshot.json',
    format = 'newline_delimited'
);

-- Match the field names, order and logical types in
-- open-arrow-access-service 0.20.4/src/arxiv_metadata/format.rs.
-- NULL means the source does not supply this OAI value; it is not reconstructed.
CREATE TABLE arxiv_metadata AS
WITH decoded AS (
    SELECT *, from_json(source_metadata,
        '{"versions":[{"version":"VARCHAR","created":"VARCHAR"}]}') AS parsed
    FROM arxiv_metadata_raw
), canonical AS (
    SELECT *, CASE
        WHEN regexp_matches(source_id, '^[0-9]') OR contains(source_id, '/')
            THEN source_id
        ELSE left(source_id, length(source_id) - 7) || '/' || right(source_id, 7)
    END AS oai_article_id
    FROM decoded
)
SELECT
    article_id,
    struct_pack(
        "oai:identifier" := 'oai:arXiv.org:' || oai_article_id,
        "oai:datestamp" := NULL::VARCHAR,
        "@status" := NULL::VARCHAR,
        "oai:setSpec" := NULL::VARCHAR[]
    ) AS "oai:header",
    struct_pack(
        "arxiv:arXivRaw" := struct_pack(
            "arxiv:id" := source_id,
            "arxiv:submitter" := json_extract_string(source_metadata, '$.submitter'),
            "arxiv:title" := json_extract_string(source_metadata, '$.title'),
            "arxiv:authors" := json_extract_string(source_metadata, '$.authors'),
            "arxiv:categories" := json_extract_string(source_metadata, '$.categories'),
            "arxiv:comments" := json_extract_string(source_metadata, '$.comments'),
            "arxiv:proxy" := json_extract_string(source_metadata, '$.proxy'),
            "arxiv:report-no" := json_extract_string(source_metadata, '$."report-no"'),
            "arxiv:acm-class" := json_extract_string(source_metadata, '$."acm-class"'),
            "arxiv:msc-class" := json_extract_string(source_metadata, '$."msc-class"'),
            "arxiv:journal-ref" := json_extract_string(source_metadata, '$."journal-ref"'),
            "arxiv:doi" := json_extract_string(source_metadata, '$.doi'),
            "arxiv:license" := json_extract_string(source_metadata, '$.license'),
            "arxiv:abstract" := json_extract_string(source_metadata, '$.abstract'),
            "@xsi:schemaLocation" := NULL::VARCHAR,
            "arxiv:version" := list_transform(parsed.versions, lambda v:
                struct_pack(
                    "@version" := v.version,
                    "arxiv:date" := v.created,
                    "arxiv:size" := NULL::VARCHAR,
                    "arxiv:source_type" := NULL::VARCHAR
                )
            )
        )
    ) AS "oai:metadata"
FROM canonical;

CREATE UNIQUE INDEX arxiv_metadata_article_id_idx
    ON arxiv_metadata(article_id);

-- Exactly the two top-level logical columns expected for OAI records.
-- A Rust reader must bind/cast the resulting Arrow schema to the library's
-- explicit schema, including field nullability and list element names.
CREATE VIEW arxiv_oai_records AS
SELECT "oai:header", "oai:metadata"
FROM arxiv_metadata;

CHECKPOINT;

SELECT count(*) AS metadata_records FROM arxiv_metadata;

-- Query with modern ID or legacy bulk spelling (legacy slash is normalized).
-- SELECT "oai:header", "oai:metadata"
-- FROM arxiv_metadata
-- WHERE article_id = replace('2603.11251', '/', '');

-- Completeness: source update_date stays in arxiv_metadata_raw.source_metadata;
-- it is not asserted to be the original OAI header datestamp.
-- Kaggle does not establish complete deleted-record coverage. No deletion
-- tombstones, set membership, version size/source_type or other absent values
-- are fabricated. This is a partial OAI-shaped snapshot, not a lossless OAI dump.
