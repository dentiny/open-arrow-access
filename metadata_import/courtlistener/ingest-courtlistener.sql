-- One normalized schema (version 1) for all reviewed CourtListener snapshots.
-- Run this complete CREATE + IMPORT script against a NEW database with -bail.
FORCE INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;
FORCE INSTALL compression_fs FROM community;
LOAD compression_fs;

SET VARIABLE courtlistener_export_date = '2026-09-30';
SET VARIABLE courtlistener_bulk_root =
    'https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/bulk-data';
SET preserve_insertion_order = true;

-- These dates have reviewed CSV headers/dialects and nonempty four-table exports.
-- Review a new date before adding it; 2024-03-01 is an empty upstream export.
SELECT CASE WHEN getvariable('courtlistener_export_date') IN (
    '2022-08-02', '2022-08-31', '2022-09-30', '2022-10-31',
    '2022-11-30', '2022-12-31', '2023-01-31', '2023-02-28',
    '2023-03-31', '2023-04-30', '2023-05-31', '2023-07-31',
    '2023-08-31', '2023-12-04', '2024-03-11', '2024-05-06',
    '2024-08-13', '2024-08-15', '2024-08-31', '2024-09-30',
    '2024-10-31', '2024-11-30', '2024-12-31', '2025-01-31',
    '2025-02-28', '2025-03-31', '2025-04-30', '2025-07-02',
    '2025-09-04', '2025-10-09', '2025-10-31', '2025-12-02',
    '2025-12-31', '2026-03-31', '2026-06-30', '2026-09-30'
) THEN true ELSE error('unreviewed or empty CourtListener export date') END;

-- Upstream CSV syntax changed; it does not change the normalized output schema.
SET VARIABLE courtlistener_csv_quote = CASE
    WHEN getvariable('courtlistener_export_date') BETWEEN '2024-08-13' AND '2024-12-31'
        THEN '`' ELSE '"' END;
SET VARIABLE courtlistener_csv_escape = CASE
    WHEN getvariable('courtlistener_export_date') >= '2025-01-31'
        THEN '\' ELSE getvariable('courtlistener_csv_quote') END;
SET VARIABLE courtlistener_csv_skip = CASE
    WHEN getvariable('courtlistener_export_date') = '2023-07-31' THEN 1 ELSE 0 END;

CREATE TEMP MACRO courtlistener_csv(table_name) AS TABLE
SELECT * FROM read_csv(
    getvariable('courtlistener_bulk_root') || '/' || table_name || '-' ||
        getvariable('courtlistener_export_date') || '.csv.bz2',
    header = true, delim = ',',
    quote = getvariable('courtlistener_csv_quote'),
    escape = getvariable('courtlistener_csv_escape'),
    skip = getvariable('courtlistener_csv_skip'),
    all_varchar = true, nullstr = '', allow_quoted_nulls = false,
    -- The forced sequential CSV path retained buffers and exhausted 12 GB.
    -- Keep the bounded parallel path; preserve_insertion_order/row_number retain ordinals.
    parallel = true, hive_partitioning = false,
    max_line_size = 67108864, buffer_size = 67108864
);

-- Fixed union of all 132 exported source fields. A field is nullable if any
-- reviewed snapshot lacks it or permits NULL. Missing fields are never filled
-- with fabricated empty strings, zeroes or current values.
CREATE TABLE opinions (
    "id" BIGINT NOT NULL,
    "date_created" TIMESTAMPTZ NOT NULL,
    "date_modified" TIMESTAMPTZ NOT NULL,
    "author_str" VARCHAR NOT NULL,
    "per_curiam" BOOLEAN NOT NULL,
    "joined_by_str" VARCHAR NOT NULL,
    "type" VARCHAR NOT NULL,
    "sha1" VARCHAR NOT NULL,
    "page_count" UBIGINT,
    "download_url" VARCHAR,
    "local_path" VARCHAR NOT NULL,
    "plain_text" VARCHAR NOT NULL,
    "html" VARCHAR NOT NULL,
    "html_lawbox" VARCHAR NOT NULL,
    "html_columbia" VARCHAR NOT NULL,
    "html_anon_2020" VARCHAR NOT NULL,
    "xml_harvard" VARCHAR NOT NULL,
    "xml_scan" VARCHAR,
    "html_with_citations" VARCHAR NOT NULL,
    "extracted_by_ocr" BOOLEAN NOT NULL,
    "author_id" BIGINT,
    "cluster_id" BIGINT NOT NULL,
    "__courtlistener_ordinal" UBIGINT NOT NULL
);

CREATE TABLE opinion_clusters (
    "id" BIGINT NOT NULL,
    "date_created" TIMESTAMPTZ NOT NULL,
    "date_modified" TIMESTAMPTZ NOT NULL,
    "judges" VARCHAR NOT NULL,
    "date_filed" DATE NOT NULL,
    "date_filed_is_approximate" BOOLEAN NOT NULL,
    "slug" VARCHAR,
    "case_name_short" VARCHAR NOT NULL,
    "case_name" VARCHAR NOT NULL,
    "case_name_full" VARCHAR NOT NULL,
    "scdb_id" VARCHAR NOT NULL,
    "scdb_decision_direction" INTEGER,
    "scdb_votes_majority" UBIGINT,
    "scdb_votes_minority" UBIGINT,
    "source" VARCHAR NOT NULL,
    "procedural_history" VARCHAR NOT NULL,
    "attorneys" VARCHAR NOT NULL,
    "nature_of_suit" VARCHAR NOT NULL,
    "posture" VARCHAR NOT NULL,
    "syllabus" VARCHAR NOT NULL,
    "headnotes" VARCHAR NOT NULL,
    "summary" VARCHAR NOT NULL,
    "disposition" VARCHAR NOT NULL,
    "history" VARCHAR NOT NULL,
    "other_dates" VARCHAR NOT NULL,
    "cross_reference" VARCHAR NOT NULL,
    "correction" VARCHAR NOT NULL,
    "citation_count" BIGINT NOT NULL,
    "precedential_status" VARCHAR NOT NULL,
    "date_blocked" DATE,
    "blocked" BOOLEAN NOT NULL,
    "filepath_json_harvard" VARCHAR,
    "filepath_pdf_harvard" VARCHAR,
    "docket_id" BIGINT NOT NULL,
    "arguments" VARCHAR,
    "headmatter" VARCHAR,
    "__courtlistener_ordinal" UBIGINT NOT NULL
);

CREATE TABLE dockets (
    "id" BIGINT NOT NULL,
    "date_created" TIMESTAMPTZ NOT NULL,
    "date_modified" TIMESTAMPTZ NOT NULL,
    "source" SMALLINT NOT NULL,
    "appeal_from_str" VARCHAR NOT NULL,
    "assigned_to_str" VARCHAR NOT NULL,
    "referred_to_str" VARCHAR NOT NULL,
    "panel_str" VARCHAR NOT NULL,
    "date_last_index" TIMESTAMPTZ,
    "date_cert_granted" DATE,
    "date_cert_denied" DATE,
    "date_argued" DATE,
    "date_reargued" DATE,
    "date_reargument_denied" DATE,
    "date_filed" DATE,
    "date_terminated" DATE,
    "date_last_filing" DATE,
    "case_name_short" VARCHAR NOT NULL,
    "case_name" VARCHAR NOT NULL,
    "case_name_full" VARCHAR NOT NULL,
    "slug" VARCHAR NOT NULL,
    "docket_number" VARCHAR,
    "docket_number_core" VARCHAR NOT NULL,
    "pacer_case_id" VARCHAR,
    "cause" VARCHAR NOT NULL,
    "nature_of_suit" VARCHAR NOT NULL,
    "jury_demand" VARCHAR NOT NULL,
    "jurisdiction_type" VARCHAR NOT NULL,
    "appellate_fee_status" VARCHAR NOT NULL,
    "appellate_case_type_information" VARCHAR NOT NULL,
    "mdl_status" VARCHAR NOT NULL,
    "filepath_local" VARCHAR,
    "filepath_ia" VARCHAR NOT NULL,
    "filepath_ia_json" VARCHAR NOT NULL,
    "ia_upload_failure_count" UBIGINT,
    "ia_needs_upload" BOOLEAN,
    "ia_date_first_change" TIMESTAMPTZ,
    "view_count" UBIGINT,
    "date_blocked" DATE,
    "blocked" BOOLEAN NOT NULL,
    "appeal_from_id" VARCHAR,
    "assigned_to_id" BIGINT,
    "court_id" VARCHAR NOT NULL,
    "idb_data_id" BIGINT,
    "originating_court_information_id" BIGINT,
    "referred_to_id" BIGINT,
    "federal_dn_case_type" VARCHAR,
    "federal_dn_office_code" VARCHAR,
    "federal_dn_judge_initials_assigned" VARCHAR,
    "federal_dn_judge_initials_referred" VARCHAR,
    "federal_defendant_number" SMALLINT,
    "parent_docket_id" BIGINT,
    "docket_number_raw" VARCHAR,
    "docket_number_source" SMALLINT,
    "__courtlistener_ordinal" UBIGINT NOT NULL
);

CREATE TABLE courts (
    "id" VARCHAR NOT NULL,
    "pacer_court_id" SMALLINT,
    "pacer_has_rss_feed" BOOLEAN,
    "pacer_rss_entry_types" VARCHAR,
    "date_last_pacer_contact" TIMESTAMPTZ,
    "fjc_court_id" VARCHAR NOT NULL,
    "date_modified" TIMESTAMPTZ NOT NULL,
    "in_use" BOOLEAN NOT NULL,
    "has_opinion_scraper" BOOLEAN NOT NULL,
    "has_oral_argument_scraper" BOOLEAN NOT NULL,
    "position" DOUBLE NOT NULL,
    "citation_string" VARCHAR NOT NULL,
    "short_name" VARCHAR NOT NULL,
    "full_name" VARCHAR NOT NULL,
    "url" VARCHAR NOT NULL,
    "start_date" DATE,
    "end_date" DATE,
    "jurisdiction" VARCHAR NOT NULL,
    "notes" VARCHAR,
    "parent_court_id" VARCHAR,
    "__courtlistener_ordinal" UBIGINT NOT NULL
);


-- Expected source headers vary by date; the physical table schema above does not.
CREATE TEMP MACRO courtlistener_expected_columns(target) AS TABLE
SELECT list(name ORDER BY cid) AS expected_columns,
    (SELECT list(name ORDER BY cid) FROM pragma_table_info(target)
        WHERE name <> '__courtlistener_ordinal') AS canonical_columns
FROM pragma_table_info(target)
WHERE name <> '__courtlistener_ordinal' AND NOT (
    (target = 'opinions' AND getvariable('courtlistener_export_date') < '2026-03-31'
        AND name IN ('xml_scan'))
    OR (target = 'opinion_clusters' AND getvariable('courtlistener_export_date') < '2023-07-31'
        AND name IN ('filepath_json_harvard', 'arguments', 'headmatter'))
    OR (target = 'opinion_clusters' AND getvariable('courtlistener_export_date') < '2024-08-31'
        AND name = 'filepath_pdf_harvard')
    OR (target = 'dockets' AND getvariable('courtlistener_export_date') < '2023-07-31'
        AND name IN ('date_last_index', 'filepath_local', 'ia_upload_failure_count',
            'ia_needs_upload', 'ia_date_first_change', 'view_count'))
    OR (target = 'dockets' AND getvariable('courtlistener_export_date') < '2024-08-13'
        AND name IN ('federal_dn_case_type', 'federal_dn_office_code',
            'federal_dn_judge_initials_assigned', 'federal_dn_judge_initials_referred',
            'federal_defendant_number', 'parent_docket_id'))
    OR (target = 'dockets' AND getvariable('courtlistener_export_date') < '2025-12-02'
        AND name = 'docket_number_raw')
    OR (target = 'dockets' AND getvariable('courtlistener_export_date') < '2026-03-31'
        AND name = 'docket_number_source')
    OR (target = 'courts' AND getvariable('courtlistener_export_date') < '2023-07-31'
        AND name IN ('pacer_rss_entry_types', 'notes'))
    OR (target = 'courts' AND getvariable('courtlistener_export_date') < '2023-12-04'
        AND name IN ('date_last_pacer_contact', 'parent_court_id'))
);

CREATE TEMP TABLE __courtlistener_expected_headers AS
SELECT 'opinions' AS table_name, expected_columns, canonical_columns FROM courtlistener_expected_columns('opinions')
UNION ALL
SELECT 'opinion_clusters' AS table_name, expected_columns, canonical_columns FROM courtlistener_expected_columns('opinion_clusters')
UNION ALL
SELECT 'dockets' AS table_name, expected_columns, canonical_columns FROM courtlistener_expected_columns('dockets')
UNION ALL
SELECT 'courts' AS table_name, expected_columns, canonical_columns FROM courtlistener_expected_columns('courts');

CREATE TEMP TABLE __courtlistener_actual_headers AS
SELECT 'opinions' AS table_name, list(column_name ORDER BY source_index) AS exported_columns
FROM (SELECT column_name, row_number() OVER () AS source_index
    FROM (DESCRIBE SELECT * FROM courtlistener_csv('opinions')))
UNION ALL
SELECT 'opinion_clusters' AS table_name, list(column_name ORDER BY source_index) AS exported_columns
FROM (SELECT column_name, row_number() OVER () AS source_index
    FROM (DESCRIBE SELECT * FROM courtlistener_csv('opinion-clusters')))
UNION ALL
SELECT 'dockets' AS table_name, list(column_name ORDER BY source_index) AS exported_columns
FROM (SELECT column_name, row_number() OVER () AS source_index
    FROM (DESCRIBE SELECT * FROM courtlistener_csv('dockets')))
UNION ALL
SELECT 'courts' AS table_name, list(column_name ORDER BY source_index) AS exported_columns
FROM (SELECT column_name, row_number() OVER () AS source_index
    FROM (DESCRIBE SELECT * FROM courtlistener_csv('courts')));

SELECT CASE WHEN count(*) = 4 AND bool_and(exported_columns = expected_columns)
    THEN true ELSE error('source CSV headers do not match the reviewed snapshot') END
FROM __courtlistener_actual_headers JOIN __courtlistener_expected_headers USING (table_name);

-- Distinguish a missing historical field from an exported field whose value is NULL.
-- This is not the separate prepared-reader courtlistener_snapshot header.
CREATE TABLE courtlistener_import_manifest (
    schema_version UINTEGER NOT NULL,
    snapshot_date DATE NOT NULL,
    table_name VARCHAR PRIMARY KEY,
    source_uri VARCHAR NOT NULL,
    exported_columns VARCHAR[] NOT NULL,
    missing_columns VARCHAR[] NOT NULL,
    csv_quote VARCHAR NOT NULL,
    csv_escape VARCHAR NOT NULL,
    csv_skip_rows UBIGINT NOT NULL,
    completed BOOLEAN NOT NULL DEFAULT false,
    row_count UBIGINT
);
INSERT INTO courtlistener_import_manifest BY NAME
SELECT 1::UINTEGER AS schema_version,
    getvariable('courtlistener_export_date')::DATE AS snapshot_date, table_name,
    getvariable('courtlistener_bulk_root') || '/' || replace(table_name, '_', '-') || '-' ||
        getvariable('courtlistener_export_date') || '.csv.bz2' AS source_uri,
    exported_columns,
    list_filter(canonical_columns, lambda name: NOT list_contains(exported_columns, name)) AS missing_columns,
    getvariable('courtlistener_csv_quote') AS csv_quote,
    getvariable('courtlistener_csv_escape') AS csv_escape,
    getvariable('courtlistener_csv_skip')::UBIGINT AS csv_skip_rows
FROM __courtlistener_actual_headers JOIN __courtlistener_expected_headers USING (table_name);

-- BY NAME fills missing nullable fields with NULL. Each source record keeps its ordinal.
BEGIN TRANSACTION;
INSERT INTO opinions BY NAME
SELECT *, (row_number() OVER () - 1)::UBIGINT AS __courtlistener_ordinal
FROM courtlistener_csv('opinions');
SELECT CASE WHEN count(*) = count(DISTINCT id) THEN true
    ELSE error('duplicate IDs in opinions') END FROM opinions;
UPDATE courtlistener_import_manifest SET completed = true, row_count = (SELECT count(*) FROM opinions)
WHERE table_name = 'opinions';
COMMIT;
CHECKPOINT;

BEGIN TRANSACTION;
INSERT INTO opinion_clusters BY NAME
SELECT *, (row_number() OVER () - 1)::UBIGINT AS __courtlistener_ordinal
FROM courtlistener_csv('opinion-clusters');
SELECT CASE WHEN count(*) = count(DISTINCT id) THEN true
    ELSE error('duplicate IDs in opinion_clusters') END FROM opinion_clusters;
UPDATE courtlistener_import_manifest SET completed = true, row_count = (SELECT count(*) FROM opinion_clusters)
WHERE table_name = 'opinion_clusters';
COMMIT;
CHECKPOINT;

BEGIN TRANSACTION;
INSERT INTO dockets BY NAME
SELECT *, (row_number() OVER () - 1)::UBIGINT AS __courtlistener_ordinal
FROM courtlistener_csv('dockets');
SELECT CASE WHEN count(*) = count(DISTINCT id) THEN true
    ELSE error('duplicate IDs in dockets') END FROM dockets;
UPDATE courtlistener_import_manifest SET completed = true, row_count = (SELECT count(*) FROM dockets)
WHERE table_name = 'dockets';
COMMIT;
CHECKPOINT;

BEGIN TRANSACTION;
INSERT INTO courts BY NAME
SELECT *, (row_number() OVER () - 1)::UBIGINT AS __courtlistener_ordinal
FROM courtlistener_csv('courts');
SELECT CASE WHEN count(*) = count(DISTINCT id) THEN true
    ELSE error('duplicate IDs in courts') END FROM courts;
UPDATE courtlistener_import_manifest SET completed = true, row_count = (SELECT count(*) FROM courts)
WHERE table_name = 'courts';
COMMIT;
CHECKPOINT;
