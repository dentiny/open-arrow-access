-- Run this complete CREATE + IMPORT script against a NEW database with DuckDB -bail.
-- Tested with DuckDB 2.0.0 and the published compression_fs extension.
-- cache_httpfs supplies cached HTTP access; compression_fs supplies .bz2 decoding.
FORCE INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;
FORCE INSTALL compression_fs FROM community;
LOAD compression_fs;

-- Review the source headers against the explicit table definitions below before changing the date.
SET VARIABLE courtlistener_export_date = '2026-09-30';
SET VARIABLE courtlistener_bulk_root =
    'https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/bulk-data';
SET preserve_insertion_order = true;

CREATE TEMP MACRO courtlistener_csv(table_name) AS TABLE
SELECT *
FROM read_csv(
    getvariable('courtlistener_bulk_root') || '/' || table_name || '-' ||
        getvariable('courtlistener_export_date') || '.csv.bz2',
    header = true,
    delim = ',',
    quote = '"',
    escape = '\',
    all_varchar = true,
    nullstr = '',
    allow_quoted_nulls = false,
    parallel = false,
    hive_partitioning = false,
    max_line_size = 67108864,
    buffer_size = 67108864
);


-- Default source schema pinned to the 2026-09-30 CSV headers and companion SQL.
-- Tables are populated by the read_csv INSERT statements later in this script.
-- Existing tables are not dropped or replaced; other export schemas require review.

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
    "xml_scan" VARCHAR NOT NULL,
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
    "filepath_json_harvard" VARCHAR NOT NULL,
    "filepath_pdf_harvard" VARCHAR NOT NULL,
    "docket_id" BIGINT NOT NULL,
    "arguments" VARCHAR NOT NULL,
    "headmatter" VARCHAR NOT NULL,
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
    "filepath_local" VARCHAR NOT NULL,
    "filepath_ia" VARCHAR NOT NULL,
    "filepath_ia_json" VARCHAR NOT NULL,
    "ia_upload_failure_count" UBIGINT,
    "ia_needs_upload" BOOLEAN,
    "ia_date_first_change" TIMESTAMPTZ,
    "view_count" UBIGINT NOT NULL,
    "date_blocked" DATE,
    "blocked" BOOLEAN NOT NULL,
    "appeal_from_id" VARCHAR,
    "assigned_to_id" BIGINT,
    "court_id" VARCHAR NOT NULL,
    "idb_data_id" BIGINT,
    "originating_court_information_id" BIGINT,
    "referred_to_id" BIGINT,
    "federal_dn_case_type" VARCHAR NOT NULL,
    "federal_dn_office_code" VARCHAR NOT NULL,
    "federal_dn_judge_initials_assigned" VARCHAR NOT NULL,
    "federal_dn_judge_initials_referred" VARCHAR NOT NULL,
    "federal_defendant_number" SMALLINT,
    "parent_docket_id" BIGINT,
    "docket_number_raw" VARCHAR NOT NULL,
    "docket_number_source" SMALLINT NOT NULL,
    "__courtlistener_ordinal" UBIGINT NOT NULL
);

CREATE TABLE courts (
    "id" VARCHAR NOT NULL,
    "pacer_court_id" SMALLINT,
    "pacer_has_rss_feed" BOOLEAN,
    "pacer_rss_entry_types" VARCHAR NOT NULL,
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
    "notes" VARCHAR NOT NULL,
    "parent_court_id" VARCHAR,
    "__courtlistener_ordinal" UBIGINT NOT NULL
);

-- INSERT casts into the explicit types/NOT NULL constraints above.
-- Read each file sequentially so ordinals retain original CSV record order.
BEGIN TRANSACTION;
INSERT INTO opinions BY NAME
SELECT *, (row_number() OVER () - 1)::UBIGINT AS __courtlistener_ordinal
FROM courtlistener_csv('opinions');
SELECT CASE WHEN count(*) = count(DISTINCT id) THEN true
    ELSE error('duplicate IDs in opinions') END FROM opinions;
COMMIT;

BEGIN TRANSACTION;
INSERT INTO opinion_clusters BY NAME
SELECT *, (row_number() OVER () - 1)::UBIGINT AS __courtlistener_ordinal
FROM courtlistener_csv('opinion-clusters');
SELECT CASE WHEN count(*) = count(DISTINCT id) THEN true
    ELSE error('duplicate IDs in opinion_clusters') END FROM opinion_clusters;
COMMIT;

BEGIN TRANSACTION;
INSERT INTO dockets BY NAME
SELECT *, (row_number() OVER () - 1)::UBIGINT AS __courtlistener_ordinal
FROM courtlistener_csv('dockets');
SELECT CASE WHEN count(*) = count(DISTINCT id) THEN true
    ELSE error('duplicate IDs in dockets') END FROM dockets;
COMMIT;

BEGIN TRANSACTION;
INSERT INTO courts BY NAME
SELECT *, (row_number() OVER () - 1)::UBIGINT AS __courtlistener_ordinal
FROM courtlistener_csv('courts');
SELECT CASE WHEN count(*) = count(DISTINCT id) THEN true
    ELSE error('duplicate IDs in courts') END FROM courts;
COMMIT;

CHECKPOINT;
