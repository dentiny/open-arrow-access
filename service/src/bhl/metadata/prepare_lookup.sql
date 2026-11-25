-- Optional immutable BHL lookup profile, version 1.
-- Run once after all 13 raw tables are finalized. Raw schemas/values are retained.
-- Additional derived tables and indexes require disk and build memory.
-- Each table commits before CREATE INDEX, enabling DuckDB bulk index builds.
-- A failed build can leave partial objects: discard the unpublished output.
-- Do not update raw tables afterward: rebuild the complete snapshot instead.
CREATE TEMP MACRO __bhl_lookup_id(value) AS CASE
    WHEN regexp_full_match(value, '[+]?[0-9]+')
    THEN nullif(try_cast(value AS UBIGINT), 0) END;

CREATE TABLE main.__bhl_lookup_title AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(TitleID) AS __key
FROM main.title ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_title_key ON main.__bhl_lookup_title (__key);

CREATE TABLE main.__bhl_lookup_item AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(ItemID) AS __key
FROM main.item ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_item_key ON main.__bhl_lookup_item (__key);

CREATE TABLE main.__bhl_lookup_page AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(PageID) AS __key
FROM main.page ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_page_key ON main.__bhl_lookup_page (__key);

CREATE TABLE main.__bhl_lookup_part AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(PartID) AS __key
FROM main.part ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_part_key ON main.__bhl_lookup_part (__key);

CREATE TABLE main.__bhl_lookup_partpage AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(PartID) AS __key
FROM main.partpage ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_partpage_key ON main.__bhl_lookup_partpage (__key);

CREATE TABLE main.__bhl_lookup_creator AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(TitleID) AS __key
FROM main.creator ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_creator_key ON main.__bhl_lookup_creator (__key);

CREATE TABLE main.__bhl_lookup_creatoridentifier AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(CreatorID) AS __key
FROM main.creatoridentifier ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_creatoridentifier_key ON main.__bhl_lookup_creatoridentifier (__key);

CREATE TABLE main.__bhl_lookup_subject AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(TitleID) AS __key
FROM main.subject ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_subject_key ON main.__bhl_lookup_subject (__key);

CREATE TABLE main.__bhl_lookup_titleidentifier AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(TitleID) AS __key
FROM main.titleidentifier ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_titleidentifier_key ON main.__bhl_lookup_titleidentifier (__key);

CREATE TABLE main.__bhl_lookup_partcreator AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(PartID) AS __key
FROM main.partcreator ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_partcreator_key ON main.__bhl_lookup_partcreator (__key);

CREATE TABLE main.__bhl_lookup_partidentifier AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(PartID) AS __key
FROM main.partidentifier ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_partidentifier_key ON main.__bhl_lookup_partidentifier (__key);

CREATE TABLE main.__bhl_lookup_pagename AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(PageID) AS __key
FROM main.pagename ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_pagename_key ON main.__bhl_lookup_pagename (__key);

CREATE TABLE main.__bhl_lookup_doi AS
SELECT *, rowid::UBIGINT AS __source_rowid, __bhl_lookup_id(EntityID) AS __key
FROM main.doi ORDER BY __key, __source_rowid;
CREATE INDEX __bhl_lookup_doi_key ON main.__bhl_lookup_doi (__key);

BEGIN TRANSACTION;
CREATE TABLE main.__bhl_lookup (version UBIGINT);
INSERT INTO main.__bhl_lookup VALUES (1);
COMMIT;
CHECKPOINT;
