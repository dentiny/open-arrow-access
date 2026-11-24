-- Run with DuckDB v1.5.6 and httpfs enabled, against a NEW database file.
LOAD httpfs;
SET threads = 4;
SET preserve_insertion_order = false;

-- Gzip is decompressed automatically. Keep every field as VARCHAR;
-- disable quote parsing and preserve empty strings as empty strings.
CREATE TEMP MACRO bhl_tsv(name, required_columns) AS TABLE
SELECT * FROM read_csv(
    'https://bhl-open-data.s3.us-east-2.amazonaws.com/data/' || name || '.txt.gz',
    delim = '\t',
    header = true,
    all_varchar = true,
    quote = '',
    escape = '',
    force_not_null = required_columns
);

CREATE TABLE title AS SELECT * FROM bhl_tsv('title', ['TitleID', 'MARCBibID', 'MARCLeader', 'FullTitle', 'ShortTitle', 'PublicationDetails', 'CallNumber', 'StartYear', 'EndYear', 'LanguageCode', 'TL2Author', 'TitleURL', 'CreationDate']);
CREATE TABLE item AS SELECT * FROM bhl_tsv('item', ['ItemID', 'TitleID', 'ThumbnailPageID', 'BarCode', 'MARCItemID', 'CallNumber', 'VolumeInfo', 'ItemURL', 'ItemTextURL', 'ItemPDFURL', 'ItemImagesURL', 'LocalID', 'Year', 'InstitutionName', 'ZQuery', 'CreationDate', 'CopyrightStatus', 'RightsStatement', 'LicenseType', 'RightsHolder']);
CREATE TABLE page AS SELECT * FROM bhl_tsv('page', ['PageID', 'ItemID', 'SequenceOrder', 'Year', 'Volume', 'Issue', 'PagePrefix', 'PageNumber', 'PageTypeName', 'CreationDate']);
CREATE TABLE part AS SELECT * FROM bhl_tsv('part', ['PartID', 'ItemID', 'ContributorName', 'SequenceOrder', 'SegmentType', 'Title', 'ContainerTitle', 'PublicationDetails', 'Volume', 'Series', 'Issue', 'Date', 'PageRange', 'StartPageID', 'LanguageName', 'SegmentUrl', 'ExternalUrl', 'DownloadUrl', 'RightsStatus', 'RightsStatement', 'LicenseName', 'LicenseUrl', 'RightsHolder', 'BarCode']);
CREATE TABLE partpage AS SELECT * FROM bhl_tsv('partpage', ['PartID', 'PageID', 'ItemID', 'SequenceOrder', 'CreationDate']);
CREATE TABLE creator AS SELECT * FROM bhl_tsv('creator', ['TitleID', 'CreatorID', 'CreatorType', 'CreatorName', 'CreationDate']);
CREATE TABLE creatoridentifier AS SELECT * FROM bhl_tsv('creatoridentifier', ['CreatorID', 'IdentifierName', 'IdentifierValue', 'CreationDate']);
CREATE TABLE subject AS SELECT * FROM bhl_tsv('subject', ['TitleID', 'Subject', 'CreationDate']);
CREATE TABLE titleidentifier AS SELECT * FROM bhl_tsv('titleidentifier', ['TitleID', 'IdentifierName', 'IdentifierValue', 'CreationDate']);
CREATE TABLE partcreator AS SELECT * FROM bhl_tsv('partcreator', ['PartID', 'CreatorID', 'CreatorName', 'CreationDate']);
CREATE TABLE partidentifier AS SELECT * FROM bhl_tsv('partidentifier', ['PartID', 'IdentifierName', 'IdentifierValue', 'CreationDate']);
CREATE TABLE pagename AS SELECT * FROM bhl_tsv('pagename', ['NameBankID', 'NameConfirmed', 'PageID', 'CreationDate']);
CREATE TABLE doi AS SELECT * FROM bhl_tsv('doi', ['EntityType', 'EntityID', 'DOI', 'CreationDate']);

CHECKPOINT;

-- Build optional numeric BHL lookup profile after the raw import.
-- Optional immutable BHL lookup profile, version 1.
-- Run once after all 13 raw tables are finalized. Raw schemas/values are retained.
-- Additional derived tables and indexes require disk and build memory.
-- Do not update raw tables afterward: rebuild the complete snapshot instead.
BEGIN TRANSACTION;
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

CREATE TABLE main.__bhl_lookup (version UBIGINT);
INSERT INTO main.__bhl_lookup VALUES (1);
COMMIT;
CHECKPOINT;
