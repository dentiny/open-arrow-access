-- Build only query-local views. Selecting the source row as a struct keeps
-- its lexical fields in schema order without generating a struct_pack in Rust.
CREATE TEMP VIEW bhl_title AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.TitleID) AS __key, src AS __record
FROM main.title src;

CREATE TEMP VIEW bhl_item AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.ItemID) AS __key, src AS __record
FROM main.item src;

CREATE TEMP VIEW bhl_page AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.PageID) AS __key, src AS __record
FROM main.page src;

CREATE TEMP VIEW bhl_part AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.PartID) AS __key, src AS __record
FROM main.part src;

CREATE TEMP VIEW bhl_partpage AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.PartID) AS __key, src AS __record
FROM main.partpage src;

CREATE TEMP VIEW bhl_creator AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.TitleID) AS __key, src AS __record
FROM main.creator src;

CREATE TEMP VIEW bhl_creatoridentifier AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.CreatorID) AS __key, src AS __record
FROM main.creatoridentifier src;

CREATE TEMP VIEW bhl_subject AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.TitleID) AS __key, src AS __record
FROM main.subject src;

CREATE TEMP VIEW bhl_titleidentifier AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.TitleID) AS __key, src AS __record
FROM main.titleidentifier src;

CREATE TEMP VIEW bhl_partcreator AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.PartID) AS __key, src AS __record
FROM main.partcreator src;

CREATE TEMP VIEW bhl_partidentifier AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.PartID) AS __key, src AS __record
FROM main.partidentifier src;

CREATE TEMP VIEW bhl_pagename AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.PageID) AS __key, src AS __record
FROM main.pagename src;

CREATE TEMP VIEW bhl_doi AS
SELECT src.*, src.rowid AS rowid, bhl_id(src.EntityID) AS __key, src AS __record
FROM main.doi src;
