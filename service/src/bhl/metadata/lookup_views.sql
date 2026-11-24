-- The lookup profile keeps the raw columns plus two helper columns. Exclude
-- only those helpers from the record, retaining the original field order.
-- query_table and unpacked COLUMNS are expanded when each view is bound.
CREATE TEMP MACRO bhl_lookup_rows(table_name) AS TABLE
SELECT * EXCLUDE (__source_rowid, __key),
       __source_rowid AS rowid, __key,
       struct_pack(*COLUMNS(* EXCLUDE (__source_rowid, __key))) AS __record
FROM query_table(table_name);

CREATE TEMP VIEW bhl_title AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_title');
CREATE TEMP VIEW bhl_item AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_item');
CREATE TEMP VIEW bhl_page AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_page');
CREATE TEMP VIEW bhl_part AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_part');
CREATE TEMP VIEW bhl_partpage AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_partpage');
CREATE TEMP VIEW bhl_creator AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_creator');
CREATE TEMP VIEW bhl_creatoridentifier AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_creatoridentifier');
CREATE TEMP VIEW bhl_subject AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_subject');
CREATE TEMP VIEW bhl_titleidentifier AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_titleidentifier');
CREATE TEMP VIEW bhl_partcreator AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_partcreator');
CREATE TEMP VIEW bhl_partidentifier AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_partidentifier');
CREATE TEMP VIEW bhl_pagename AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_pagename');
CREATE TEMP VIEW bhl_doi AS SELECT * FROM bhl_lookup_rows('main.__bhl_lookup_doi');
