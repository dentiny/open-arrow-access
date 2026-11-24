-- Bound join inputs before DuckDB builds wide correlated intermediates.
-- A view or inline predicate alone can still allow a full source relation to
-- enter the correlated join. Materialize filtered rows, then rebind the existing
-- view names so join.sql keeps its reconciliation logic unchanged.
-- All objects are private to this query connection. Keep the explicit source
-- rowid, duplicates and conflicting parent candidates; temporary-table insertion
-- order must not replace source order after a parallel scan.

CREATE TEMP TABLE bhl_scope_page AS
SELECT * FROM bhl_page
WHERE __key IN (SELECT page_id FROM bhl_selection);
CREATE OR REPLACE TEMP VIEW bhl_page AS
SELECT * FROM bhl_scope_page;

CREATE TEMP TABLE bhl_scope_part AS
SELECT * FROM bhl_part
WHERE __key IN (SELECT entity_id FROM bhl_selection WHERE entity_type = 'part');
CREATE OR REPLACE TEMP VIEW bhl_part AS
SELECT * FROM bhl_scope_part;

CREATE TEMP TABLE bhl_scope_partpage AS
SELECT * FROM bhl_partpage
WHERE __key IN (SELECT entity_id FROM bhl_selection WHERE entity_type = 'part')
  AND bhl_id(PageID) IN (SELECT page_id FROM bhl_selection);
CREATE OR REPLACE TEMP VIEW bhl_partpage AS
SELECT * FROM bhl_scope_partpage;

-- Retain every possible parent, including disagreements and missing targets.
-- The existing join resolves the parent and reports conflicts afterward.
CREATE TEMP TABLE bhl_scope_parent_ids AS
SELECT entity_id AS id FROM bhl_selection WHERE entity_type = 'item'
UNION SELECT bhl_id(ItemID) FROM bhl_part
UNION SELECT bhl_id(ItemID) FROM bhl_partpage
UNION SELECT bhl_id(ItemID) FROM bhl_page;

CREATE TEMP TABLE bhl_scope_item AS
SELECT * FROM bhl_item
WHERE __key IN (SELECT id FROM bhl_scope_parent_ids);
CREATE OR REPLACE TEMP VIEW bhl_item AS
SELECT * FROM bhl_scope_item;

CREATE TEMP TABLE bhl_scope_title AS
SELECT * FROM bhl_title
WHERE __key IN (SELECT bhl_id(TitleID) FROM bhl_item);
CREATE OR REPLACE TEMP VIEW bhl_title AS
SELECT * FROM bhl_scope_title;

CREATE TEMP TABLE bhl_scope_creator AS
SELECT * FROM bhl_creator
WHERE __key IN (SELECT bhl_id(TitleID) FROM bhl_item);
CREATE OR REPLACE TEMP VIEW bhl_creator AS
SELECT * FROM bhl_scope_creator;

CREATE TEMP TABLE bhl_scope_subject AS
SELECT * FROM bhl_subject
WHERE __key IN (SELECT bhl_id(TitleID) FROM bhl_item);
CREATE OR REPLACE TEMP VIEW bhl_subject AS
SELECT * FROM bhl_scope_subject;

CREATE TEMP TABLE bhl_scope_titleidentifier AS
SELECT * FROM bhl_titleidentifier
WHERE __key IN (SELECT bhl_id(TitleID) FROM bhl_item);
CREATE OR REPLACE TEMP VIEW bhl_titleidentifier AS
SELECT * FROM bhl_scope_titleidentifier;

CREATE TEMP TABLE bhl_scope_partcreator AS
SELECT * FROM bhl_partcreator
WHERE __key IN (SELECT entity_id FROM bhl_selection WHERE entity_type = 'part');
CREATE OR REPLACE TEMP VIEW bhl_partcreator AS
SELECT * FROM bhl_scope_partcreator;

CREATE TEMP TABLE bhl_scope_partidentifier AS
SELECT * FROM bhl_partidentifier
WHERE __key IN (SELECT entity_id FROM bhl_selection WHERE entity_type = 'part');
CREATE OR REPLACE TEMP VIEW bhl_partidentifier AS
SELECT * FROM bhl_scope_partidentifier;

CREATE TEMP TABLE bhl_scope_creatoridentifier AS
SELECT * FROM bhl_creatoridentifier
WHERE __key IN (
    SELECT bhl_id(CreatorID) FROM bhl_creator
    UNION SELECT bhl_id(CreatorID) FROM bhl_partcreator
);
CREATE OR REPLACE TEMP VIEW bhl_creatoridentifier AS
SELECT * FROM bhl_scope_creatoridentifier;

CREATE TEMP TABLE bhl_scope_pagename AS
SELECT * FROM bhl_pagename
WHERE __key IN (SELECT page_id FROM bhl_selection);
CREATE OR REPLACE TEMP VIEW bhl_pagename AS
SELECT * FROM bhl_scope_pagename;

CREATE TEMP TABLE bhl_scope_doi AS
SELECT * FROM bhl_doi
WHERE (lower(EntityType) = 'title' AND __key IN (SELECT bhl_id(TitleID) FROM bhl_item))
   OR (lower(EntityType) = 'item' AND __key IN (SELECT id FROM bhl_scope_parent_ids))
   OR (lower(EntityType) = 'page' AND __key IN (SELECT page_id FROM bhl_selection))
   OR (lower(EntityType) = 'part' AND __key IN (SELECT entity_id FROM bhl_selection WHERE entity_type = 'part'));
CREATE OR REPLACE TEMP VIEW bhl_doi AS
SELECT * FROM bhl_scope_doi;

