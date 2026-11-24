//! Bound the correlated metadata joins to the current page selection.
use super::error;
use open_arrow_access_core::Result;

fn materialize(connection: &::duckdb::Connection, table: &str, predicate: &str) -> Result<()> {
    // Identifiers and predicates are fixed below, never supplied by a caller.
    // Keep the explicit source rowid: list ordering must not use the new table's
    // insertion order, which can differ after a parallel scan.
    connection
        .execute_batch(&format!(
            "CREATE TEMP TABLE bhl_scope_{table} AS
                 SELECT * FROM bhl_{table} WHERE {predicate};
             CREATE OR REPLACE TEMP VIEW bhl_{table} AS
                 SELECT * FROM bhl_scope_{table};"
        ))
        .map_err(error::duckdb)
}

pub(super) fn prepare(connection: &::duckdb::Connection) -> Result<()> {
    materialize(
        connection,
        "page",
        "__key IN (SELECT page_id FROM bhl_selection)",
    )?;
    let parts = "SELECT entity_id FROM bhl_selection WHERE entity_type = 'part'";
    materialize(connection, "part", &format!("__key IN ({parts})"))?;
    materialize(
        connection,
        "partpage",
        &format!("__key IN ({parts}) AND bhl_id(PageID) IN (SELECT page_id FROM bhl_selection)"),
    )?;
    // Retain every possible parent, including disagreements and missing item
    // targets. The existing join remains responsible for resolving the parent
    // and reporting conflicts; scoping must not change reconciliation.
    connection
        .execute_batch(
            "CREATE TEMP TABLE bhl_scope_parent_ids AS
         SELECT entity_id AS id FROM bhl_selection WHERE entity_type = 'item'
         UNION SELECT bhl_id(ItemID) FROM bhl_part
         UNION SELECT bhl_id(ItemID) FROM bhl_partpage
         UNION SELECT bhl_id(ItemID) FROM bhl_page;",
        )
        .map_err(error::duckdb)?;
    materialize(
        connection,
        "item",
        "__key IN (SELECT id FROM bhl_scope_parent_ids)",
    )?;
    let titles = "SELECT bhl_id(TitleID) FROM bhl_item";
    for table in ["title", "creator", "subject", "titleidentifier"] {
        materialize(connection, table, &format!("__key IN ({titles})"))?;
    }
    for table in ["partcreator", "partidentifier"] {
        materialize(connection, table, &format!("__key IN ({parts})"))?;
    }
    materialize(
        connection,
        "creatoridentifier",
        "__key IN (SELECT bhl_id(CreatorID) FROM bhl_creator
                   UNION SELECT bhl_id(CreatorID) FROM bhl_partcreator)",
    )?;
    materialize(
        connection,
        "pagename",
        "__key IN (SELECT page_id FROM bhl_selection)",
    )?;
    materialize(
        connection,
        "doi",
        &format!(
            "(lower(EntityType) = 'title' AND __key IN ({titles})) OR
             (lower(EntityType) = 'item' AND __key IN (SELECT id FROM bhl_scope_parent_ids)) OR
             (lower(EntityType) = 'page' AND __key IN (SELECT page_id FROM bhl_selection)) OR
             (lower(EntityType) = 'part' AND __key IN ({parts}))"
        ),
    )
}
