-- Bound parameters: application format version, export date, source URI root,
-- and the exact versioned source-schema contract produced by CourtListenerSchemas.
INSERT INTO courtlistener_snapshot (
    version, snapshot_date, source_root, schema_contract
) VALUES (?, ?, ?, ?);
