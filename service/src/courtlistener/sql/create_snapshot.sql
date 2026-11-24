-- Created after all four imports and primary-key validation have succeeded.
-- Exactly one bound manifest row marks the completed staged database.
CREATE TABLE courtlistener_snapshot (
    version INTEGER NOT NULL,
    snapshot_date VARCHAR NOT NULL,
    source_root VARCHAR NOT NULL,
    schema_contract VARCHAR NOT NULL
);
