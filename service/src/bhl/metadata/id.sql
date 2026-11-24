-- Query-local normalization; raw lexical values remain unchanged.
CREATE TEMP MACRO bhl_id(value) AS CASE
    WHEN regexp_full_match(value, '[+]?[0-9]+')
    THEN nullif(try_cast(value AS UBIGINT), 0) END;
