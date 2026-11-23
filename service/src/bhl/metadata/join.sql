-- The only joins/relationship unions are here. Keep source order via rowid;
-- aggregate each relation separately so multiple 1:N relations cannot multiply.
WITH parents AS (
    SELECT s.*, p.records AS page, pt.records AS part,
        CASE WHEN s.entity_type = 'part'
             THEN coalesce(pt.item_id, pp.item_id, p.item_id)
             ELSE s.entity_id END AS parent_id
    FROM bhl_selection s
    LEFT JOIN LATERAL (
        SELECT coalesce(list(__record ORDER BY rowid), []) AS records,
            CASE WHEN count(DISTINCT bhl_id(ItemID)) = 1
                    THEN min(bhl_id(ItemID)) END AS item_id
        FROM bhl_page WHERE __key = s.page_id
    ) p ON true
    LEFT JOIN LATERAL (
        SELECT coalesce(list(__record ORDER BY rowid), []) AS records,
            CASE WHEN count(DISTINCT bhl_id(ItemID)) = 1
                    THEN min(bhl_id(ItemID)) END AS item_id
        FROM bhl_part WHERE s.entity_type = 'part' AND __key = s.entity_id
    ) pt ON true
    LEFT JOIN LATERAL (
        SELECT bhl_id(ItemID) AS item_id FROM bhl_partpage
        WHERE s.entity_type = 'part' AND __key = s.entity_id
          AND bhl_id(PageID) = s.page_id
        ORDER BY rowid LIMIT 1
    ) pp ON true
), base AS (
    SELECT p.*,
        (SELECT coalesce(list(i.__record ORDER BY i.rowid), [])
         FROM bhl_item i WHERE i.__key = p.parent_id) AS item,
        (SELECT coalesce(list(t.__record ORDER BY t.rowid), [])
         FROM bhl_title t WHERE EXISTS (
             SELECT 1 FROM bhl_item i WHERE i.__key = p.parent_id
               AND bhl_id(i.TitleID) = t.__key)) AS title,
        CASE WHEN p.parent_id IS NULL THEN 'None'
             ELSE format('Some({})', p.parent_id) END AS parent_repr
    FROM parents p
), joined AS (
    SELECT b.*,
        (SELECT coalesce(list(r.__record ORDER BY r.rowid), [])
         FROM bhl_partpage r WHERE b.entity_type = 'part'
         AND r.__key = b.entity_id AND bhl_id(r.PageID) = b.page_id) AS partpage,
        (SELECT coalesce(list(r.__record ORDER BY r.rowid), [])
         FROM bhl_creator r WHERE r.__key IN (SELECT bhl_id(i.TitleID) FROM bhl_item i WHERE i.__key = b.parent_id)) AS creator,
        (SELECT coalesce(list(r.__record ORDER BY r.rowid), [])
         FROM bhl_partcreator r WHERE b.entity_type = 'part'
         AND r.__key = b.entity_id) AS partcreator,
        (SELECT coalesce(list(r.__record ORDER BY r.rowid), [])
         FROM bhl_creatoridentifier r WHERE r.__key IN (
             SELECT bhl_id(c.CreatorID) FROM bhl_creator c
             JOIN bhl_item i ON c.__key = bhl_id(i.TitleID)
             WHERE i.__key = b.parent_id
             UNION ALL
             SELECT bhl_id(c.CreatorID) FROM bhl_partcreator c
             WHERE b.entity_type = 'part' AND c.__key = b.entity_id
         )) AS creatoridentifier,
        (SELECT coalesce(list(r.__record ORDER BY r.rowid), [])
         FROM bhl_subject r WHERE r.__key IN (SELECT bhl_id(i.TitleID) FROM bhl_item i WHERE i.__key = b.parent_id)) AS subject,
        (SELECT coalesce(list(r.__record ORDER BY r.rowid), [])
         FROM bhl_titleidentifier r WHERE r.__key IN (SELECT bhl_id(i.TitleID) FROM bhl_item i WHERE i.__key = b.parent_id)) AS titleidentifier,
        (SELECT coalesce(list(r.__record ORDER BY r.rowid), [])
         FROM bhl_partidentifier r WHERE b.entity_type = 'part'
         AND r.__key = b.entity_id) AS partidentifier,
        (SELECT coalesce(list(r.__record ORDER BY r.rowid), [])
         FROM bhl_pagename r WHERE r.__key = b.page_id) AS pagename,
        -- Separate disjoint entity types before aggregation. A correlated title
        -- membership test inside OR otherwise expands DOI rows across pages.
        (SELECT coalesce(list(r.__record ORDER BY r.rowid), []) FROM (
            SELECT d.rowid, d.__record FROM bhl_doi d
            JOIN (SELECT DISTINCT __key, bhl_id(TitleID) AS title_id FROM bhl_item) i
              ON d.__key = i.title_id
            WHERE lower(d.EntityType) = 'title' AND i.__key = b.parent_id
            UNION ALL
            SELECT d.rowid, d.__record FROM bhl_doi d
            WHERE lower(d.EntityType) = 'item' AND d.__key = b.parent_id
            UNION ALL
            SELECT d.rowid, d.__record FROM bhl_doi d
            WHERE lower(d.EntityType) = 'page' AND d.__key = b.page_id
            UNION ALL
            SELECT d.rowid, d.__record FROM bhl_doi d
            WHERE lower(d.EntityType) = 'part' AND b.entity_type = 'part'
              AND d.__key = b.entity_id
        ) r) AS doi,
        (SELECT coalesce(list(message ORDER BY kind, ordinal), []) FROM (
            SELECT 0 AS kind, r.rowid AS ordinal,
                format('page.ItemID={}, selected item={}', bhl_id(r.ItemID), b.parent_repr) AS message
            FROM bhl_page r WHERE r.__key = b.page_id
              AND bhl_id(r.ItemID) IS NOT NULL
              AND bhl_id(r.ItemID) IS DISTINCT FROM b.parent_id
            UNION ALL
            SELECT 1, r.rowid,
                format('part.ItemID={}, selected item={}', bhl_id(r.ItemID), b.parent_repr)
            FROM bhl_part r WHERE b.entity_type = 'part' AND r.__key = b.entity_id
              AND bhl_id(r.ItemID) IS NOT NULL
              AND bhl_id(r.ItemID) IS DISTINCT FROM b.parent_id
            UNION ALL
            SELECT 2, r.rowid,
                format('partpage.ItemID={}, selected item={}', bhl_id(r.ItemID), b.parent_repr)
            FROM bhl_partpage r WHERE b.entity_type = 'part'
              AND r.__key = b.entity_id AND bhl_id(r.PageID) = b.page_id
              AND bhl_id(r.ItemID) IS NOT NULL
              AND bhl_id(r.ItemID) IS DISTINCT FROM b.parent_id
            UNION ALL
            SELECT 3, r.rowid,
                format('SequenceOrder={}, filename sequence={}', bhl_id(r.SequenceOrder), b.sequence)
            FROM bhl_page r WHERE r.__key = b.page_id
              AND b.entity_type = 'item' AND bhl_id(r.SequenceOrder) IS NOT NULL
              AND bhl_id(r.SequenceOrder) <> b.sequence
            UNION ALL
            SELECT 3, r.rowid,
                format('SequenceOrder={}, filename sequence={}', bhl_id(r.SequenceOrder), b.sequence)
            FROM bhl_partpage r WHERE b.entity_type = 'part'
              AND r.__key = b.entity_id AND bhl_id(r.PageID) = b.page_id
              AND bhl_id(r.SequenceOrder) IS NOT NULL
              AND bhl_id(r.SequenceOrder) <> b.sequence
        ) messages) AS conflicts
    FROM base b
), sequences AS (
    SELECT *, CASE WHEN entity_type = 'item'
        THEN list_distinct(list_transform(page, lambda r: bhl_id(r.SequenceOrder)))
        ELSE list_distinct(list_transform(partpage, lambda r: bhl_id(r.SequenceOrder)))
        END AS expected_sequences
    FROM joined
)
SELECT request_id,
    struct_pack(
        title := title, item := item, page := page, part := part,
        partpage := partpage, creator := creator,
        creatoridentifier := creatoridentifier, subject := subject,
        titleidentifier := titleidentifier, partcreator := partcreator,
        partidentifier := partidentifier, pagename := pagename, doi := doi
    ) AS metadata,
    list_concat(list_filter([
        CASE WHEN len(page) = 0 THEN format('page:{}', page_id) END,
        CASE WHEN entity_type = 'part' AND len(part) = 0
             THEN format('part:{}', entity_id) END,
        CASE WHEN entity_type = 'part' AND len(partpage) = 0
             THEN format('partpage:{}:{}', entity_id, page_id) END,
        CASE WHEN len(item) = 0 THEN 'item:' || coalesce(parent_id::VARCHAR, 'unresolved parent') END
    ], lambda value: value IS NOT NULL),
        CASE WHEN len(item) = 0 THEN ['title:unresolved parent'] ELSE [] END,
        (SELECT coalesce(list('title:' || coalesce(bhl_id(i.TitleID)::VARCHAR, 'unresolved parent') ORDER BY i.rowid), [])
         FROM bhl_item i LEFT JOIN bhl_title t ON t.__key = bhl_id(i.TitleID)
         WHERE i.__key = joined.parent_id AND t.__key IS NULL)
    ) AS missing,
    conflicts,
    min(request_id) FILTER (WHERE expected_sequences = [sequence]) OVER versions AS canonical_id,
    (count(*) FILTER (WHERE expected_sequences = [sequence]) OVER versions)::UBIGINT AS canonical_matches
FROM sequences AS joined
WINDOW versions AS (PARTITION BY entity_type, entity_id, page_id)
ORDER BY request_id
