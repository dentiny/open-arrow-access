-- The only joins/relationship unions are here. Keep source order via __ordinal;
-- aggregate each relation separately so multiple 1:N relations cannot multiply.
WITH parents AS (
    SELECT s.*, p.__record AS page, pt.__record AS part,
        CASE WHEN s.entity_type = 'part'
             THEN coalesce(bhl_id(pt.ItemID), pp.item_id, bhl_id(p.ItemID))
             ELSE s.entity_id END AS parent_id
    FROM bhl_selection s
    LEFT JOIN bhl_page p ON p.__key = s.page_id
    LEFT JOIN bhl_part pt ON s.entity_type = 'part' AND pt.__key = s.entity_id
    LEFT JOIN LATERAL (
        SELECT bhl_id(ItemID) AS item_id FROM bhl_partpage
        WHERE s.entity_type = 'part' AND __key = s.entity_id
          AND bhl_id(PageID) = s.page_id
        ORDER BY __ordinal LIMIT 1
    ) pp ON true
), base AS (
    SELECT p.*, i.__record AS item, t.__record AS title,
        bhl_id(i.TitleID) AS title_id,
        CASE WHEN p.parent_id IS NULL THEN 'None'
             ELSE format('Some({})', p.parent_id) END AS parent_repr
    FROM parents p
    LEFT JOIN bhl_item i ON i.__key = p.parent_id
    LEFT JOIN bhl_title t ON t.__key = bhl_id(i.TitleID)
), joined AS (
    SELECT b.*,
        (SELECT coalesce(list(r.__record ORDER BY r.__ordinal), [])
         FROM bhl_partpage r WHERE b.entity_type = 'part'
         AND r.__key = b.entity_id AND bhl_id(r.PageID) = b.page_id) AS partpage,
        (SELECT coalesce(list(r.__record ORDER BY r.__ordinal), [])
         FROM bhl_creator r WHERE r.__key = b.title_id) AS creator,
        (SELECT coalesce(list(r.__record ORDER BY r.__ordinal), [])
         FROM bhl_partcreator r WHERE b.entity_type = 'part'
         AND r.__key = b.entity_id) AS partcreator,
        (SELECT coalesce(list(r.__record ORDER BY r.__ordinal), [])
         FROM bhl_creatoridentifier r WHERE EXISTS (
             SELECT 1 FROM bhl_creator c
             WHERE c.__key = b.title_id AND bhl_id(c.CreatorID) = r.__key
             UNION ALL
             SELECT 1 FROM bhl_partcreator c
             WHERE b.entity_type = 'part' AND c.__key = b.entity_id
               AND bhl_id(c.CreatorID) = r.__key
         )) AS creatoridentifier,
        (SELECT coalesce(list(r.__record ORDER BY r.__ordinal), [])
         FROM bhl_subject r WHERE r.__key = b.title_id) AS subject,
        (SELECT coalesce(list(r.__record ORDER BY r.__ordinal), [])
         FROM bhl_titleidentifier r WHERE r.__key = b.title_id) AS titleidentifier,
        (SELECT coalesce(list(r.__record ORDER BY r.__ordinal), [])
         FROM bhl_partidentifier r WHERE b.entity_type = 'part'
         AND r.__key = b.entity_id) AS partidentifier,
        (SELECT coalesce(list(r.__record ORDER BY r.__ordinal), [])
         FROM bhl_pagename r WHERE r.__key = b.page_id) AS pagename,
        (SELECT coalesce(list(r.__record ORDER BY r.__ordinal), [])
         FROM bhl_doi r WHERE
             (lower(r.EntityType) = 'title' AND r.__key = b.title_id)
          OR (lower(r.EntityType) = 'item' AND r.__key = b.parent_id)
          OR (lower(r.EntityType) = 'page' AND r.__key = b.page_id)
          OR (lower(r.EntityType) = 'part' AND b.entity_type = 'part'
              AND r.__key = b.entity_id)) AS doi,
        (SELECT coalesce(list(message ORDER BY kind, ordinal), []) FROM (
            SELECT 0 AS kind, 0::UBIGINT AS ordinal,
                format('page.ItemID={}, selected item={}', bhl_id(b.page.ItemID), b.parent_repr) AS message
            WHERE bhl_id(b.page.ItemID) IS NOT NULL
              AND bhl_id(b.page.ItemID) IS DISTINCT FROM b.parent_id
            UNION ALL
            SELECT 1, 0,
                format('part.ItemID={}, selected item={}', bhl_id(b.part.ItemID), b.parent_repr)
            WHERE bhl_id(b.part.ItemID) IS NOT NULL
              AND bhl_id(b.part.ItemID) IS DISTINCT FROM b.parent_id
            UNION ALL
            SELECT 2, r.__ordinal,
                format('partpage.ItemID={}, selected item={}', bhl_id(r.ItemID), b.parent_repr)
            FROM bhl_partpage r WHERE b.entity_type = 'part'
              AND r.__key = b.entity_id AND bhl_id(r.PageID) = b.page_id
              AND bhl_id(r.ItemID) IS NOT NULL
              AND bhl_id(r.ItemID) IS DISTINCT FROM b.parent_id
            UNION ALL
            SELECT 3, 0,
                format('SequenceOrder={}, filename sequence={}', bhl_id(b.page.SequenceOrder), b.sequence)
            WHERE b.entity_type = 'item' AND bhl_id(b.page.SequenceOrder) IS NOT NULL
              AND bhl_id(b.page.SequenceOrder) <> b.sequence
            UNION ALL
            SELECT 3, r.__ordinal,
                format('SequenceOrder={}, filename sequence={}', bhl_id(r.SequenceOrder), b.sequence)
            FROM bhl_partpage r WHERE b.entity_type = 'part'
              AND r.__key = b.entity_id AND bhl_id(r.PageID) = b.page_id
              AND bhl_id(r.SequenceOrder) IS NOT NULL
              AND bhl_id(r.SequenceOrder) <> b.sequence
        ) messages) AS conflicts
    FROM base b
)
SELECT request_id,
    struct_pack(
        title := title, item := item, page := page, part := part,
        partpage := partpage, creator := creator,
        creatoridentifier := creatoridentifier, subject := subject,
        titleidentifier := titleidentifier, partcreator := partcreator,
        partidentifier := partidentifier, pagename := pagename, doi := doi
    ) AS metadata,
    list_filter([
        CASE WHEN page IS NULL THEN format('page:{}', page_id) END,
        CASE WHEN entity_type = 'part' AND part IS NULL
             THEN format('part:{}', entity_id) END,
        CASE WHEN entity_type = 'part' AND len(partpage) = 0
             THEN format('partpage:{}:{}', entity_id, page_id) END,
        CASE WHEN item IS NULL THEN 'item:' || coalesce(parent_id::VARCHAR, 'unresolved parent') END,
        CASE WHEN title IS NULL THEN 'title:' || coalesce(title_id::VARCHAR, 'unresolved parent') END
    ], lambda value: value IS NOT NULL) AS missing,
    conflicts
FROM joined
ORDER BY request_id
