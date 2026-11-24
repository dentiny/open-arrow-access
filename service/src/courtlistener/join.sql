SELECT
    CAST($1 AS DATE) AS snapshot_date,
    {opinion} AS opinion,
    {cluster} AS cluster,
    {docket} AS docket,
    {court} AS court,
    struct_pack(
        opinions_source_uri := CAST($2 AS VARCHAR),
        clusters_source_uri := CAST($3 AS VARCHAR),
        dockets_source_uri := CAST($4 AS VARCHAR),
        courts_source_uri := CAST($5 AS VARCHAR),
        opinion_record_index := o.{ordinal},
        cluster_record_index := c.{ordinal},
        docket_record_index := d.{ordinal},
        court_record_index := k.{ordinal}
    ) AS provenance
FROM opinions o
LEFT JOIN opinion_clusters c ON o.cluster_id = c.id
LEFT JOIN dockets d ON c.docket_id = d.id
LEFT JOIN courts k ON d.court_id = k.id
ORDER BY o.{ordinal}
