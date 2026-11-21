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
FROM cl_0 o
LEFT JOIN cl_1 c ON o.cluster_id = c.id
LEFT JOIN cl_2 d ON c.docket_id = d.id
LEFT JOIN cl_3 k ON d.court_id = k.id
ORDER BY o.{ordinal}
