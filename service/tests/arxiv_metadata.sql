CREATE TABLE arxiv_metadata AS
SELECT article_id,
    struct_pack("oai:identifier" := 'oai:arXiv.org:' || source_id) AS "oai:header",
    struct_pack("arxiv:arXivRaw" := struct_pack(
        "arxiv:id" := source_id,
        "arxiv:submitter" := 'Submitter',
        "arxiv:title" := title,
        "arxiv:authors" := 'Author',
        "arxiv:categories" := 'cs.AI',
        "arxiv:comments" := NULL::VARCHAR,
        "arxiv:report-no" := NULL::VARCHAR,
        "arxiv:journal-ref" := NULL::VARCHAR,
        "arxiv:doi" := NULL::VARCHAR,
        "arxiv:license" := 'http://arxiv.org/licenses/nonexclusive-distrib/1.0/',
        "arxiv:abstract" := 'Abstract',
        "arxiv:version" := [struct_pack("@version" := 'v1', "arxiv:date" := '2026-01-01')]
    )) AS "oai:metadata"
FROM (VALUES ('2410.00001', '2410.00001', 'Modern'),
             ('hep-th9901001', 'hep-th/9901001', 'Legacy')) records(article_id, source_id, title);
CREATE UNIQUE INDEX arxiv_metadata_article_id_idx ON arxiv_metadata(article_id);
CHECKPOINT;
