Synthetic single-member USTAR shards wrapping existing repository fixtures.
PDF wraps arxiv-articles/paper.pdf, LaTeX wraps arxiv-articles/main.tex.gz,
and XML wraps arxiv-xml/article.xml. Member names identify 2410.00001v1.
No downloaded paper or credentials; all header timestamps are zero.
latex-two.tar wraps the existing main.tex.gz fixture twice with distinct IDs,
for the gated concurrent-member read regression.

metadata.xml is a synthetic OAI-PMH page for content enrichment tests. It contains
three records in order: legacy hep-th/9901001, modern 2410.00001, and deleted
2410.99999. Active records include a title, authors, abstract, DOI, license and
two versions. Tests select rows or duplicate decoded batches for missing-match
and duplicate-ID cases; no network access is required.
