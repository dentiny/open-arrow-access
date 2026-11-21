Synthetic single-member USTAR shards wrapping existing repository fixtures.
PDF wraps arxiv-articles/paper.pdf, LaTeX wraps arxiv-articles/main.tex.gz,
and XML wraps arxiv-xml/article.xml. Member names identify 2410.00001v1.
No downloaded paper or credentials; all header timestamps are zero.
mixed-source.tar contains a PDF-only article, one synthetic TeX article, and a
PostScript-only article. anc-only.tar contains only anc/extra.tex. They verify
article-level missing-LaTeX reporting without metadata lookups for skipped articles.
latex-two.tar wraps the existing main.tex.gz fixture twice with distinct IDs,
for the gated concurrent-member read regression.

source-manifest.xml selects zero shards for 2409, one for 2410 and two for 2411.
Each selected object serves latex-two.tar. The 2412 row deliberately declares a
3073-byte payload lower bound, exceeding that fixture's 3072-byte object length,
to exercise manifest-length validation. Other rows use a valid one-byte lower
bound. Checksums are synthetic placeholders and are not verified by the reader.

large-attachments-bad-crc.tar.gz preserves arxiv-articles/source.tar entries,
with 2 MiB of zero padding added to figures/a.png, 4 MiB to anc/demo.mp4,
and 128 KiB after the TAR end markers. Gzip mtime is zero; bit 0 of the gzip
CRC trailer is flipped. It verifies that TeX output precedes a trailer error.

metadata.xml is a synthetic OAI-PMH page for content enrichment tests. It contains
three records in order: legacy hep-th/9901001, modern 2410.00001, and deleted
2410.99999. Active records include a title, authors, abstract, DOI, license and
two versions. Tests select rows or duplicate decoded batches for missing-match
and duplicate-ID cases; no network access is required.

metadata-record.xml, metadata-second.xml, metadata-legacy.xml and
metadata-deleted.xml are single-record GetRecord responses for the default
metadata lookup path. They retain the corresponding metadata.xml values (the
second response uses 2410.00002 for the two-article shard). metadata-missing.xml
is an OAI idDoesNotExist error; metadata-empty.xml has no records, and
metadata-incomplete.xml omits the article metadata.
All responses are synthetic and used with the in-memory HTTP transport.
