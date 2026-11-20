Synthetic single-member USTAR shards wrapping existing repository fixtures.
PDF wraps arxiv-articles/paper.pdf, LaTeX wraps arxiv-articles/main.tex.gz,
and XML wraps arxiv-xml/article.xml. Member names identify 2410.00001v1.
No downloaded paper or credentials; all header timestamps are zero.
latex-two.tar wraps the existing main.tex.gz fixture twice with distinct IDs,
for the gated concurrent-member read regression.

large-attachments.tar.gz preserves the entries of arxiv-articles/source.tar,
with 2 MiB of zero padding added to figures/a.png and 4 MiB to anc/demo.mp4,
plus 128 KiB after the TAR end markers. Gzip mtime is zero.
large-attachments-bad-crc.tar.gz differs only by flipping bit 0 of the gzip CRC
trailer. These fixtures check incremental output, early attachment exclusion
and decoder validation after TAR EOF.
