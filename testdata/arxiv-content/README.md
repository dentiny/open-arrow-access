Synthetic single-member USTAR shards wrapping existing repository fixtures.
PDF wraps arxiv-articles/paper.pdf, LaTeX wraps arxiv-articles/main.tex.gz,
and XML wraps arxiv-xml/article.xml. Member names identify 2410.00001v1.
No downloaded paper or credentials; all header timestamps are zero.
latex-two.tar wraps the existing main.tex.gz fixture twice with distinct IDs,
for the gated concurrent-member read regression.

large-attachments-bad-crc.tar.gz preserves arxiv-articles/source.tar entries,
with 2 MiB of zero padding added to figures/a.png, 4 MiB to anc/demo.mp4,
and 128 KiB after the TAR end markers. Gzip mtime is zero; bit 0 of the gzip
CRC trailer is flipped. It verifies that TeX output precedes a trailer error.
