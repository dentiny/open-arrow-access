# Gutenberg fixture

`book.rdf` is synthetic per-eBook metadata using Gutenberg's RDF namespace
layout. It covers nested metadata, alternate namespace prefixes, multiple file
formats and an unexpanded upstream field. No upstream book content is included.

`metadata_only.rdf` contains only ebook ID 1, with all optional metadata absent.
The invalid samples are copies of `book.rdf` with one deliberate change:

| Fixture | Change |
| --- | --- |
| `bad_ebook_id.rdf` | `ebooks/42` becomes `ebooks/invalid`. |
| `bad_date.rdf` | `1970-01-02` becomes the nonexistent `2026-02-30`. |
| `bad_file_uri.rdf` | Text URI `https://example.invalid/42.txt` is emptied. |
| `bad_namespace.rdf` | The ebook element overrides prefix `p` with `urn:wrong`. |
| `bad_multiple_books.rdf` | A second ebook, ID 43, is added before the RDF closing tag. |
