# arXiv original PDF and HTML

Project-authored inputs covered by this repository's MIT license. `paper.pdf`
is a minimal one-page PDF; `paper.html` is a small original HTML document.
`pdf/` and `html/` mirror article routes for local storage tests. Tests load the
committed bytes without generating documents at runtime.

```sh
cargo test -p open-arrow-access-service --features arxiv --test arxiv_articles
cargo test --features service-arxiv --test arxiv_documents
```
