# arXiv XML attachment

`article.xml` is a project-authored fixture covered by this repository's MIT
license, representing an XML file submitted with a source package. It is not
an arXiv full-text XML download format or an OAI-PMH response.

Tests verify both byte-exact original XML and structured fields through the
existing `CrawlAccessor::attachments` and XML reader. No new format parser or
arXiv download route is required.

```sh
cargo test --features format-xml --test arxiv_xml
```
