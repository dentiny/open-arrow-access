# arXiv metadata

Enable `service-arxiv` on the facade, or `arxiv` on the service crate. This single feature enables both bulk PDF/source TAR access through `arxiv::ArxivService` and OAI-PMH metadata harvesting through `arxiv_metadata::ArxivMetadataService`.

Metadata harvesting uses the shared OAI-PMH protocol and our XML format reader, retains original XML pages and resumes using upstream tokens. It does not download PDF or source payloads.
