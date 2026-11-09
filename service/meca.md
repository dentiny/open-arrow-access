# MECA archives

Enable `meca` (`service-meca` on the facade). `MecaArchive::open(reader, ZipLimits::default())` accepts an existing ranged, known-length byte reader, including an OpenDAL S3 reader configured by the caller for requester-pays access.

The existing ZIP implementation handles storage ranges, compression and limits. Only the directory and manifest are decoded initially. `manifest()` retains exact XML, `files()` preserves ordered manifest instances, and `archive().open_entry(file.entry_index, chunk_size)` opens a selected payload. All ZIP members, including unlisted files, remain accessible. No external DTD is fetched. Paths resolve percent-encoded relative URI references and reject traversal, missing or ambiguous members. This validates the envelope and references, not every MECA schema rule.

Sources: [MECA specification resources](https://meca.zip/) and [NISO transfer manifest example](https://www.ncbi.nlm.nih.gov/books/NBK555480/bin/randall-ubnoske_app.pdf).
