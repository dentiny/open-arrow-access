# Committed test inputs

Read serialized input from this directory in both valid and invalid test cases.
Use `fixture!(format, name)` for bytes or `fixture_path!(format, name)` when a
filesystem backend needs to copy a fixture into an isolated temporary directory.
Tests must not create their input with inline documents, encoders, string
replacement, or binary corruption. Commit the resulting sample instead, with a
name and README entry describing the case it covers.

These are local regression samples; see each directory's README for provenance.
Many are project-authored examples, not captures from an external dataset. They
require no network access. Invalid files are intentional: preserve duplicate
keys, invalid UTF-8, whitespace, missing final newlines, and corrupt binary fields.

Keep expected values, schemas, projections, request parameters, and assertions
in test code. Test doubles may still model invalid decoder output, split fixture
bytes into chunks, inject I/O failures, or truncate a temporary copy after opening
it: these exercise reader behavior rather than define serialized file contents.
HTTP transports may select, paginate, and serialize entries loaded from fixtures.

Format and provider directories contain their own samples. `bytes/` contains
format-independent inputs for byte-reader, storage, and decoder contracts.

`openstax/book.zip` is a project-authored minimal GitHub source ZIP layout with one CNXML module, used to verify provider-to-ZIP composition. It contains no upstream book content.
