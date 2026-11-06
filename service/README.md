# Dataset services

The default-off `huggingface` feature (`service-huggingface` on the facade)
provides `HuggingFaceService::repository` for explicit dataset repository files.
Provider-owned types select repository ID, revision, root and connection options.

The internal OpenDAL HF constructor delegates stat, recursive file listing,
exact ranges and bounded sequential reads to `storage::OpenDalAccessor`.
Wrap its byte accessor with our `ParquetAccessor` or another existing format
accessor to interpret records. Provider code does not decode file formats.

Metadata/options and I/O error categories are preserved, credentials are
redacted, and cached-token lookup runs on the blocking pool. HTTP/Xet transfer
protocols remain delegated to OpenDAL. Floating revisions are not snapshots.

Config/split selection and commit-pinned dataset discovery are implemented in
the dependent dataset-resolution PR. Multi-file Arrow/schema composition
remains in `dataset/`.
