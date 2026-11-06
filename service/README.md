# Dataset services

Provider-specific discovery starts with Hugging Face behind the default-off
`huggingface` feature (`service-huggingface` on the facade).

`HuggingFaceService` owns repository/revision/config/split selection. `resolve`
reads the selected revision's Hub metadata and dataset-card configuration,
expands declared native-file patterns against that manifest, and returns a
commit-pinned `HuggingFaceDataset`. Explicit path/pattern order is preserved;
matches within each pattern are sorted, and overlapping matches are deduplicated.
Missing patterns fail the whole selection. Paths, path lists, named splits and
`data_dir` are supported; script execution, inferred split conventions, external
URLs and Dataset Viewer conversion are outside this implementation.

`repository` exposes explicit single-file access and recursive file discovery.
The private OpenDAL HF constructor delegates byte reads and reader I/O budgets
to `storage::OpenDalAccessor`. Existing format accessors consume those shared
byte contracts; dataset-wide schema and multi-file Arrow scans remain in
`dataset/`. A service using a floating revision directly makes no snapshot claim.

Discovery uses the Hub metadata API, with a 32 MiB response limit. Connection
options and effective HF credentials are shared by metadata and file requests.
Cached token lookup runs on the blocking pool. OpenDAL owns HTTP/Xet transfers.

Protocol references:
- [Hub manual configuration](https://huggingface.co/docs/hub/datasets-manual-configuration)
- [Hub dataset metadata](https://huggingface.co/docs/huggingface_hub/package_reference/hf_api#huggingface_hub.HfApi.dataset_info)

The [source-attribute proposal](../docs/source-attributes.md) defines provider
constraints and conditional planning. Sequential cursor-based file discovery
can coexist with independent file reads; dependent API page requests require
a different plan. Services retain their own ordering requirements while
delegating discovered representations to dataset and format accessors.
