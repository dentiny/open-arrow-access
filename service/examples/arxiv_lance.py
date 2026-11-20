"""Optional downstream Lance Blob V2 bridge for the arXiv artifact Arrow schema.

Requires pylance and pyarrow; these are not open-arrow-access dependencies.
Run with an output path to verify a synthetic, byte-exact round trip. Production
callers pass bounded Arrow tables/batches emitted by the Rust article/source APIs.
"""

import sys

import lance
import pyarrow as pa
from lance import blob_array, blob_field


def write_artifacts(table: pa.Table, destination: str):
    """Create one dataset; do not use concurrently against the same destination.

    Convert a bounded batch/table at a time in production. This example converts
    its input binary column to Python bytes before passing it to Blob V2.
    """
    schema = pa.schema(
        [blob_field("data") if field.name == "data" else field for field in table.schema],
        metadata=table.schema.metadata,
    )
    arrays = [
        blob_array(column.to_pylist()) if name == "data" else column
        for name, column in zip(table.column_names, table.columns)
    ]
    return lance.write_dataset(
        pa.Table.from_arrays(arrays, schema=schema),
        destination,
        data_storage_version="2.2",
        mode="create",
    )


def main(destination: str):
    # Synthetic bytes test storage fidelity, not PDF/image/video validity.
    payloads = [b"%PDF-example", b"\\includegraphics{figures/a.png}", b"\x89PNG\x00\xff", b"\x00video\xff"]
    fields = [
        pa.field("article_id", pa.large_string(), nullable=False),
        pa.field("format", pa.large_string(), nullable=False),
        pa.field("source_uri", pa.large_string(), nullable=False),
        pa.field("entry_index", pa.uint64(), nullable=False),
        pa.field("entry_name", pa.large_binary(), nullable=False),
        pa.field("data", pa.large_binary(), nullable=False),
    ]
    table = pa.Table.from_pydict(
        {
            "article_id": ["2410.00001v1"] * 4,
            "format": ["pdf", "source_member", "source_member", "source_member"],
            "source_uri": ["pdf/2410.00001v1"] + ["src/2410.00001v1"] * 3,
            "entry_index": [0, 0, 1, 2],
            "entry_name": [b"", b"main.tex", b"figures/a.png", b"anc/demo.mp4"],
            "data": payloads,
        },
        schema=pa.schema(fields),
    )
    dataset = write_artifacts(table, destination)
    assert dataset.schema.field("data").type.extension_name == "lance.blob.v2"
    metadata = dataset.to_table(columns=["article_id", "format", "entry_name"])
    assert metadata.num_rows == 4
    assert metadata.column("entry_name").to_pylist() == table.column("entry_name").to_pylist()
    assert [value for _, value in dataset.read_blobs("data", indices=list(range(4)))] == payloads
    print(f"Verified {len(payloads)} artifact rows with Blob V2 in {destination}; Lance {lance.__version__}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: arxiv_lance.py <new-output.lance>")
    main(sys.argv[1])
