# ATIF fixtures

Synthetic ATIF v1.8 trajectories with Unicode, tool calls/observations, and an
unknown root field. Trajectory IDs are `first`, `second`, and `third`.

| Input | Case |
| --- | --- |
| `trajectory.json` | One pretty-printed trajectory. |
| `trajectories.jsonl` | Three ordered trajectories; CRLF, blank lines, no final newline. |
| `trajectories.jsonl.gz` | Gzip version for independent compressed-file reads. |
| `repeated-trajectories.jsonl.gz` | Whitespace and two copies of the JSONL fixture; six records for global indices, raw JSON, projection, and parallel framing. |
| `bad_truncated.jsonl`, `bad_array.jsonl`, `bad_utf8.jsonl` | Valid first record, then `{`, `[]`, or invalid UTF-8 byte `ff`. |
| `bad_missing_fields.jsonl`, `bad_version.jsonl` | Second record is `{}` or uses `ATIF-v9.0`. |
| `bad_late_record.jsonl` | Valid first record followed by `{` and a newline; failure after an emitted batch. |
| `empty.jsonl`, `whitespace.jsonl` | Zero bytes or space/tab/CR/LF/LF; valid empty JSONL, invalid single-document ATIF. |

Invalid JSONL cases begin with the physical newlines removed from the valid
trajectory, followed by an invalid second record. Both gzip fixtures were
prepared with a zero timestamp; all inputs are project-authored.

## Use these inputs

Facade feature: `format-atif`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-file-format --no-default-features --features atif
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
