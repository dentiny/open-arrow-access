# ATIF fixtures

Project-authored synthetic ATIF v1.8 trajectories, with Unicode text, a tool
call and observation, and an unknown root field. No external dataset is used.

- `trajectory.json`: one pretty-printed document.
- `trajectories.jsonl`: three ordered documents, CRLF, blank lines, no final newline.

The fixture trajectory IDs are `first`, `second`, `third`.

Invalid JSONL files start with `trajectory.json` with its physical newlines
removed, then a newline and one invalid second record. They exercise discarding
an incomplete batch while preserving the record-2 error context:

- `bad_truncated.jsonl`: second record is `{`, without a final newline.
- `bad_array.jsonl`: second record is `[]`.
- `bad_utf8.jsonl`: second record is the invalid UTF-8 byte `ff`.
- `bad_missing_fields.jsonl`: second record is `{}`.
- `bad_version.jsonl`: second record uses `ATIF-v9.0` instead of `ATIF-v1.8`.
- `bad_late_record.jsonl`: valid first record, then `{` and a final newline;
  exercises returning the first batch before a later failure.
- `empty.jsonl`: zero bytes.
- `whitespace.jsonl`: space, tab, CR, LF, LF. These last two are valid empty
  JSONL streams but invalid single-document ATIF JSON.
