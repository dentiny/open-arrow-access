# API metadata

`ApiDocument::from_bytes(bytes)` parses an API response into a single
`serde_json::Value`. `value()` exposes all parsed fields, including unknown fields.
The input buffer is released after parsing.

Provider file content uses the existing byte-reader contracts. JSON metadata
uses its parsed representation; callers can serialize that value if needed.
