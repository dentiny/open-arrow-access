# BHL inventory/job fixtures

Project-authored saved JSON documents for offline AWS inventory and worker tests.
`inventory.json` has two item objects and one part object, sorted by exact key.
The item and part objects with PageID 65077706 intentionally remain separate
representations under explicit `scope: all`. Their contents are supplied locally.
`job.json` is the final one-object job when that inventory is split two at a time.

Invalid documents cover unsupported versions, duplicate numeric identities with
different padding, noncanonical key ordering, scope violations, out-of-range job
indices, and page counts inconsistent with the declared job sizing.
No document contains downloaded upstream content or a real immutable snapshot.

The owning tests are in `service/tests/bhl_open_data.rs` under feature `bhl`.
