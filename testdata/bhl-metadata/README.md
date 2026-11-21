# BHL AWS metadata TSV fixtures

Project-authored gzip TSV inputs matching the fixed 13-table / 104-field BHL
schema. They contain no downloaded data. Readers preserve original lexical IDs,
literal quotes, Unicode, empty cells, relation duplicates and source row order.

The fixture joins title 001, item 2, page 3 and part 8. The page sequence is 5;
partpage instead has sequence 1, while part.SequenceOrder is 900. Item and part
rights differ. Two identical creator identifiers remain duplicate rows. A DOI
with the same numeric ID and a different entity type must not join.

`title-duplicate.txt.gz` adds numeric ID 1 after lexical ID 001 and must fail the
singleton collision check. `title-bad-header.txt.gz` changes TitleID to WrongID
and must fail exact header validation. Other files use their table's AWS name.

`title-invalid-key.txt.gz` has a nonnumeric TitleID and must fail loading.
Tests load these committed gzip files. SQL-specific tests also extend the loaded
fixture tables to check independent relation aggregation and conflicting parents.
