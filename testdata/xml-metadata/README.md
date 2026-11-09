# XML metadata fixtures

These project-authored inputs cover service XML envelope validation and document
decoding. They use fictional data and are covered by the repository's MIT license.
No network access is required; external DTD declarations must not be fetched.

- `namespaced-header.xml`: a prefixed manifest with an XML declaration, comment,
  and external DTD declaration.
- `oai.xml`: an OAI-PMH envelope with an empty ListRecords element.
- `namespaces-entities.xml`: a prefixed manifest with an external DTD declaration
  and a title containing `&amp;`, decoded as `A & B`.
- `wrong-root.xml`: a valid XML document whose root is not `manifest`.
- `empty.xml`: an intentionally empty input with no root element.
- `multiple-roots.xml`: two root elements; full-document decoding must fail.
- `truncated.xml`: a manifest missing its closing tag.
- `trailing-entity.xml`: an unknown entity after the closed root element.
- `internal-dtd.xml`: an unsupported internal DTD subset defining an entity.

The fixture bytes match the original inline test inputs exactly, including the
absence of final newlines. Keep malformed and empty inputs unchanged.
