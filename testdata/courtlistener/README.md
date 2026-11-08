Synthetic PostgreSQL COPY-style CSV, not downloaded court records. Includes
UTF-8, quoted commas and newlines, backslash-escaped quotes/backslashes, an
unquoted NULL, and quoted empty strings. Recreate the compressed fixture with:

```python
from pathlib import Path
import bz2
root = Path("testdata/courtlistener")
(root / "table.csv.bz2").write_bytes(bz2.compress((root / "table.csv").read_bytes()))
```
