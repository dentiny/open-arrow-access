# Decompression extension point

Reserved for byte accessor/reader wrappers, starting with gzip. The initial PR
does not support external compression. A decoder uses the common `Layer`,
`Accessor` and `ByteReader` contracts, updates byte metadata, and accurately
reports range capability. Gzip normally produces a sequential-only reader;
random-access consumers need an explicit materialization or indexing strategy.
