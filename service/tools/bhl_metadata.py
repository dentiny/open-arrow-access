#!/usr/bin/env python3
"""Build a disk metadata index once; export raw table subsets for BHL OCR jobs."""
import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

PROFILE = json.loads((Path(__file__).resolve().parents[1] / 'src/bhl/pages/profile.json').read_text())
VERSION = 1


def compact(value):
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'))


def number(value):
    if not re.fullmatch(r'[0-9]+', value) or not 0 < int(value) <= 2**64-1:
        raise ValueError(f'invalid BHL numeric ID/sequence: {value!r}')
    return str(int(value))


def identity(key):
    match = re.fullmatch(r'ocr/((item|part)-([0-9]+))/\1-([0-9]+)-([0-9]+)\.txt', key)
    if match is None:
        raise ValueError(f'invalid OCR page key: {key}')
    return match[2], number(match[3]), number(match[4]), number(match[5])


class DigestReader:
    def __init__(self, inner):
        self.inner = inner
        self.digest = hashlib.sha256()

    def read(self, size=-1):
        data = self.inner.read(size)
        self.digest.update(data)
        return data


def build(directory, index, snapshot_id):
    """Atomic per-table import; resumptions use already committed table hashes.

    All table rows remain on disk. Relation duplicates retain their source ordinal.
    Source .gz files must be pinned locally and retained with the index provenance.
    """
    if not snapshot_id.strip():
        raise ValueError('snapshot_id must be nonempty')
    directory, index = Path(directory).resolve(), Path(index)
    db = sqlite3.connect(index)
    csv.field_size_limit(sys.maxsize)
    try:
        db.execute('PRAGMA journal_mode=WAL')
        with db:
            db.execute('CREATE TABLE IF NOT EXISTS sources (name TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS config (value TEXT NOT NULL)')
            expected = compact(dict(version=VERSION, snapshot_id=snapshot_id, directory=str(directory), profile=PROFILE))
            config = db.execute('SELECT value FROM config').fetchall()
            if config and config != [(expected,)]:
                raise ValueError('metadata snapshot/directory/profile differs; create a new index')
            if not config:
                db.execute('INSERT INTO config VALUES (?)', (expected,))
        for p in PROFILE:
            name = p['name']
            source = directory / (name+'.txt.gz')
            saved = db.execute('SELECT value FROM sources WHERE name=?', (name,)).fetchone()
            if saved:
                digest = hashlib.sha256()
                with source.open('rb') as file:
                    for block in iter(lambda: file.read(1024 * 1024), b''):
                        digest.update(block)
                if digest.hexdigest() != json.loads(saved[0])['sha256']:
                    raise ValueError(f'{name}: pinned source bytes changed; create a new index')
                continue
            # A failed file/header/row/gzip CRC rolls back the entire table.
            with db:
                db.execute('BEGIN IMMEDIATE')
                db.execute(f'CREATE TABLE "{name}" (position INTEGER PRIMARY KEY, id TEXT NOT NULL, row TEXT NOT NULL)')
                rows = 0
                with source.open('rb') as compressed:
                    hashed = DigestReader(compressed)
                    with gzip.GzipFile(fileobj=hashed) as zipped:
                        with io.TextIOWrapper(zipped, encoding='utf-8-sig', newline='') as text:
                            reader = csv.reader(text, delimiter='\t', quoting=csv.QUOTE_NONE)
                            if next(reader, None) != p['columns']:
                                raise ValueError(f'{name}: source header differs from all declared fields')
                            batch = []
                            for position, cells in enumerate(reader):
                                if len(cells) != len(p['columns']):
                                    raise ValueError(f'{name}: malformed TSV row {position+2}')
                                row = dict(zip(p['columns'], cells))
                                batch.append((position, number(row[p['key']]), compact(row)))
                                if len(batch) == 4096:
                                    db.executemany(f'INSERT INTO "{name}" VALUES (?,?,?)', batch)
                                    rows += len(batch)
                                    batch.clear()
                            db.executemany(f'INSERT INTO "{name}" VALUES (?,?,?)', batch)
                            rows += len(batch)
                unique = 'UNIQUE' if p['single'] else ''
                db.execute(f'CREATE {unique} INDEX "{name}_id" ON "{name}"(id)')
                provenance = dict(path=str(source), sha256=hashed.digest.hexdigest(), rows=rows)
                db.execute('INSERT INTO sources VALUES (?,?)', (name, compact(provenance)))
            print(compact(dict(table=name, **provenance)), flush=True)
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    finally:
        db.close()


class MetadataIndex:
    def __init__(self, path):
        self.db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True)
        self.db.execute('BEGIN')
        config = json.loads(self.db.execute('SELECT value FROM config').fetchone()[0])
        if config['version'] != VERSION or config['profile'] != PROFILE:
            self.close()
            raise ValueError('metadata index profile/version differs')
        self.snapshot_id = config['snapshot_id']
        self.sources = {name: json.loads(value) for name, value in self.db.execute('SELECT name,value FROM sources')}
        if set(self.sources) != {p['name'] for p in PROFILE}:
            self.close()
            raise ValueError('all 13 acquired metadata tables are required')

    def close(self):
        self.db.close()

    def select(self, keys):
        keys = list(keys)
        if not keys or len(set(keys)) != len(keys):
            raise ValueError('selected keys must be nonempty and unique')
        ids = [identity(key) for key in keys]
        page_ids = {p[2] for p in ids}
        part_ids = {p[1] for p in ids if p[0] == 'part'}
        item_ids = {p[1] for p in ids if p[0] == 'item'}
        def rows(table, identifiers):
            # Deduplicate source row positions across lookups, not relation values.
            found = {}
            for identifier in identifiers:
                for position, row in self.db.execute(f'SELECT position,row FROM "{table}" WHERE id=?', (identifier,)):
                    found[position] = json.loads(row)
            return [row for _, row in sorted(found.items())]
        tables = {}
        tables['page'] = rows('page', page_ids)
        tables['part'] = rows('part', part_ids)
        tables['partpage'] = [row for row in rows('partpage', part_ids) if number(row['PageID']) in page_ids]
        for row in tables['page'] + tables['part'] + tables['partpage']:
            if row['ItemID']:
                item_ids.add(number(row['ItemID']))
        tables['item'] = rows('item', item_ids)
        title_ids = {number(row['TitleID']) for row in tables['item'] if row['TitleID']}
        tables['title'] = rows('title', title_ids)
        for name in ('creator', 'subject', 'titleidentifier'):
            tables[name] = rows(name, title_ids)
        for name in ('partcreator', 'partidentifier'):
            tables[name] = rows(name, part_ids)
        creator_ids = {number(row['CreatorID']) for row in tables['creator'] + tables['partcreator'] if row['CreatorID']}
        tables['creatoridentifier'] = rows('creatoridentifier', creator_ids)
        tables['pagename'] = rows('pagename', page_ids)
        related = dict(title=title_ids, item=item_ids, page=page_ids, part=part_ids)
        tables['doi'] = [row for row in rows('doi', set().union(*related.values()))
                         if number(row['EntityID']) in related.get(row['EntityType'].lower(), set())]
        return dict(version=VERSION, snapshot_id=self.snapshot_id, keys=keys,
                    source_tables={name: dict(key=f'data/{name}.txt.gz', sha256=value['sha256'], rows=value['rows'])
                                   for name, value in self.sources.items()}, tables=tables)

    def write(self, keys, path, snapshot_id=None):
        path = Path(path)
        payload = self.select(keys)
        if snapshot_id is not None:
            if not snapshot_id.strip():
                raise ValueError('snapshot_id must be nonempty')
            payload['snapshot_id'] = snapshot_id
        path.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive publication; never replace an existing sidecar.
        descriptor, temporary = tempfile.mkstemp(prefix='.bhl-metadata-', dir=path.parent)
        try:
            with os.fdopen(descriptor, 'w') as output:
                output.write(compact(payload)+'\n')
                output.flush()
                os.fsync(output.fileno())
            os.link(temporary, path)
        finally:
            os.unlink(temporary)
        return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    create = commands.add_parser('index')
    create.add_argument('directory', type=Path)
    create.add_argument('index', type=Path)
    create.add_argument('--snapshot-id', required=True)
    export = commands.add_parser('export')
    export.add_argument('index', type=Path)
    export.add_argument('keys', type=Path, help='JSON array of selected bucket-relative OCR keys')
    export.add_argument('output', type=Path)
    export.add_argument('--snapshot-id', help='combined frozen OCR-and-metadata snapshot identity')
    args = parser.parse_args()
    if args.command == 'index':
        build(args.directory, args.index, args.snapshot_id)
    else:
        index = MetadataIndex(args.index)
        try:
            index.write(json.loads(args.keys.read_text()), args.output, snapshot_id=args.snapshot_id)
        finally:
            index.close()


if __name__ == '__main__':
    main()
