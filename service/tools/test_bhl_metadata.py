import contextlib
import gzip
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import bhl_metadata as bhl


class MetadataTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.index = self.root/'index.sqlite'
        self.rows = {p['name']: [] for p in bhl.PROFILE}

    def add(self, table, **values):
        p = next(p for p in bhl.PROFILE if p['name'] == table)
        self.rows[table].append({c: values.get(c, '') for c in p['columns']})

    def build(self):
        for p in bhl.PROFILE:
            with gzip.open(self.root/(p['name']+'.txt.gz'), 'wt', newline='', encoding='utf8') as f:
                f.write('\t'.join(p['columns'])+'\n')
                for row in self.rows[p['name']]:
                    f.write('\t'.join(row[c] for c in p['columns'])+'\n')
        with contextlib.redirect_stdout(io.StringIO()):
            bhl.build(self.root, self.index, 'test-snapshot')

    def test_all_fields_quotes_duplicates_source_order_and_part_scope(self):
        self.add('page', PageID='003', ItemID='01', SequenceOrder='2')
        self.add('part', PartID='8', ItemID='1')
        self.add('partpage', PartID='8', PageID='3', ItemID='1', SequenceOrder='1')
        self.add('item', ItemID='1', TitleID='9', RightsStatement='"all rights retained"')
        self.add('title', TitleID='9')
        self.add('creator', TitleID='9', CreatorID='2', CreatorName='é')
        self.add('partcreator', PartID='8', CreatorID='4')
        for creator in ('4', '2', '4'):
            self.add('creatoridentifier', CreatorID=creator, IdentifierValue='duplicate')
        self.add('doi', EntityType='Part', EntityID='8', DOI='part-doi')
        self.add('doi', EntityType='Title', EntityID='8', DOI='wrong-type')
        self.add('pagename', PageID='3', NameConfirmed='name')
        self.add('subject', TitleID='9', Subject='subject')
        self.add('titleidentifier', TitleID='9')
        self.add('partidentifier', PartID='8')
        self.build()
        index = bhl.MetadataIndex(self.index)
        self.addCleanup(index.close)
        result = index.select(['ocr/part-8/part-8-3-0001.txt'])
        self.assertEqual(len(result['tables']), 13)
        self.assertEqual(result['tables']['item'][0]['RightsStatement'], '"all rights retained"')
        self.assertEqual([r['CreatorID'] for r in result['tables']['creatoridentifier']], ['4','2','4'])
        self.assertEqual([r['DOI'] for r in result['tables']['doi']], ['part-doi'])
        self.assertEqual(result['tables']['page'][0]['PageID'], '003')
        self.assertEqual(result['tables']['item'][0]['ItemID'], '1')
        self.assertEqual(len(result['source_tables']), 13)

    def test_duplicate_single_row_id_rolls_back_table(self):
        self.add('title', TitleID='1')
        self.add('title', TitleID='01')
        with self.assertRaises(sqlite3.IntegrityError):
            self.build()
        with sqlite3.connect(self.index) as db:
            self.assertIsNone(db.execute("SELECT value FROM sources WHERE name='title'").fetchone())

    def test_incomplete_index_rejected_and_missing_join_preserved(self):
        self.build()
        index = bhl.MetadataIndex(self.index)
        result = index.select(['ocr/item-1/item-1-3-0001.txt'])
        self.assertTrue(all(not rows for rows in result['tables'].values()))
        index.close()
        with sqlite3.connect(self.index) as db:
            db.execute("DELETE FROM sources WHERE name='page'")
        with self.assertRaisesRegex(ValueError, '13'):
            bhl.MetadataIndex(self.index)

    def test_malformed_header_and_invalid_object_identity(self):
        self.build()
        for key in ('ocr/item-1/part-1-3-1.txt', 'ocr/item-1/item-2-3-1.txt',
                    'ocr/item-1/item-1-0-1.txt', 'ocr/item-1/../item-1-3-1.txt'):
            with self.assertRaises(ValueError):
                bhl.identity(key)
        with gzip.open(self.root/'title.txt.gz','wt') as f:
            f.write('TitleID\tUnexpected\n')
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, 'header'):
                bhl.build(self.root, self.root/'new.sqlite', 'new')

    def test_resume_rejects_changed_source_and_malformed_rows(self):
        self.build()
        with gzip.open(self.root/'title.txt.gz', 'at') as file:
            file.write('not enough columns\n')
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, 'bytes changed'):
                bhl.build(self.root, self.index, 'test-snapshot')
            with self.assertRaisesRegex(ValueError, 'malformed TSV row'):
                bhl.build(self.root, self.root/'malformed.sqlite', 'test-snapshot')


if __name__ == '__main__':
    unittest.main()
