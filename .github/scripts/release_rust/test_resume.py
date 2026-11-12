import io
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from publish import is_published, parse_retry_after, prepare_manifest, publish_package, restore, wait_for_retry


class Response(io.BytesIO):
    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.close()


class ResumeTests(unittest.TestCase):
    def test_published_version_is_skipped_before_token_or_upload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'Cargo.toml').write_text('[package]\nname="already-done"\nversion="0.1.0"\n')
            with mock.patch('publish.is_published', return_value=True), mock.patch('publish.temporary_trusted_publishing_token') as token, mock.patch('publish.subprocess.run') as upload:
                publish_package(root, '.', False)
            token.assert_not_called()
            upload.assert_not_called()

    def test_sparse_index_rejects_yanked_version(self):
        with mock.patch('publish.urllib.request.urlopen', return_value=Response(b'{"vers":"0.1.0","yanked":true}\n')):
            with self.assertRaisesRegex(RuntimeError, 'yanked'):
                is_published('already-done', '0.1.0')

    def test_retry_time_accepts_real_crates_io_error_suffix(self):
        future = datetime.now(timezone.utc) + timedelta(minutes=3)
        message = 'Please try again after ' + future.strftime('%a, %d %b %Y %H:%M:%S GMT') + ' and see https://crates.io/docs/rate-limits for more details.'
        self.assertGreater(parse_retry_after(message), 170)
        self.assertLess(parse_retry_after(message), 190)

    def test_waits_are_interruptible_and_at_most_sixty_seconds(self):
        with mock.patch('publish.time.sleep') as sleep:
            wait_for_retry(125)
        self.assertEqual(sleep.call_args_list, [mock.call(60), mock.call(60), mock.call(5)])

    def test_inherited_dev_dependencies_and_lockfile_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / 'crate'
            package.mkdir()
            (root / 'other').mkdir()
            (root / 'Cargo.toml').write_text('[workspace.dependencies]\nother = { path = "other", version = "0.1.0" }\n')
            manifest = package / 'Cargo.toml'
            original = '[package]\nname="crate"\nversion="0.1.0"\n[dev-dependencies]\nother.workspace = true\n'
            manifest.write_text(original)
            lock = root / 'Cargo.lock'
            lock.write_text('original')
            patch = prepare_manifest(root, package)
            self.assertNotIn('other.workspace', manifest.read_text())
            lock.write_text('changed')
            restore(patch)
            self.assertEqual(manifest.read_text(), original)
            self.assertEqual(lock.read_text(), 'original')

    def test_failed_upload_restores_manifest_and_lockfile(self):
        from contextlib import contextmanager
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / 'crate'
            package.mkdir()
            (root / 'other').mkdir()
            (root / 'Cargo.toml').write_text('[workspace.dependencies]\nother = { path = "other", version = "0.1.0" }\n')
            original = '[package]\nname="crate"\nversion="0.1.0"\n[dev-dependencies]\nother.workspace = true\n'
            manifest = package / 'Cargo.toml'
            manifest.write_text(original)
            lock = root / 'Cargo.lock'
            lock.write_text('original')
            released = []
            @contextmanager
            def token():
                try:
                    yield 'temporary'
                finally:
                    released.append(True)
            def upload(*args, **kwargs):
                self.assertNotIn('other.workspace', manifest.read_text())
                lock.write_text('changed by cargo')
                return mock.Mock(returncode=101, stdout='permission denied')
            with mock.patch('publish.is_published', return_value=False), mock.patch('publish.temporary_trusted_publishing_token', token), mock.patch('publish.subprocess.run', upload):
                with self.assertRaises(subprocess.CalledProcessError):
                    publish_package(root, 'crate', False)
            self.assertEqual(released, [True])
            self.assertEqual(manifest.read_text(), original)
            self.assertEqual(lock.read_text(), 'original')
