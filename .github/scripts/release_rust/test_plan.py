import tempfile
import unittest
from pathlib import Path
from plan import plan


def write(root, relative, text):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def fixture(root):
    write(root, 'Cargo.toml', '''[package]
name = "facade"
version.workspace = true
[workspace]
members = ["core", "adapter", "test-support"]
[workspace.package]
version = "0.1.0"
[workspace.dependencies]
core = { path = "core", version = "0.1.0" }
adapter = { path = "adapter", version = "0.1.0" }
[dependencies]
adapter.workspace = true
''')
    write(root, 'core/Cargo.toml', '''[package]
name = "core"
version.workspace = true
[dev-dependencies]
adapter.workspace = true
''')
    write(root, 'adapter/Cargo.toml', '''[package]
name = "adapter"
version.workspace = true
[target.'cfg(unix)'.build-dependencies]
core.workspace = true
''')
    write(root, 'test-support/Cargo.toml', '''[package]
name = "test-support"
version.workspace = true
publish = false
''')


class PlannerTests(unittest.TestCase):
    def test_inherited_target_dependencies_and_dev_cycles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            self.assertEqual(plan(root, 'v0.1.0'), ['core', 'adapter', '.'])

    def test_tag_must_match_workspace_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            for tag in ['main', 'v0.1.0-rc.1', 'v0.2.0']:
                with self.assertRaisesRegex(RuntimeError, 'tag'):
                    plan(root, tag)

    def test_requires_registry_version_for_path_dependency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            p = root / 'Cargo.toml'
            p.write_text(p.read_text().replace('path = "core", version = "0.1.0"', 'path = "core"'))
            with self.assertRaisesRegex(RuntimeError, 'registry version'):
                plan(root)

    def test_runtime_dependency_on_private_crate_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            p = root / 'core/Cargo.toml'
            p.write_text(p.read_text().replace('[dev-dependencies]', '[dependencies]'))
            with self.assertRaisesRegex(RuntimeError, 'cycle'):
                plan(root)
            p.write_text(p.read_text().replace('adapter.workspace = true', 'private = { path = "../test-support", version = "0.1.0" }'))
            with self.assertRaisesRegex(RuntimeError, 'unpublished'):
                plan(root)

    def test_versions_must_stay_in_lockstep(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            p = root / 'core/Cargo.toml'
            p.write_text(p.read_text().replace('version.workspace = true', 'version = "0.2.0"'))
            with self.assertRaisesRegex(RuntimeError, 'version 0.1.0'):
                plan(root)
