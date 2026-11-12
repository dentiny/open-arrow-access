#!/usr/bin/env python3
# Adapted from Apache OpenDAL's Rust release planner at
# ac07ab094903b8515e882d654e4217ad5dced1a2.
# Licensed under Apache-2.0; see LICENSE and NOTICE in this directory.

import argparse
import heapq
import json
import os
import re
import tomllib
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[3]


def load(path):
    with path.open('rb') as stream:
        return tomllib.load(stream)


def version(manifest, workspace):
    value = manifest['package']['version']
    return workspace['workspace']['package']['version'] if isinstance(value, dict) else value


def dependency_tables(manifest, kinds=('dependencies', 'build-dependencies')):
    for kind in kinds:
        yield manifest.get(kind, {})
    for target in manifest.get('target', {}).values():
        for kind in kinds:
            yield target.get(kind, {})


def local_dependencies(manifest, manifest_dir, project_dir, workspace, kinds=('dependencies', 'build-dependencies')):
    for table in dependency_tables(manifest, kinds):
        for name, dependency in table.items():
            if not isinstance(dependency, dict):
                continue
            base = manifest_dir
            if dependency.get('workspace'):
                dependency = workspace.get('workspace', {}).get('dependencies', {}).get(name, {})
                base = project_dir
            if isinstance(dependency, dict) and 'path' in dependency:
                yield name, (base / dependency['path']).resolve(), dependency


def manifests(project_dir, workspace):
    paths = {project_dir / 'Cargo.toml'}
    for member in workspace.get('workspace', {}).get('members', []):
        paths.update(path / 'Cargo.toml' for path in project_dir.glob(member))
    excluded = set()
    for pattern in workspace.get('workspace', {}).get('exclude', []):
        excluded.update(path / 'Cargo.toml' for path in project_dir.glob(pattern))
    return sorted(path for path in paths - excluded if path.is_file())


def plan(project_dir=PROJECT_DIR, tag=None):
    project_dir = project_dir.resolve()
    workspace = load(project_dir / 'Cargo.toml')
    expected = workspace.get('workspace', {}).get('package', {}).get('version')
    if tag is not None and (not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', tag) or tag != f'v{expected}'):
        raise RuntimeError(f'tag {tag!r} does not match workspace version v{expected}')
    packages = {}
    names = set()
    for path in manifests(project_dir, workspace):
        manifest = load(path)
        package = manifest.get('package', {})
        if not package or package.get('publish') is False or package.get('publish') == []:
            continue
        if expected and version(manifest, workspace) != expected:
            raise RuntimeError(f'{path}: all released crates must use version {expected}')
        name = package['name']
        if name in names:
            raise RuntimeError(f'duplicate package name: {name}')
        names.add(name)
        packages[path.parent.resolve()] = manifest
    dependencies = {}
    for directory, manifest in packages.items():
        edges = set()
        for name, dependency, spec in local_dependencies(manifest, directory, project_dir, workspace):
            if dependency not in packages:
                raise RuntimeError(f'{manifest["package"]["name"]} requires unpublished local dependency {name}')
            if not spec.get('version'):
                raise RuntimeError(f'local dependency {name} requires a registry version')
            if expected and spec['version'] != expected:
                raise RuntimeError(f'local dependency {name} must use workspace version {expected}')
            edges.add(dependency)
        dependencies[directory] = edges
    ready = [str(path.relative_to(project_dir)) for path, edges in dependencies.items() if not edges]
    heapq.heapify(ready)
    ordered = []
    while ready:
        item = heapq.heappop(ready)
        ordered.append(item)
        path = (project_dir / item).resolve()
        for dependent, edges in dependencies.items():
            if path in edges:
                edges.remove(path)
                if not edges:
                    heapq.heappush(ready, str(dependent.relative_to(project_dir)))
    if len(ordered) != len(packages):
        raise RuntimeError('cycle in runtime/build dependencies; development dependencies are excluded')
    return ordered


def main():
    parser = argparse.ArgumentParser(description='Plan the unified Rust workspace release.')
    parser.add_argument('--project-dir', type=Path, default=PROJECT_DIR)
    parser.add_argument('--tag')
    parser.add_argument('--github-output', action='store_true')
    args = parser.parse_args()
    packages = json.dumps(plan(args.project_dir, args.tag))
    print(packages)
    if args.github_output:
        with Path(os.environ['GITHUB_OUTPUT']).open('a') as stream:
            stream.write(f'packages={packages}\n')


if __name__ == '__main__':
    main()
