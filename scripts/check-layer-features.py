#!/usr/bin/env python3
"""Check layer feature compilation and normal dependency isolation."""
import argparse
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--dependencies-only", action="store_true", help="check dependency graphs without compiling")
args = parser.parse_args()


def cargo(command, package, features=(), *extra):
    args = ["cargo", command, "-p", package, "--no-default-features", "--locked"]
    if features:
        args += ["--features", ",".join(features)]
    return args + list(extra)


def check(package, features=(), targets="--lib"):
    if args.dependencies_only:
        return
    print(f"Checking {package}: {','.join(features) or 'no features'} ({targets})", flush=True)
    subprocess.run(cargo("check", package, features, targets), check=True)


def dependency_names(package, features=()):
    output = subprocess.check_output(
        cargo("tree", package, features, "--edges", "normal,build", "--prefix", "none", "--format", "{p}"),
        text=True,
    )
    return {line.split()[0] for line in output.splitlines() if line.strip()}


def assert_dependencies(package, features=(), *, datafusion=False, sql=False):
    names = dependency_names(package, features)
    has_datafusion = any(name.startswith("datafusion") for name in names)
    assert has_datafusion == datafusion, (package, features, "DataFusion", has_datafusion)
    has_sql = "datafusion-sql" in names or "sqlparser" in names
    assert has_sql == sql, (package, features, "SQL parser", has_sql)


layers = "open-arrow-access-layers"
facade = "open-arrow-access"
metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"], text=True
))
features = next(p for p in metadata["packages"] if p["name"] == layers)["features"]["all"]
check(layers, targets="--all-targets")
assert_dependencies(layers)
assert_dependencies(facade)
for feature in features:
    heavy = feature in {"filter", "schema-evolution"}
    sql = feature == "schema-evolution"
    check(layers, [feature], "--all-targets")
    assert_dependencies(layers, [feature], datafusion=heavy, sql=sql)
    assert_dependencies(facade, ["layer-" + feature], datafusion=heavy, sql=sql)
check(facade, targets="--all-targets")
for feature in features:
    check(facade, ["layer-" + feature])
for combination in [("crawl",), ("format-warc",), ("source-positions",)]:
    check(facade, combination, "--all-targets")
    assert_dependencies(facade, combination)
check(facade, ["crawl", "layer-schema-evolution"], "--all-targets")
check(facade, ["service-semantic-scholar"])
assert_dependencies(facade, ["service-semantic-scholar"], datafusion=True, sql=True)
check(layers, ["all"], "--all-targets")
check(facade, ["layers-all"], "--all-targets")
print("Layer dependency isolation checks passed." if args.dependencies_only
      else "All layer feature and dependency isolation checks passed.")
