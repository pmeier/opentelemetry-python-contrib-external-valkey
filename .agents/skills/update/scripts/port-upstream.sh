#!/usr/bin/env bash
# Port upstream opentelemetry-python-contrib redis instrumentation changes into the
# local valkey package.
#
# Usage: port-upstream.sh [new-version]     (run from the valkey repo root)
#   new-version: e.g. 0.65b0. If omitted, the next upstream tag after the
#   version in pyproject.toml is auto-detected.
#
# What it does:
#   1. Shallow-clones upstream (blobless) into a temp dir (removed on exit).
#   2. Diffs upstream redis src + tests between the old and new tag.
#      (Upstream pyproject/version.py are excluded; versions are bumped directly.)
#   3. Rewrites paths and translates redis -> valkey, protecting tokens that
#      refer to real external symbols (see PROTECTED TOKENS in SKILL.md).
#   4. Applies the diff with `git apply --reject` (rejects need manual review).
#   5. Bumps versions in pyproject.toml and version.py.
#
# Prints the transformed diff path and whether rejects occurred.
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }

command -v uv >/dev/null || die "uv not found"

REPO=$(git rev-parse --show-toplevel 2>/dev/null) || die "not inside a git repo"
cd "$REPO"
git diff --quiet && git diff --cached --quiet || die "working tree is not clean; commit or stash first"

UPSTREAM_URL=https://github.com/open-telemetry/opentelemetry-python-contrib
UP=instrumentation/opentelemetry-instrumentation-redis

OLD=$(sed -n 's/^version = "\([^"]*\)"/\1/p' pyproject.toml | head -1)
[ -n "$OLD" ] || die "could not read version from pyproject.toml"

if [ $# -ge 1 ]; then
  NEW=$1
else
  NEW=$(git ls-remote --tags "$UPSTREAM_URL" \
    | sed -n 's|.*refs/tags/v\([0-9][0-9]*\.[0-9][0-9]*b[0-9][0-9]*\)$|\1|p' \
    | { echo "$OLD"; cat; } | sort -V -u \
    | awk -v c="$OLD" 'seen {print} $0 == c {seen = 1}' \
    | head -1)
  [ -n "$NEW" ] || die "no upstream tag newer than $OLD found"
fi

echo "Porting upstream v$OLD -> v$NEW"

CLONE=$(mktemp -d /tmp/otel-contrib.XXXXXX)
trap 'rm -rf "$CLONE"' EXIT
git clone --quiet --filter=blob:none --no-checkout "$UPSTREAM_URL" "$CLONE"

DIFF=/tmp/valkey-port-$NEW.diff
git -C "$CLONE" diff "v$OLD..v$NEW" -- \
  "$UP/src/opentelemetry/instrumentation/redis" \
  ":(exclude)$UP/src/opentelemetry/instrumentation/redis/version.py" \
  "$UP/tests/test_redis.py" \
| sed \
    -e "s|$UP/src/opentelemetry/instrumentation/redis/|src/opentelemetry/instrumentation/valkey/|g" \
    -e "s|$UP/tests/test_redis.py|tests/test_valkey.py|g" \
    -e 's/redis/valkey/g' -e 's/Redis/Valkey/g' -e 's/REDIS/VALKEY/g' \
    -e 's/_VALKEY_ASYNCIO_CLUSTER_VERSION/_REDIS_ASYNCIO_CLUSTER_VERSION/g' \
    -e 's/_VALKEY_ASYNCIO_VERSION/_REDIS_ASYNCIO_VERSION/g' \
    -e 's/_VALKEY_CLUSTER_VERSION/_REDIS_CLUSTER_VERSION/g' \
    -e 's/_set_db_valkey_database_index/_set_db_redis_database_index/g' \
    -e 's/DbSystemValues\.VALKEY/DbSystemValues.REDIS/g' \
    -e 's/DB_VALKEY_DATABASE_INDEX/DB_REDIS_DATABASE_INDEX/g' \
    -e 's/valkey_ConnectionError/redis_ConnectionError/g' \
    -e 's|from valkey\.exceptions import ConnectionError as redis_ConnectionError|from redis.exceptions import ConnectionError as redis_ConnectionError|g' \
    -e 's/fakevalkey/fakeredis/g' \
    -e 's|fakeredis\.aiovalkey import FakeValkey|fakeredis import FakeAsyncValkey|g' \
    -e 's|fakeredis\.aiovalkey\.FakeValkey|FakeAsyncValkey|g' \
    -e 's/FakeValkey/FakeAsyncValkey/g' \
  > "$DIFF"

if [ ! -s "$DIFF" ]; then
  echo "No upstream changes between v$OLD and v$NEW for the redis package."
  exit 0
fi

if git apply --reject "$DIFF"; then
  echo "APPLIED CLEANLY"
else
  echo "APPLIED WITH REJECTS - .rej files need manual review"
fi
echo "Transformed diff: $DIFF"

# Version bumps (done directly; upstream version.py/pyproject diffs are excluded).
sed -i \
  -e "s/^version = \"$OLD\"\$/version = \"$NEW\"/" \
  -e "s/==$OLD\"/==$NEW\"/g" \
  pyproject.toml
sed -i "s/__version__ = \"$OLD\"/__version__ = \"$NEW\"/" \
  src/opentelemetry/instrumentation/valkey/version.py

echo "Bumped pyproject.toml and version.py: $OLD -> $NEW"
