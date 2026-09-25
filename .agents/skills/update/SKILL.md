---
name: update
description: Update this valkey instrumentation package to the next upstream opentelemetry-python-contrib release. Ports the changes from upstream instrumentation/opentelemetry-instrumentation-redis with a redis->valkey translation, runs the tests, and commits on an update/<new-version> branch.
---

# Update

Port the newest upstream changes from `opentelemetry-python-contrib`'s redis
instrumentation into this valkey package.

Upstream repo: `https://github.com/open-telemetry/opentelemetry-python-contrib`
Upstream package path: `instrumentation/opentelemetry-instrumentation-redis`

File mapping (1:1):

| Upstream | Local |
|---|---|
| `src/opentelemetry/instrumentation/redis/*.py` | `src/opentelemetry/instrumentation/valkey/*.py` |
| `tests/test_redis.py` | `tests/test_valkey.py` |
| `pyproject.toml`, `test-requirements.txt`, README, CHANGELOG | not ported (local pyproject is bumped directly) |

## Workflow

### 1. Determine the versions

- `OLD` = `version` in `pyproject.toml` (e.g. `0.64b0`).
- `NEW` = the lowest upstream tag strictly greater than `OLD`:

```bash
git ls-remote --tags https://github.com/open-telemetry/opentelemetry-python-contrib \
  | sed -n 's|.*refs/tags/v\([0-9][0-9]*\.[0-9][0-9]*b[0-9][0-9]*\)$|\1|p' \
  | { echo "$OLD"; cat; } | sort -V -u \
  | awk -v c="$OLD" 'seen {print} $0 == c {seen = 1}'
```

This lists all candidate tags in order. Take the first one as `NEW`. If more
than one exists, tell the user which newer tags you are skipping (this repo
historically updates one release at a time). If none exists, stop: already at
the latest.

### 2. Run the port script

From the repo root (working tree must be clean):

```bash
bash .agents/skills/update/scripts/port-upstream.sh "$NEW"
```

The script clones upstream into a temp dir (blobless clone, removed on exit),
diffs `v$OLD..v$NEW` for the redis src + tests, rewrites paths, applies the
redis->valkey translation (see rules below), applies the result with
`git apply --reject`, and bumps the versions in `pyproject.toml` and
`version.py`. It prints the transformed diff at `/tmp/valkey-port-$NEW.diff`
and whether it applied cleanly or produced `.rej` files.

### 3. Review the result

- `git status` / `git diff` — sanity-check the translation.
- If there are `.rej` files, fix each one manually by hand-applying the
  corresponding hunk from `/tmp/valkey-port-$NEW.diff` with the translation
  rules applied, then delete the `.rej` file. Rejects are expected when an
  upstream hunk touches the copyright header (local files carry an extra
  `# Copyright 2025 Philip Meier` line) or the `valkey >= 6.1.0` pin in
  `package.py`.
- If the translation produced something suspicious that the rules don't
  cover, fix it and consider adding a new protection rule to the script.

### 4. Run the tests

```bash
uv lock && uv run pytest -q
```

Must be green before committing. If the lock fails because the new
`opentelemetry-instrumentation==$NEW` etc. are not on PyPI yet, the upstream
release may not be published; stop and tell the user.

### 5. Branch and commit

```bash
git checkout -b "update/$NEW"
git add -A
git commit -m "update to $NEW"
```

Do not push or open a PR unless the user asks.

## Translation rules (what the script automates)

Base translation: `redis` -> `valkey`, `Redis` -> `Valkey`, `REDIS` -> `VALKEY`
everywhere in the diff (paths first, then content).

PROTECTED TOKENS — reverted after the base translation because they refer to
real external symbols or established local conventions:

| Sed result | Reverted to | Why |
|---|---|---|
| `_VALKEY_ASYNCIO_VERSION`, `_VALKEY_CLUSTER_VERSION`, `_VALKEY_ASYNCIO_CLUSTER_VERSION` | `_REDIS_*` | internal constants kept as-is locally |
| `_set_db_valkey_database_index` | `_set_db_redis_database_index` | real function in `opentelemetry.instrumentation._semconv` |
| `DbSystemValues.VALKEY` | `DbSystemValues.REDIS` | real semconv enum member |
| `DB_VALKEY_DATABASE_INDEX` | `DB_REDIS_DATABASE_INDEX` | real semconv constant |
| `fakevalkey` | `fakeredis` | PyPI library name |
| `from valkey.exceptions import ConnectionError as redis_ConnectionError` | `from redis.exceptions import ...` | fakeredis raises redis exceptions |
| `fakeredis.aiovalkey import FakeValkey` / `fakeredis.aiovalkey.FakeValkey` / bare `FakeValkey` | `fakeredis import FakeAsyncValkey` / `FakeAsyncValkey` | fakeredis async class name |

Local deviations that are NOT automated (manual review if a hunk touches them):

- `# Copyright 2025 Philip Meier` header line in every local file
- `_instruments = ("valkey >= 6.1.0",)` in `package.py` (upstream pins `redis >= 2.6`)
- `fakeredis.FakeServer(server_type="valkey")` in tests
- pyproject entry point name stays `redis`
