# tinybird-sdk (Python)

Python SDK for Tinybird with a TypeScript-SDK-like surface.

It includes:

- A low-level API wrapper (`TinybirdApi` / `createTinybirdApi`)
- A high-level runtime client (`TinybirdClient` / `createClient`)
- Schema DSL (`t`, `p`, `engine`, `defineDatasource`, `definePipe`, `defineProject`)
- Resource generation helpers (`generateResources`, `build`, `buildFromInclude`)
- API helpers for branches, workspaces, tokens, regions, dashboard URLs, and local mode

## Requirements

- Python `>=3.11`
- [`uv`](https://docs.astral.sh/uv/) for environment and dependency management

## Setup with uv

From the repository root:

```bash
cd python
uv sync --dev
```

This creates `.venv` and installs the package in editable mode with dev dependencies.

## Development Commands

Run from `python/`:

```bash
# Run test suite
uv run pytest

# Quick syntax validation
uv run python -m compileall -q src

# Build wheel/sdist
uv build
```

## Quick Start

### 1) Low-level API wrapper

Use this when you want direct HTTP-like operations.

```python
from tinybird_sdk import createTinybirdApi

api = createTinybirdApi({
    "baseUrl": "https://api.tinybird.co",
    "token": "p.your_token",
})

# Query endpoint pipe
result = api.query("top_pages", {"limit": 10})

# Ingest event
api.ingest("events", {
    "timestamp": "2026-01-01T00:00:00Z",
    "event_name": "page_view",
})

# Execute SQL
rows = api.sql("SELECT 1 AS value")
```

### 2) High-level runtime client

Use this when you want a more SDK-oriented client with datasource namespace operations.

```python
from tinybird_sdk import createClient

client = createClient({
    "baseUrl": "https://api.tinybird.co",
    "token": "p.your_token",
    "devMode": False,
})

client.query("top_pages", {"limit": 5})
client.ingest("events", {"event_name": "click"})

client.datasources.append("events", {
    "url": "https://example.com/events.csv"
})

client.datasources.delete("events", {
    "deleteCondition": "event_name = 'test'",
    "dryRun": True,
})

context = client.getContext()
```

### 3) Schema DSL + project client

Use this to define datasources/pipes as code and expose a structured client.

```python
from tinybird_sdk import (
    t,
    p,
    node,
    engine,
    defineDatasource,
    defineEndpoint,
    createTinybirdClient,
)

page_views = defineDatasource("page_views", {
    "schema": {
        "timestamp": t.dateTime(),
        "pathname": t.string(),
        "country": t.string().nullable().lowCardinality(),
    },
    "engine": engine.mergeTree({
        "sortingKey": ["pathname", "timestamp"],
    }),
})

top_pages = defineEndpoint("top_pages", {
    "params": {
        "limit": p.int32().optional(10),
    },
    "nodes": [
        node({
            "name": "endpoint",
            "sql": """
                SELECT pathname, count() AS views
                FROM page_views
                GROUP BY pathname
                ORDER BY views DESC
                LIMIT {{Int32(limit, 10)}}
            """,
        })
    ],
    "output": {
        "pathname": t.string(),
        "views": t.uint64(),
    },
})

tinybird = createTinybirdClient({
    "datasources": {"page_views": page_views},
    "pipes": {"top_pages": top_pages},
})
```

## SDK API Shape

### Top-level imports

```python
from tinybird_sdk import (
    # Runtime clients
    TinybirdClient,
    createClient,
    TinybirdApi,
    createTinybirdApi,

    # Schema DSL
    t,
    p,
    engine,
    defineDatasource,
    definePipe,
    defineEndpoint,
    defineMaterializedView,
    defineCopyPipe,
    defineProject,
    createTinybirdClient,
    node,
    sql,

    # Tokens
    createJWT,
)
```

### Common runtime methods

`TinybirdApi`:

- `query(endpointName, params=None, options=None)`
- `ingest(datasourceName, event, options=None)`
- `ingestBatch(datasourceName, events, options=None)`
- `sql(sql, options=None)`
- `appendDatasource(datasourceName, options, apiOptions=None)`
- `deleteDatasource(datasourceName, options, apiOptions=None)`
- `truncateDatasource(datasourceName, options=None, apiOptions=None)`
- `createToken(body, options=None)`

`TinybirdClient`:

- `query(pipeName, params=None, options=None)`
- `ingest(datasourceName, event, options=None)`
- `ingestBatch(datasourceName, events, options=None)`
- `sql(sql, options=None)`
- `getContext()`
- `datasources.ingest/append/replace/delete/truncate(...)`
- `tokens.createJWT(...)`

## Generation and Build Workflows

Generate Tinybird resource content from Python project definitions:

```python
from tinybird_sdk import defineProject, generateResources

project = defineProject({
    "datasources": {"page_views": page_views},
    "pipes": {"top_pages": top_pages},
})

resources = generateResources(project)

for ds in resources.datasources:
    print(ds.name, ds.content)
```

Build from a Python schema module file:

```python
from tinybird_sdk.generator import build

result = build({
    "schemaPath": "src/tinybird_schema.py",  # must export `project` or `tinybird_project`
})

print(result.stats)
```

Deploy generated resources using API layer helpers:

```python
from tinybird_sdk.api.build import buildToTinybird

response = buildToTinybird(
    {"baseUrl": "https://api.tinybird.co", "token": "p.your_token"},
    resources,
)
```

## Token and Preview Behavior

Preview token resolution supports these environment conventions:

- `TINYBIRD_BRANCH_TOKEN` (highest priority)
- `TINYBIRD_TOKEN`
- CI/preview branch env detection (`VERCEL_*`, `GITHUB_*`, `CI_*`, etc.)

Helpers:

- `isPreviewEnvironment()`
- `getPreviewBranchName()`
- `resolveToken(...)`
- `clearTokenCache()`

## Error Types

Main SDK errors:

- `TinybirdApiError`: HTTP/API-level errors from low-level wrapper
- `TinybirdError`: high-level client errors
- `TokenApiError`, `BranchApiError`, `WorkspaceApiError`, `ResourceApiError`, `RegionsApiError`, `LocalApiError`

## Project Layout

```text
python/
  src/tinybird_sdk/
    api/
    client/
    schema/
    generator/
    codegen/
    infer/
    cli/
  tests/
```

## Status

This package is parity-oriented and actively evolving. API details may still be refined as more TS test contracts are ported.
