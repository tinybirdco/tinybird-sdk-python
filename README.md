# tinybird-sdk (Python)

> **Note:** This package is experimental. APIs may change between versions.

A Python SDK for defining Tinybird resources with a TypeScript-SDK-like workflow.
Define your datasources, pipes, and queries in Python and sync them directly to Tinybird.

## Installation

```bash
pip install tinybird-sdk
```

## Requirements

- Python `>=3.11`
- Server-side usage only (do not expose Tinybird credentials in browser code)

## Quick Start

### 1. Initialize your project

```bash
tinybird init
```

This creates:
- `tinybird.config.json` - Configuration file
- `lib/datasources.py` - Define your datasources
- `lib/pipes.py` - Define your pipes/endpoints
- `lib/client.py` - Your Tinybird client module

### 2. Configure your token

Create a `.env.local` file:

```env
TINYBIRD_TOKEN=p.your_token_here
```

### 3. Define your datasources

```python
# lib/datasources.py
from tinybird_sdk import define_datasource, t, engine

page_views = define_datasource(
    "page_views",
    {
        "description": "Page view tracking data",
        "schema": {
            "timestamp": t.date_time(),
            "pathname": t.string(),
            "session_id": t.string(),
            "country": t.string().low_cardinality().nullable(),
        },
        "engine": engine.merge_tree(
            {
                "sorting_key": ["pathname", "timestamp"],
            }
        ),
    },
)
```

### 4. Define your endpoints

```python
# lib/pipes.py
from tinybird_sdk import define_endpoint, node, p, t

top_pages = define_endpoint(
    "top_pages",
    {
        "description": "Get the most visited pages",
        "params": {
            "start_date": p.date_time(),
            "end_date": p.date_time(),
            "limit": p.int32().optional(10),
        },
        "nodes": [
            node(
                {
                    "name": "aggregated",
                    "sql": """
                        SELECT pathname, count() AS views
                        FROM page_views
                        WHERE timestamp >= {{DateTime(start_date)}}
                          AND timestamp <= {{DateTime(end_date)}}
                        GROUP BY pathname
                        ORDER BY views DESC
                        LIMIT {{Int32(limit, 10)}}
                    """,
                }
            )
        ],
        "output": {
            "pathname": t.string(),
            "views": t.uint64(),
        },
    },
)
```

### 5. Create your client

```python
# lib/client.py
from tinybird_sdk import Tinybird
from .datasources import page_views
from .pipes import top_pages

tinybird = Tinybird(
    {
        "datasources": {"page_views": page_views},
        "pipes": {"top_pages": top_pages},
    }
)

__all__ = ["tinybird", "page_views", "top_pages"]
```

### 6. Optional: use a stable local import path

In larger applications, keep a single module (for example `lib/client.py`) and import from there:

```python
from lib.client import tinybird
```

### 7. Start development

```bash
tinybird dev
```

This watches your schema files and syncs changes to Tinybird.

### 8. Use the client

```python
from lib.client import tinybird

# Ingest one row
tinybird.page_views.ingest(
    {
        "timestamp": "2024-01-15 10:30:00",
        "pathname": "/home",
        "session_id": "abc123",
        "country": "US",
    }
)

# Query endpoint
result = tinybird.top_pages.query(
    {
        "start_date": "2024-01-01 00:00:00",
        "end_date": "2024-01-31 23:59:59",
        "limit": 5,
    }
)
```

### 9. Manage datasource rows

```python
from lib.client import tinybird

# Datasource accessors support: ingest, append, replace, delete, truncate

tinybird.page_views.ingest(
    {
        "timestamp": "2024-01-15 10:30:00",
        "pathname": "/pricing",
        "session_id": "session_123",
        "country": "US",
    }
)

tinybird.page_views.append(
    {
        "url": "https://example.com/page_views.csv",
    }
)

tinybird.page_views.replace(
    {
        "url": "https://example.com/page_views_full_snapshot.csv",
    }
)

tinybird.page_views.delete(
    {
        "delete_condition": "country = 'XX'",
    }
)

tinybird.page_views.delete(
    {
        "delete_condition": "country = 'XX'",
        "dry_run": True,
    }
)

tinybird.page_views.truncate()
```

## Public Tinybird API (Optional)

If you want a low-level API wrapper decoupled from the high-level client layer,
use `create_tinybird_api()` directly with `base_url` and `token`:

```python
from tinybird_sdk import create_tinybird_api

api = create_tinybird_api(
    {
        "base_url": "https://api.tinybird.co",
        "token": "p.your_token",
    }
)

# Query endpoint pipe
top_pages = api.query(
    "top_pages",
    {
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "limit": 5,
    },
)

# Ingest one row
api.ingest(
    "events",
    {
        "timestamp": "2024-01-15 10:30:00",
        "event_name": "page_view",
        "pathname": "/home",
    },
)

# Ingest retry behavior (disabled by default):
# - 429 retries use Retry-After / X-RateLimit-Reset headers.
# - 503 retries use SDK default exponential backoff.
api.ingest(
    "events",
    {
        "timestamp": "2024-01-15 10:31:00",
        "event_name": "button_click",
        "pathname": "/pricing",
    },
    {
        "max_retries": 3,
    },
)

# Import rows from URL/file
api.append_datasource(
    "events",
    {
        "url": "https://example.com/events.csv",
    },
)

# Delete rows matching a SQL condition
api.delete_datasource(
    "events",
    {
        "delete_condition": "event_name = 'test'",
    },
)

# Delete dry run
api.delete_datasource(
    "events",
    {
        "delete_condition": "event_name = 'test'",
        "dry_run": True,
    },
)

# Truncate datasource
api.truncate_datasource("events")

# Execute raw SQL
sql_result = api.sql("SELECT count() AS total FROM events")

# Optional per-request token override
workspace_response = api.request_json(
    "/v1/workspace",
    token="p.branch_or_jwt_token",
)
```

This Tinybird API is standalone and can be used without `create_client()` or `Tinybird(...)`.

## JWT Token Creation

Create short-lived JWT tokens for secure scoped access to Tinybird resources.

```python
from datetime import datetime, timedelta, timezone

from tinybird_sdk import create_client

client = create_client(
    {
        "base_url": "https://api.tinybird.co",
        "token": "p.your_admin_token",
    }
)

result = client.tokens.create_jwt(
    {
        "name": "user_123_session",
        "expires_at": datetime.now(tz=timezone.utc) + timedelta(hours=1),
        "scopes": [
            {
                "type": "PIPES:READ",
                "resource": "user_dashboard",
                "fixed_params": {"user_id": 123},
            }
        ],
        "limits": {"rps": 10},
    }
)

jwt_token = result["token"]
```

### Scope Types

| Scope | Description |
|-------|-------------|
| `PIPES:READ` | Read access to a specific pipe endpoint |
| `DATASOURCES:READ` | Read access to a datasource (with optional `filter`) |
| `DATASOURCES:APPEND` | Append access to a datasource |

### Scope Options

- **`fixed_params`**: For pipes, embed parameters that cannot be overridden by the caller.
- **`filter`**: For datasources, append a SQL WHERE clause (for example, `"org_id = 'acme'"`).

## CLI Commands

This package installs `tinybird` as a runtime dependency.
`tinybird generate` is handled by this SDK; other commands are delegated to the Tinybird CLI.

### `tinybird init`

Initialize a new Tinybird project:

```bash
tinybird init
tinybird init --force
tinybird init --skip-login
```

### `tinybird migrate`

Migrate local Tinybird datafiles (`.datasource`, `.pipe`, `.connection`) into a Python definitions file.

```bash
tinybird migrate "tinybird/**/*.datasource" "tinybird/**/*.pipe" "tinybird/**/*.connection"
tinybird migrate tinybird/legacy --out ./tinybird.migration.py
tinybird migrate tinybird --dry-run
```

### `tinybird dev`

```bash
tinybird dev
tinybird dev --local
tinybird dev --branch
```

### `tinybird build`

```bash
tinybird build
tinybird build --dry-run
tinybird build --local
tinybird build --branch
```

### `tinybird deploy`

```bash
tinybird deploy
tinybird deploy --check
tinybird deploy --allow-destructive-operations
```

### `tinybird pull`

```bash
tinybird pull
tinybird pull --output-dir ./tinybird-datafiles
tinybird pull --force
```

### `tinybird login`

```bash
tinybird login
```

### `tinybird branch`

```bash
tinybird branch list
tinybird branch status
tinybird branch delete <name>
```

### `tinybird info`

```bash
tinybird info
tinybird info --json
```

## Configuration

Create a `tinybird.config.json` (or `tinybird.config.py` / `tinybird_config.py` for dynamic logic) in your project root:

```json
{
  "include": [
    "lib/*.py",
    "tinybird/**/*.datasource",
    "tinybird/**/*.pipe",
    "tinybird/**/*.connection"
  ],
  "token": "${TINYBIRD_TOKEN}",
  "base_url": "https://api.tinybird.co",
  "dev_mode": "branch"
}
```

You can mix Python files with raw `.datasource`, `.pipe`, and `.connection` files for incremental migration.
`include` supports glob patterns.

### Config File Formats

Supported config files (search order):

| File | Description |
|------|-------------|
| `tinybird.config.py` | Python config with dynamic logic |
| `tinybird_config.py` | Python config alias |
| `tinybird.config.json` | JSON config (default for new projects) |
| `tinybird.json` | Legacy JSON config |

For Python configs, export one of:
- `config` dict
- `CONFIG` dict
- `default` dict
- `get_config()` returning a dict

Example:

```python
# tinybird.config.py
config = {
    "include": ["lib/*.py"],
    "token": "${TINYBIRD_TOKEN}",
    "base_url": "https://api.tinybird.co",
    "dev_mode": "branch",
}
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `include` | `list[str]` | *required* | File paths or glob patterns for Python and raw datafiles |
| `token` | `str` | *required* | API token; supports `${ENV_VAR}` interpolation. If missing, SDK falls back to `TINYBIRD_TOKEN`, then `.tinyb` |
| `base_url` | `str` | `"https://api.tinybird.co"` | Tinybird API URL |
| `dev_mode` | `"branch"` \| `"local"` | `"branch"` | Development mode |

If `base_url` is omitted, SDK resolves it from `TINYBIRD_URL`, then `TINYBIRD_HOST`, then `.tinyb` (`host`), and finally defaults to `https://api.tinybird.co`.

### Local Development Mode

Use a local Tinybird container for development without affecting cloud workspaces:

1. Start the local container:
   ```bash
   docker run -d -p 7181:7181 --name tinybird-local tinybirdco/tinybird-local:latest
   ```

2. Configure your project:
   ```json
   {
     "dev_mode": "local"
   }
   ```

   Or use CLI flag:
   ```bash
   tinybird dev --local
   ```

## Defining Resources

### Connections

```python
from tinybird_sdk import define_gcs_connection, define_kafka_connection, define_s3_connection, secret

events_kafka = define_kafka_connection(
    "events_kafka",
    {
        "bootstrap_servers": "kafka.example.com:9092",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "key": secret("KAFKA_KEY"),
        "secret": secret("KAFKA_SECRET"),
    },
)

landing_s3 = define_s3_connection(
    "landing_s3",
    {
        "region": "us-east-1",
        "arn": "arn:aws:iam::123456789012:role/tinybird-s3-access",
    },
)

landing_gcs = define_gcs_connection(
    "landing_gcs",
    {
        "service_account_credentials_json": secret("GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON"),
    },
)
```

### Datasources

```python
from tinybird_sdk import define_datasource, engine, t

events = define_datasource(
    "events",
    {
        "description": "Event tracking data",
        "schema": {
            "timestamp": t.date_time(),
            "event_name": t.string().low_cardinality(),
            "user_id": t.string().nullable(),
            "properties": t.string(),
        },
        "engine": engine.merge_tree(
            {
                "sorting_key": ["event_name", "timestamp"],
                "partition_key": "toYYYYMM(timestamp)",
                "ttl": "timestamp + INTERVAL 90 DAY",
            }
        ),
    },
)
```

### Endpoints (API pipes)

```python
from tinybird_sdk import define_endpoint, node, p, t

top_events = define_endpoint(
    "top_events",
    {
        "description": "Get the most frequent events",
        "params": {
            "start_date": p.date_time(),
            "end_date": p.date_time(),
            "limit": p.int32().optional(10),
        },
        "nodes": [
            node(
                {
                    "name": "aggregated",
                    "sql": """
                        SELECT event_name, count() AS event_count
                        FROM events
                        WHERE timestamp >= {{DateTime(start_date)}}
                          AND timestamp <= {{DateTime(end_date)}}
                        GROUP BY event_name
                        ORDER BY event_count DESC
                        LIMIT {{Int32(limit, 10)}}
                    """,
                }
            )
        ],
        "output": {
            "event_name": t.string(),
            "event_count": t.uint64(),
        },
    },
)
```

### Internal Pipes (not exposed as API)

```python
from tinybird_sdk import define_pipe, node, p

filtered_events = define_pipe(
    "filtered_events",
    {
        "description": "Filter events by date range",
        "params": {
            "start_date": p.date_time(),
            "end_date": p.date_time(),
        },
        "nodes": [
            node(
                {
                    "name": "filtered",
                    "sql": """
                        SELECT * FROM events
                        WHERE timestamp >= {{DateTime(start_date)}}
                          AND timestamp <= {{DateTime(end_date)}}
                    """,
                }
            )
        ],
    },
)
```

### Materialized Views

```python
from tinybird_sdk import define_datasource, define_materialized_view, engine, node, t

daily_stats = define_datasource(
    "daily_stats",
    {
        "schema": {
            "date": t.date(),
            "pathname": t.string(),
            "views": t.simple_aggregate_function("sum", t.uint64()),
            "unique_sessions": t.aggregate_function("uniq", t.string()),
        },
        "engine": engine.aggregating_merge_tree({"sorting_key": ["date", "pathname"]}),
    },
)

daily_stats_mv = define_materialized_view(
    "daily_stats_mv",
    {
        "datasource": daily_stats,
        "nodes": [
            node(
                {
                    "name": "aggregate",
                    "sql": """
                        SELECT
                          toDate(timestamp) AS date,
                          pathname,
                          count() AS views,
                          uniqState(session_id) AS unique_sessions
                        FROM page_views
                        GROUP BY date, pathname
                    """,
                }
            )
        ],
    },
)
```

### Copy Pipes

```python
from tinybird_sdk import define_copy_pipe, node

# Scheduled copy pipe
daily_snapshot = define_copy_pipe(
    "daily_snapshot",
    {
        "datasource": events,
        "copy_schedule": "0 0 * * *",
        "copy_mode": "append",
        "nodes": [
            node(
                {
                    "name": "snapshot",
                    "sql": """
                        SELECT today() AS snapshot_date, event_name, count() AS events
                        FROM events
                        WHERE toDate(timestamp) = today() - 1
                        GROUP BY event_name
                    """,
                }
            )
        ],
    },
)

# On-demand copy pipe
manual_report = define_copy_pipe(
    "manual_report",
    {
        "datasource": events,
        "copy_schedule": "@on-demand",
        "copy_mode": "replace",
        "nodes": [
            node(
                {
                    "name": "report",
                    "sql": "SELECT * FROM events WHERE timestamp >= now() - interval 7 day",
                }
            )
        ],
    },
)
```

### Sink Pipes

Use sink pipes to publish query results to external systems.
The SDK supports Kafka and S3 sinks.

```python
from tinybird_sdk import define_sink_pipe, node

# Kafka sink
kafka_events_sink = define_sink_pipe(
    "kafka_events_sink",
    {
        "sink": {
            "connection": events_kafka,
            "topic": "events_export",
            "schedule": "@on-demand",
        },
        "nodes": [
            node(
                {
                    "name": "publish",
                    "sql": "SELECT timestamp, payload FROM kafka_events",
                }
            )
        ],
    },
)

# S3 sink
s3_events_sink = define_sink_pipe(
    "s3_events_sink",
    {
        "sink": {
            "connection": landing_s3,
            "bucket_uri": "s3://my-bucket/exports/",
            "file_template": "events_{date}",
            "format": "csv",
            "schedule": "@once",
            "strategy": "create_new",
            "compression": "gzip",
        },
        "nodes": [
            node(
                {
                    "name": "export",
                    "sql": "SELECT timestamp, session_id FROM s3_landing",
                }
            )
        ],
    },
)
```

### Static Tokens

```python
from tinybird_sdk import define_datasource, define_endpoint, define_token, node, t

app_token = define_token("app_read")
ingest_token = define_token("ingest_token")

events = define_datasource(
    "events",
    {
        "schema": {
            "timestamp": t.date_time(),
            "event_name": t.string(),
        },
        "tokens": [
            {"token": app_token, "scope": "READ"},
            {"token": ingest_token, "scope": "APPEND"},
        ],
    },
)

top_events = define_endpoint(
    "top_events",
    {
        "nodes": [node({"name": "endpoint", "sql": "SELECT * FROM events LIMIT 10"})],
        "output": {"timestamp": t.date_time(), "event_name": t.string()},
        "tokens": [{"token": app_token, "scope": "READ"}],
    },
)
```

## Type Validators

Use `t.*` to define column types:

```python
from tinybird_sdk import t

schema = {
    # Strings
    "name": t.string(),
    "id": t.uuid(),
    "code": t.fixed_string(3),

    # Numbers
    "count": t.int32(),
    "amount": t.float64(),
    "big_number": t.uint64(),
    "price": t.decimal(10, 2),

    # Date/Time
    "created_at": t.date_time(),
    "updated_at": t.date_time64(3),
    "birth_date": t.date(),

    # Boolean
    "is_active": t.bool(),

    # Complex types
    "tags": t.array(t.string()),
    "metadata": t.map(t.string(), t.string()),

    # Aggregate functions
    "total": t.simple_aggregate_function("sum", t.uint64()),
    "unique_users": t.aggregate_function("uniq", t.string()),

    # Modifiers
    "optional_field": t.string().nullable(),
    "category": t.string().low_cardinality(),
    "status": t.string().default("pending"),
}
```

## Parameter Validators

Use `p.*` to define query parameters:

```python
from tinybird_sdk import p

params = {
    "start_date": p.date_time(),
    "user_id": p.string(),

    "limit": p.int32().optional(10),
    "offset": p.int32().optional(0),

    "status": p.string().optional("active").describe("Filter by status"),
}
```

## Engine Configurations

```python
from tinybird_sdk import engine

engine.merge_tree(
    {
        "sorting_key": ["user_id", "timestamp"],
        "partition_key": "toYYYYMM(timestamp)",
        "ttl": "timestamp + INTERVAL 90 DAY",
    }
)

engine.replacing_merge_tree(
    {
        "sorting_key": ["id"],
        "ver": "updated_at",
    }
)

engine.summing_merge_tree(
    {
        "sorting_key": ["date", "category"],
        "columns": ["count", "total"],
    }
)

engine.aggregating_merge_tree(
    {
        "sorting_key": ["date"],
    }
)
```

## Python App Integration

For Python web apps (FastAPI, Django, Flask), keep Tinybird definitions and client in a dedicated module and import that module from your app services.

The CLI automatically loads `.env.local` and `.env` files in project root when resolving configuration.

## Schema Inference Helpers

The `tinybird_sdk.infer` module can inspect datasource and pipe definitions:

```python
from tinybird_sdk.infer import infer_output_schema, infer_params_schema, infer_row_schema

row_schema = infer_row_schema(page_views)
params_schema = infer_params_schema(top_pages)
output_schema = infer_output_schema(top_pages)
```

## License

MIT
