# ClickHouse MCP Server

[![PyPI - Version](https://img.shields.io/pypi/v/mcp-clickhouse)](https://pypi.org/project/mcp-clickhouse)

An MCP server for ClickHouse, maintained as a simple fork based on
[ClickHouse/mcp-clickhouse](https://github.com/ClickHouse/mcp-clickhouse) at `edc9fe5`.

The fork keeps a deliberately simple single-module implementation:
it connects to a ClickHouse cluster, runs read-only queries to analyze
cluster state, and intentionally excludes upstream features that add
complexity without self-use value (chDB, OAuth, middleware hooks, runtime
config overrides, etc.).
Select upstream fixes and features are ported as simplified equivalents;
ported commits are referenced by hash throughout this document.

<a href="https://glama.ai/mcp/servers/yvjy4csvo1"><img width="380" height="200" src="https://glama.ai/mcp/servers/yvjy4csvo1/badge" alt="mcp-clickhouse MCP server" /></a>

## Features

### Tools

* `run_select_query`
  * Execute SQL queries on your ClickHouse cluster (async; other tool calls
    keep being served while a slow query runs, ported from `cb75c27`).
  * Input: `query` (string): The SQL query to execute.
  * Input: `params` (optional object): Native ClickHouse `{name:Type}` bind
    parameters, for example `{"id": 42}` with `{id:UInt32} WHERE ...`
    (ported from `1cf9ab6`). Python percent formatting and `$name$` raw
    binary parameters are not supported.
  * All ClickHouse queries are run with `readonly = 1` to ensure they are safe.
  * Fails with a `ToolError` on query errors or when the query exceeds
    `CLICKHOUSE_MCP_QUERY_TIMEOUT`; on timeout/cancel a best-effort server-side
    `KILL QUERY` is issued using a generated `query_id` (ported from `0893019`).
  * Returns a JSON string with `columns` and `rows`; integers outside
    JavaScript's safe range are stringified to avoid precision loss
    (ported from `6e04286` + `82f0d12`).

* `list_databases`
  * List all databases on your ClickHouse cluster (returns a JSON string).

* `list_tables`
  * List all tables in a database, including schema and column comments
    (returns a JSON string).
  * Input: `database` (string): The name of the database.
  * Input: `like` (optional string): Table name LIKE filter.
  * Input: `not_like` (optional string): Table name NOT LIKE filter.

### HTTP endpoint

* `GET /health`
  * Liveness probe available on HTTP/SSE transports: `OK` (200) when a
    `SELECT 1` succeeds, `503` otherwise.
  * A completed probe result is cached for one second and in-flight probes
    are shared (single-flight), ported from `4fea804`.

### Connection handling

* ClickHouse clients are cached and reused across tool calls; a client idle
  for more than 60 seconds is pinged before reuse and re-created when dead
  (simplified port of `0893019` — single config per process, simple use
  connects to one cluster).
* Connection failures carry actionable hints for common misconfigurations:
  native TCP ports (9000/9440) instead of HTTP ports, TLS/scheme mismatches,
  and opaque connectivity errors (ported from `423ca2e`).

## Configuration

1. Open the Claude Desktop configuration file located at:
   * On macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   * On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

2. Add the following:

```json
{
  "mcpServers": {
    "mcp-clickhouse": {
      "command": "uv",
      "args": [
        "run",
        "--with",
        "mcp-clickhouse",
        "--python",
        "3.13",
        "mcp-clickhouse"
      ],
      "env": {
        "CLICKHOUSE_HOST": "<clickhouse-host>",
        "CLICKHOUSE_PORT": "<clickhouse-port>",
        "CLICKHOUSE_USER": "<clickhouse-user>",
        "CLICKHOUSE_PASSWORD": "<clickhouse-password>",
        "CLICKHOUSE_SECURE": "true",
        "CLICKHOUSE_VERIFY": "true",
        "CLICKHOUSE_CONNECT_TIMEOUT": "30",
        "CLICKHOUSE_SEND_RECEIVE_TIMEOUT": "30"
      }
    }
  }
}
```

Update the environment variables to point to your own ClickHouse service.

Or, if you'd like to try it out with the [ClickHouse SQL Playground](https://sql.clickhouse.com/), you can use the following config:

```json
{
  "mcpServers": {
    "mcp-clickhouse": {
      "command": "uv",
      "args": [
        "run",
        "--with",
        "mcp-clickhouse",
        "--python",
        "3.13",
        "mcp-clickhouse"
      ],
      "env": {
        "CLICKHOUSE_HOST": "sql-clickhouse.clickhouse.com",
        "CLICKHOUSE_PORT": "8443",
        "CLICKHOUSE_USER": "demo",
        "CLICKHOUSE_PASSWORD": "",
        "CLICKHOUSE_SECURE": "true",
        "CLICKHOUSE_VERIFY": "true",
        "CLICKHOUSE_CONNECT_TIMEOUT": "30",
        "CLICKHOUSE_SEND_RECEIVE_TIMEOUT": "30"
      }
    }
  }
}
```

3. Locate the command entry for `uv` and replace it with the absolute path to the `uv` executable. This ensures that the correct version of `uv` is used when starting the server. On a mac, you can find this path using `which uv`.

4. Restart Claude Desktop to apply the changes.

## Development

1. In `test-services` directory run `docker compose up -d` to start the ClickHouse cluster.

2. Add the following variables to a `.env` file in the root of the repository.

*Note: The use of the `default` user in this context is intended solely for local development purposes.*

```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse
```

3. Run `uv sync` to install the dependencies. To install `uv` follow the instructions [here](https://docs.astral.sh/uv/). Then do `source .venv/bin/activate`.

4. For easy testing with the MCP Inspector, run `fastmcp dev mcp_clickhouse/mcp_server.py` to start the MCP server.

### Environment Variables

The following environment variables are used to configure the ClickHouse connection:

#### Required Variables

* `CLICKHOUSE_HOST`: The hostname of your ClickHouse server
* `CLICKHOUSE_USER`: The username for authentication
* `CLICKHOUSE_PASSWORD`: The password for authentication

> [!CAUTION]
> It is important to treat your MCP database user as you would any external client connecting to your database, granting only the minimum necessary privileges required for its operation. The use of default or administrative users should be strictly avoided at all times.

#### Optional Variables

* `CLICKHOUSE_PORT`: The port number of your ClickHouse server
  * Default: `8443` if HTTPS is enabled, `8123` if disabled
  * Usually doesn't need to be set unless using a non-standard port
* `CLICKHOUSE_SECURE`: Enable/disable HTTPS connection
  * Default: `"true"`
  * Set to `"false"` for non-secure connections
* `CLICKHOUSE_VERIFY`: Enable/disable SSL certificate verification
  * Default: `"true"`
  * Set to `"false"` to disable certificate verification (not recommended for production)
* `CLICKHOUSE_CONNECT_TIMEOUT`: Connection timeout in seconds
  * Default: `"30"`
  * Increase this value if you experience connection timeouts
* `CLICKHOUSE_SEND_RECEIVE_TIMEOUT`: Send/receive timeout in seconds
  * Default: `"300"`
  * Increase this value for long-running queries
* `CLICKHOUSE_DATABASE`: Default database to use
  * Default: None (uses server default)
  * Set this to automatically connect to a specific database
* `CLICKHOUSE_PROXY_PATH`: Path appended to the host URL, for servers behind an HTTP proxy
  * Default: None
* `CLICKHOUSE_MCP_SERVER_TRANSPORT`: Sets the transport method for the MCP server.
  * Default: `"stdio"`
  * Valid options: `"stdio"`, `"http"`, `"sse"`. This is useful for local
    development with tools like MCP Inspector.
* `CLICKHOUSE_MCP_BIND_HOST`: Bind host for HTTP/SSE transports
  * Default: `"127.0.0.1"` (ported from `9aa8188`)
* `CLICKHOUSE_MCP_BIND_PORT`: Bind port for HTTP/SSE transports
  * Default: `"8000"` (ported from `9aa8188`)
* `CLICKHOUSE_MCP_AUTH_TOKEN`: Bearer token required for HTTP/SSE transports
  * Default: None
  * Requests must present `Authorization: Bearer <token>`; the token is
    verified by a static token verifier (simplified port of `a1f3639`;
    upstream OAuth/JWT providers are intentionally not ported)
  * Required unless `CLICKHOUSE_MCP_AUTH_DISABLED=true`; stdio transport
    does not require authentication
* `CLICKHOUSE_MCP_AUTH_DISABLED`: Disable HTTP/SSE authentication
  * Default: `"false"` — only use for local development, never expose an
    unauthenticated MCP server to a network
* `CLICKHOUSE_MCP_QUERY_TIMEOUT`: Tool timeout for `run_select_query` in seconds
  * Default: `"30"` (ported from `9f91105`)
  * On timeout the query future is cancelled and a server-side `KILL QUERY`
    is issued (ported from `0893019`)

#### Example Configurations

For local development with Docker:

```env
# Required variables
CLICKHOUSE_HOST=localhost
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse

# Optional: Override defaults for local development
CLICKHOUSE_SECURE=false  # Uses port 8123 automatically
CLICKHOUSE_VERIFY=false
```

For ClickHouse Cloud:

```env
# Required variables
CLICKHOUSE_HOST=your-instance.clickhouse.cloud
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password

# Optional: These use secure defaults
# CLICKHOUSE_SECURE=true  # Uses port 8443 automatically
# CLICKHOUSE_DATABASE=your_database
```

For ClickHouse SQL Playground:

```env
CLICKHOUSE_HOST=sql-clickhouse.clickhouse.com
CLICKHOUSE_USER=demo
CLICKHOUSE_PASSWORD=
# Uses secure defaults (HTTPS on port 8443)
```

You can set these variables in your environment, in a `.env` file, or in the client configuration.

For HTTP/SSE deployment (with authentication):

```env
CLICKHOUSE_HOST=your-instance.clickhouse.cloud
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password

# MCP server transport
CLICKHOUSE_MCP_SERVER_TRANSPORT=http
CLICKHOUSE_MCP_BIND_HOST=0.0.0.0
CLICKHOUSE_MCP_BIND_PORT=8000
CLICKHOUSE_MCP_AUTH_TOKEN=<random-token>

# Query tool behavior
CLICKHOUSE_MCP_QUERY_TIMEOUT=30
```

### Behavior notes

* Tool results (`run_select_query`, `list_databases`, `list_tables`) are JSON
  strings per the MCP protocol contract (`6e04286`); parse them with
  `JSON.parse` / `json.loads` on the client side.
* All queries enforce `readonly`; the server respects the server-side
  `readonly` setting when it is stricter (`readonly=2`).
* The connection interface (`http`/`https`) is derived explicitly from
  `CLICKHOUSE_SECURE` (`2c96c64`), so `CLICKHOUSE_PORT` must be an HTTP
  interface port (8123/8443), not a native TCP port (9000/9440).
* Python 3.10+ is required; the server targets fastmcp 4.x.

### Running tests

```bash
uv sync --all-extras --dev # install dev dependencies
uv run ruff check . # run linting

docker compose up -d test_services # start ClickHouse
uv run pytest -v tests
```

## Ported upstream commits

Features and fixes ported from upstream `main` as simplified equivalents:

| Upstream commit | Ported feature |
|---|---|
| `2c96c64` | Explicit `interface` (http/https) derived from `CLICKHOUSE_SECURE` |
| `9aa8188` | `CLICKHOUSE_MCP_BIND_HOST` / `CLICKHOUSE_MCP_BIND_PORT` for HTTP/SSE |
| `a1f3639` | Bearer-token authentication for HTTP/SSE (static token only) |
| `9f91105` | Configurable query timeout (`CLICKHOUSE_MCP_QUERY_TIMEOUT`) |
| `6e04286` | Unified JSON-string return contract for all tools |
| `82f0d12` | Stringify integers outside JS safe range in JSON results |
| `1cf9ab6` | Native `{name:Type}` query parameters via `params` |
| `cb75c27` | Async query execution so long queries do not block the event loop |
| `0893019` | Client connection reuse (simplified), query_id + KILL QUERY cancellation |
| `4fea804` | `/health` endpoint with single-flight probe and 1-second result cache |
| `423ca2e` | Actionable hints on ClickHouse connection failures |

Not ported by design: chDB local analytics, OAuth/JWT providers, custom
middleware hooks, MCP Context session config overrides, write-access mode, and upstream release/CI tooling.

## YouTube Overview

[![YouTube](http://i.ytimg.com/vi/y9biAm_Fkqw/hqdefault.jpg)](https://www.youtube.com/watch?v=y9biAm_Fkqw)
