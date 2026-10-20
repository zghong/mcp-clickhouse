import logging
import asyncio
import json
import threading
import time
import uuid
from typing import Optional, List, Any, Dict
import concurrent.futures
import atexit

import clickhouse_connect
from clickhouse_connect.driver.binding import format_query_value, external_bind_re
from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.auth.providers.jwt import StaticTokenVerifier
from starlette.responses import PlainTextResponse
from dataclasses import dataclass, field, asdict

from mcp_clickhouse.mcp_env import (ClusterUnspecifiedMode,
                                    get_cluster_registry_config, get_config,
                                    get_mcp_config, TransportType)
from mcp_clickhouse.utils_clusters import get_cluster_registry


@dataclass
class Column:
    database: str
    table: str
    name: str
    column_type: str
    default_kind: Optional[str]
    default_expression: Optional[str]
    comment: Optional[str]


@dataclass
class Table:
    database: str
    name: str
    engine: str
    create_table_query: str
    dependencies_database: str
    dependencies_table: str
    engine_full: str
    sorting_key: str
    primary_key: str
    total_rows: int
    total_bytes: int
    total_bytes_uncompressed: int
    parts: int
    active_parts: int
    total_marks: int
    comment: Optional[str] = None
    columns: List[Column] = field(default_factory=list)


MCP_SERVER_NAME = "mcp-clickhouse"

# ClickHouse native TCP protocol ports (clickhouse-client). This server uses the
# HTTP interface only (default 8123/8443); connecting to native ports fails with
# messages like "Port 9000 is for clickhouse-client program" (from 423ca2e).
_NATIVE_PROTOCOL_PORTS = frozenset({9000, 9440})

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(MCP_SERVER_NAME)

QUERY_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=10)
atexit.register(lambda: QUERY_EXECUTOR.shutdown(wait=True))

load_dotenv()

def _init_cluster_registry() -> None:
    """Load the adminserver-backed cluster registry at startup, if enabled.

    A failed initial load is fatal: the registry was explicitly requested, so
    starting up with an empty/stale cluster index would be misleading.
    """
    cluster_registry_config = get_cluster_registry_config()
    if not cluster_registry_config.enabled:
        return

    registry = get_cluster_registry()
    registry.load()

    refresh_interval = cluster_registry_config.refresh_interval
    if refresh_interval > 0:
        registry.start_auto_refresh(refresh_interval)
        atexit.register(registry.stop_auto_refresh)

_init_cluster_registry()

# Configure authentication for HTTP/SSE transports (from a1f3639).
# A single static bearer token keeps the implementation simple; OAuth/JWT
# providers from upstream are intentionally not ported.
auth_provider = None
mcp_config = get_mcp_config()
http_transports = [TransportType.HTTP.value, TransportType.SSE.value]

if mcp_config.mcp_server_transport in http_transports:
    if mcp_config.auth_disabled:
        logger.warning("WARNING: MCP SERVER AUTHENTICATION IS DISABLED")
        logger.warning("Only use this for local development/testing. DO NOT expose to networks.")
    elif mcp_config.auth_token:
        auth_provider = StaticTokenVerifier(
            tokens={mcp_config.auth_token: {"client_id": "mcp-client", "scopes": []}},
            required_scopes=[],
        )
        logger.info("Authentication enabled for HTTP/SSE transport")
    else:
        raise ValueError(
            "Authentication token required for HTTP/SSE transports. "
            "Set CLICKHOUSE_MCP_AUTH_TOKEN environment variable or set "
            "CLICKHOUSE_MCP_AUTH_DISABLED=true (for development only)."
        )

mcp = FastMCP(name=MCP_SERVER_NAME, auth=auth_provider)

# --- Health endpoint with 1s result cache (ported from 4fea804) ------------
_HEALTH_CHECK_TIMEOUT_SECONDS = 2.0
_HEALTH_RESULT_CACHE_SECONDS = 1.0
_health_state_lock = threading.Lock()
_health_probe_future: Optional[concurrent.futures.Future] = None
_health_result_cache: Optional[tuple] = None  # (expiry_monotonic, healthy)
_HEALTH_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=1)
atexit.register(lambda: _HEALTH_EXECUTOR.shutdown(wait=False))


def _probe_clickhouse_health() -> None:
    """Run an authenticated ClickHouse health query with a cached client."""
    client = create_clickhouse_client()
    client.command("SELECT 1")


def _cached_health_result() -> Optional[bool]:
    """Return the cached health result if it is still fresh."""
    with _health_state_lock:
        if _health_result_cache is not None and time.monotonic() < _health_result_cache[0]:
            return _health_result_cache[1]
    return None


def _get_health_probe_future() -> concurrent.futures.Future:
    """Return the single in-flight ClickHouse health probe."""
    global _health_probe_future
    with _health_state_lock:
        if _health_probe_future is not None and not _health_probe_future.done():
            return _health_probe_future
        future = _HEALTH_EXECUTOR.submit(_probe_clickhouse_health)
        _health_probe_future = future

    def _cache_result(done: concurrent.futures.Future) -> None:
        global _health_result_cache
        if done.cancelled():
            return
        healthy = done.exception() is None
        with _health_state_lock:
            _health_result_cache = (time.monotonic() + _HEALTH_RESULT_CACHE_SECONDS, healthy)

    future.add_done_callback(_cache_result)
    return future


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request) -> PlainTextResponse:
    """Liveness probe: OK if the server can connect to ClickHouse.

    A completed probe result is reused for one second, so a failure or a
    recovery can be reported up to a second late (4fea804).
    """
    cached = _cached_health_result()
    if cached is not None:
        return PlainTextResponse("OK") if cached else PlainTextResponse(
            "ERROR. ClickHouse connection failed. Check server logs for details.",
            status_code=503,
        )

    future = _get_health_probe_future()
    try:
        await asyncio.wait_for(asyncio.wrap_future(future), timeout=_HEALTH_CHECK_TIMEOUT_SECONDS)
        return PlainTextResponse("OK")
    except asyncio.TimeoutError:
        logger.warning("Health check timed out")
        return PlainTextResponse("ERROR. ClickHouse health check timed out.", status_code=503)
    except Exception as e:
        logger.warning(f"Health check failed: {e}")
        return PlainTextResponse(
            "ERROR. ClickHouse connection failed. Check server logs for details.",
            status_code=503,
        )


def result_to_table(query_columns, result) -> List[Table]:
    return [Table(**dict(zip(query_columns, row))) for row in result]


def result_to_column(query_columns, result) -> List[Column]:
    return [Column(**dict(zip(query_columns, row))) for row in result]


# --- Tool result serialization (ported from 6e04286 + 82f0d12) -------------
# MCP protocol requires tool results to be strings. Large integers outside
# JavaScript's safe range are stringified to avoid precision corruption.
_JS_MAX_SAFE_INTEGER = 9007199254740991

def _stringify_unsafe_integers(obj: Any) -> Any:
    """Convert ints outside the JS safe range to strings (82f0d12), simply."""
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        return str(obj) if abs(obj) > _JS_MAX_SAFE_INTEGER else obj
    if isinstance(obj, dict):
        return {k: _stringify_unsafe_integers(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_stringify_unsafe_integers(v) for v in obj]
    return obj

def _serialize_tool_result(obj: Any) -> str:
    """Serialize a tool result to a JSON string (unified contract from 6e04286)."""
    return json.dumps(_stringify_unsafe_integers(obj), default=str)

def _check_cluster_specified(cluster_name: Optional[str]) -> None:
    """Enforce CLICKHOUSE_CLUSTER_UNSPECIFIED_MODE when the registry is enabled.

    In "prompt" mode, tools must be called with an explicit cluster_name once
    the adminserver-backed registry is enabled — otherwise it's ambiguous which
    of the many registered clusters the caller means.
    """
    if cluster_name:
        return

    cluster_registry_config = get_cluster_registry_config()
    if not cluster_registry_config.enabled:
        return

    if cluster_registry_config.unspecified_mode == ClusterUnspecifiedMode.PROMPT.value:
        raise ToolError(
            "STOP. No cluster_name specified. This server has multiple ClickHouse clusters "
            "registered, so it is ambiguous which one to use. Do NOT guess and MUST ask the "
            "user directly for the right cluster name they want to operate on. Or set "
            "CLICKHOUSE_CLUSTER_UNSPECIFIED_MODE=fallback to default to CLICKHOUSE_HOST "
            "instead when cluster_name is omitted."
        )

def list_clusters(
    cluster_name: Optional[str] = None,
    verbose: bool = False,
) -> str:
    """List ClickHouse clusters registered in the adminserver cluster registry.

    RESTRICTED TOOL — read this before calling:
      - Only call this tool when the user EXPLICITLY asks to list/enumerate/discover clusters.
      - Do NOT call this tool to guess or resolve a cluster after another tool
        returned an "Unknown cluster" / "ambiguous cluster" error. In that case you
        MUST ask the user directly for the exact cluster name instead.
      - Only available when CLICKHOUSE_CLUSTER_REGISTRY_ENABLED=true.

    Args:
        cluster_name: Return only the cluster with this exact name, if any.
        verbose: When False (default), return only lightweight cluster names to keep
            the payload small. When True, return full metadata for each cluster.
    """
    registry = get_cluster_registry()
    if cluster_name:
        clusters = registry.get_cluster_by_name(cluster_name) or []
    else:
        clusters = registry.get_all_clusters()

    if verbose:
        items: Any = [cluster.to_public_dict() for cluster in clusters]
    else:
        items = [cluster.name for cluster in clusters]

    return _serialize_tool_result(items)

@mcp.tool()
def list_databases(cluster_name: Optional[str] = None) -> str:
    """List available ClickHouse databases

    Args:
        cluster_name: Name of a registered ClickHouse cluster to query. If omitted,
            uses the default cluster configured via CLICKHOUSE_HOST.
    """
    _check_cluster_specified(cluster_name)
    logger.info(f"Listing all databases (cluster_name={cluster_name})")
    client = create_clickhouse_client(cluster_name)
    result = client.command("SHOW DATABASES")

    # Convert newline-separated string to list and trim whitespace
    if isinstance(result, str):
        databases = [db.strip() for db in result.strip().split("\n")]
    else:
        databases = [result]

    logger.info(f"Found {len(databases)} databases")
    return json.dumps(databases)


@mcp.tool()
def list_tables(database: str, like: Optional[str] = None, not_like: Optional[str] = None, cluster_name: Optional[str] = None):
    """List available ClickHouse tables in a database, including schema, comment,
    row count, and column count.

    Args:
        database: The database to list tables from
        like: Optional LIKE pattern to filter table names
        not_like: Optional NOT LIKE pattern to exclude table names
        cluster_name: Name of a registered ClickHouse cluster to query. If omitted,
            uses the default cluster configured via CLICKHOUSE_HOST.
    """
    _check_cluster_specified(cluster_name)
    logger.info(
        "Listing tables in database '%s' with like=%s, not_like=%s, cluster_name=%s",
        database,
        like,
        not_like,
        cluster_name,
    )
    client = create_clickhouse_client(cluster_name)
    query = f"SELECT database, name, engine, create_table_query, dependencies_database, dependencies_table, engine_full, sorting_key, primary_key, total_rows, total_bytes, total_bytes_uncompressed, parts, active_parts, total_marks, comment FROM system.tables WHERE database = {format_query_value(database)}"
    if like:
        query += f" AND name LIKE {format_query_value(like)}"

    if not_like:
        query += f" AND name NOT LIKE {format_query_value(not_like)}"

    result = client.query(query)

    # Deserialize result as Table dataclass instances
    tables = result_to_table(result.column_names, result.result_rows)

    for table in tables:
        column_data_query = f"SELECT database, table, name, type AS column_type, default_kind, default_expression, comment FROM system.columns WHERE database = {format_query_value(database)} AND table = {format_query_value(table.name)}"
        column_data_query_result = client.query(column_data_query)
        table.columns = [
            c
            for c in result_to_column(
                column_data_query_result.column_names,
                column_data_query_result.result_rows,
            )
        ]

    logger.info(f"Found {len(tables)} tables")
    # Unified string return contract (6e04286)
    return _serialize_tool_result([asdict(table) for table in tables])


def execute_query(query: str, params: Optional[Dict[str, Any]] = None, query_id: Optional[str] = None, cluster_name: Optional[str] = None):
    """Execute a query in a worker thread (off the event loop, cb75c27)."""
    client = create_clickhouse_client(cluster_name)
    read_only = get_readonly_setting(client)
    settings = {"readonly": read_only}
    if query_id:
        # Attach a server-side query_id so the query can be cancelled (0893019)
        settings["query_id"] = query_id
    res = client.query(query, parameters=params, settings=settings)
    logger.info(f"Query returned {len(res.result_rows)} rows")
    return {"columns": res.column_names, "rows": res.result_rows}

def _cancel_query(query_id: str, cluster_name: Optional[str] = None) -> None:
    """Best-effort server-side KILL QUERY for an in-flight query (0893019)."""
    try:
        client = create_clickhouse_client(cluster_name)
        client.command(f"KILL QUERY WHERE query_id = {format_query_value(query_id)}")
        logger.info(f"Successfully cancelled query {query_id}")
    except Exception as e:
        logger.warning(f"Failed to cancel query {query_id}: {e}")

@mcp.tool()
async def run_select_query(query: str, params: Optional[Dict[str, Any]] = None, cluster_name: Optional[str] = None) -> str:
    """Run a SELECT query in a ClickHouse database.

    Bind params by name with ClickHouse placeholders such as {name:String}
    (native parameter binding from 1cf9ab6). Values may be JSON scalars,
    nulls, or arrays; pass exact large integers as decimal strings.

    Args:
        query: The SQL query to execute
        params: Optional named parameters bound via {name:Type} placeholders
        cluster_name: Name of a registered ClickHouse cluster to query (see
            list_clusters). If omitted, uses the default cluster configured via
            CLICKHOUSE_HOST.
    """
    _check_cluster_specified(cluster_name)
    logger.info(f"Executing SELECT query: {query} (cluster_name={cluster_name})")
    # Native {name:Type} placeholder binding only (1cf9ab6): keep parameters
    # out of client-side string formatting and raw binary paths.
    if params is not None:
        if not isinstance(params, dict) or any(not isinstance(k, str) for k in params):
            raise ToolError("params must be an object with string keys")
        if params and external_bind_re.search(query) is None:
            raise ToolError(
                "params requires ClickHouse {name:Type} placeholders, for example {id:UInt32}. "
                "Python percent formatting and $name$ raw binary parameters are not supported."
            )

    timeout_secs = get_mcp_config().query_timeout
    query_id = str(uuid.uuid4())
    future = None
    try:
        # Await the worker-pool future asynchronously so the event loop keeps
        # serving other tool calls while a slow query runs (cb75c27).
        future = QUERY_EXECUTOR.submit(execute_query, query, params, query_id, cluster_name)
        result = await asyncio.wait_for(asyncio.wrap_future(future), timeout=timeout_secs)
        return _serialize_tool_result(result)
    except concurrent.futures.TimeoutError:
        logger.warning(f"Query timed out after {timeout_secs} seconds: {query}")
        future.cancel()
        raise ToolError(f"Query timed out after {timeout_secs} seconds")
    except asyncio.TimeoutError:
        logger.warning(f"Query timed out after {timeout_secs} seconds: {query}")
        if not future.cancel():
            # Already running: issue a server-side KILL QUERY (0893019)
            await asyncio.to_thread(_cancel_query, query_id, cluster_name)
        raise ToolError(f"Query timed out after {timeout_secs} seconds")
    except asyncio.CancelledError:
        # Client cancelled the tool call: cancel the future, and if it is
        # already running, issue a server-side KILL QUERY (0893019).
        if not future.cancel():
            await asyncio.to_thread(_cancel_query, query_id, cluster_name)
        raise
    except ToolError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in run_select_query: {str(e)}")
        raise ToolError(f"Unexpected error during query execution: {str(e)}")

def _connection_error_hints(error: Exception, client_config: dict) -> List[str]:
    """Return actionable hints for common ClickHouse connection misconfigurations.

    Ported from 423ca2e: helps users who confuse MCP transport settings with
    database settings, or who point CLICKHOUSE_PORT at the native TCP protocol.
    """
    hints: List[str] = []
    err = str(error).lower()
    port = client_config.get("port")
    secure = bool(client_config.get("secure"))
    host = client_config.get("host", "<unknown>")

    native_response_port = next(
        (
            native_port
            for native_port in _NATIVE_PROTOCOL_PORTS
            if f"port {native_port} is for clickhouse-client" in err
        ),
        None,
    )
    if port in _NATIVE_PROTOCOL_PORTS:
        hints.append(
            f"CLICKHOUSE_PORT={port} looks like ClickHouse's native TCP protocol port "
            "(used by clickhouse-client). This server uses the HTTP interface — set "
            "CLICKHOUSE_PORT to 8123 (HTTP) or 8443 (HTTPS), or your deployment's HTTP "
            "mapping. Do not use native ports 9000/9440."
        )
    elif native_response_port is not None:
        hints.append(
            f"The ClickHouse response indicates that this request reached native TCP port "
            f"{native_response_port}, even though the client was configured for {host}:{port}. "
            "Check DNS, service, proxy, load-balancer, and port mappings to ensure traffic is "
            "routed to ClickHouse's HTTP interface (8123/8443 by default)."
        )

    tls_tokens = (
        "ssl", "tls", "certificate", "handshake", "wrong version number",
        "certificate verify failed", "unexpected_eof",
        "eof occurred in violation of protocol",
    )
    if any(token in err for token in tls_tokens):
        scheme = "HTTPS" if secure else "HTTP"
        hints.append(
            f"TLS/SSL error while connecting with CLICKHOUSE_SECURE="
            f"{str(secure).lower()} ({scheme} to {host}:{port}). "
            "CLICKHOUSE_SECURE enables HTTPS for the ClickHouse database connection "
            "only — it is not MCP or ingress TLS. Use true for HTTPS database "
            "endpoints (port 8443) and false only for plain HTTP (typical local "
            "Docker on 8123)."
        )

    # General connectivity failures can surface as opaque HTTP errors.
    connection_failure_tokens = (
        "http status", "bad status line", "connection refused",
        "connection reset", "remote end closed connection",
    )
    if any(token in err for token in connection_failure_tokens) and not hints:
        hints.append(
            f"Connection to {host}:{port} failed. Verify ClickHouse is running and reachable "
            "at this address and that network or proxy routing permits access. Then confirm "
            f"CLICKHOUSE_SECURE={str(secure).lower()} matches whether ClickHouse expects HTTPS, "
            "and that CLICKHOUSE_PORT is an HTTP interface port (8123/8443), not a native TCP "
            "port (9000/9440)."
        )

    return hints


def _format_connection_failure(error: Exception, client_config: dict) -> str:
    """Build a connection failure message with optional configuration hints."""
    message = f"Failed to connect to ClickHouse: {error}"
    hints = _connection_error_hints(error, client_config)
    if hints:
        message += "\n" + "\n".join(f"Hint: {hint}" for hint in hints)
    return message


# --- Client connection reuse (ported from 0893019, simplified) -------------
# A single config (internal use: one cluster) maps to one cached client.
# The client is pinged after an idle period and re-created when dead.
_CLIENT_IDLE_PING_SECONDS = 60.0
_client_cache_lock = threading.Lock()
_client_cache: Dict[tuple, Any] = {}


def _client_cache_key(client_config: dict) -> tuple:
    """Stable cache key from the client config dict."""
    return tuple(sorted((k, str(v)) for k, v in client_config.items()))


def _create_clickhouse_client(client_config: dict):
    logger.info(
        f"Creating ClickHouse client connection to {client_config['host']}:{client_config['port']} "
        f"as {client_config['username']} "
        f"(secure={client_config['secure']}, verify={client_config['verify']}, "
        f"connect_timeout={client_config['connect_timeout']}s, "
        f"send_receive_timeout={client_config['send_receive_timeout']}s)"
    )

    try:
        logger.info(f"Connecting to ClickHouse server... {client_config}")
        client = clickhouse_connect.get_client(autogenerate_session_id=False, **client_config)
        # Test the connection
        version = client.server_version
        logger.info(f"Successfully connected to ClickHouse server version {version}")
        return client
    except Exception as e:
        # Attach actionable configuration hints to connection failures (423ca2e)
        message = _format_connection_failure(e, client_config)
        logger.error(message)
        raise


def create_clickhouse_client(cluster_name: Optional[str] = None):
    """Return a cached, reused ClickHouse client (0893019).

    Falls back to per-call creation only if the cached client cannot be
    health-checked, so a broken connection never blocks tool calls.

    With cluster_name, connect to a cluster registered in the adminserver
    cluster registry instead of the default CLICKHOUSE_HOST cluster.
    """
    if cluster_name:
        clusters = get_cluster_registry().get_cluster_by_name(cluster_name)
        if not clusters:
            raise ToolError(
                f"STOP. Unknown ClickHouse cluster '{cluster_name}'. Do NOT guess and "
                "MUST ask the user directly for the right cluster name they want to "
                "operate on, then retry this tool with that name."
            )
        if len(clusters) > 1:
            matched_names = ", ".join(c.name for c in clusters)
            raise ToolError(
                f"STOP. The cluster name '{cluster_name}' is ambiguous and matches multiple "
                f"clusters: {matched_names}. Do NOT guess and MUST ask the user directly for "
                f"the right cluster name they want to operate on, then retry this tool with "
                f"that name."
            )
        client_config = clusters[0].get_client_config()
    else:
        client_config = get_config().get_client_config()
    key = _client_cache_key(client_config)

    with _client_cache_lock:
        entry = _client_cache.get(key)
        if entry is not None:
            _client_cache[key] = (entry[0], time.monotonic())
            if time.monotonic() - entry[1] <= _CLIENT_IDLE_PING_SECONDS:
                logger.debug("Reusing cached ClickHouse client")
                return entry[0]

    # Cache miss or client idle too long: verify it is still alive.
    if entry is not None:
        try:
            if entry[0].ping():
                with _client_cache_lock:
                    _client_cache[key] = (entry[0], time.monotonic())
                logger.debug("Reusing cached ClickHouse client (ping OK after idle)")
                return entry[0]
        except Exception:
            pass
        # Dead client: drop it and create a fresh one below.
        try:
            entry[0].close()
        except Exception:
            pass

    client = _create_clickhouse_client(client_config)
    with _client_cache_lock:
        _client_cache[key] = (client, time.monotonic())
    return client


def get_readonly_setting(client) -> str:
    """Get the appropriate readonly setting value to use for queries.

    This function handles potential conflicts between server and client readonly settings:
    - readonly=0: No read-only restrictions
    - readonly=1: Only read queries allowed, settings cannot be changed
    - readonly=2: Only read queries allowed, settings can be changed (except readonly itself)

    If server has readonly=2 and client tries to set readonly=1, it would cause:
    "Setting readonly is unknown or readonly" error

    This function preserves the server's readonly setting unless it's 0, in which case
    we enforce readonly=1 to ensure queries are read-only.

    Args:
        client: ClickHouse client connection

    Returns:
        String value of readonly setting to use
    """
    read_only = client.server_settings.get("readonly")
    if read_only:
        if read_only == "0":
            return "1"  # Force read-only mode if server has it disabled
        else:
            return read_only.value  # Respect server's readonly setting (likely 2)
    else:
        return "1"  # Default to basic read-only mode if setting isn't present

# Register the cluster discovery tool only when the registry is enabled
# (upstream registers it inside the CLICKHOUSE_ENABLED block).
if get_cluster_registry_config().enabled:
    mcp.add_tool(list_clusters)
    logger.info("Cluster registry enabled: list_clusters tool registered")
