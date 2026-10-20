"""In-memory registry of ClickHouse clusters backed by JD's internal adminserver.

Lets the MCP server connect to any cluster registered in adminserver instead of
only the single host configured via CLICKHOUSE_HOST.
"""

import logging
import re
import threading
from dataclasses import asdict, dataclass
from typing import List, Optional

from sqlalchemy import text

from mcp_clickhouse.mcp_env import get_config

logger = logging.getLogger("mcp-clickhouse")

_CLUSTERS_SQL = text(
    "select name, admin_name, admin_password, domain, http_port, host_spec, "
    "shard_cnt, replicas_per_shard, host_cnt, version, zk_addrs, zk_version, owner "
    "from clusters where engine_type=0 and is_delete=0;"
)


@dataclass
class ClusterInfo:
    name: str
    admin_name: str
    admin_password: str
    domain: str
    http_port: int
    host_spec: str
    shard_cnt: int
    replicas_per_shard: int
    host_cnt: int
    version: str
    zk_addrs: str
    zk_version: str
    owner: str

    def get_client_config(self) -> dict:
        """Client config for this cluster, based on the default connection settings."""
        config = get_config().get_client_config()
        config.pop("server_host_name", None)
        config.update(
            {
                "host": self.domain,
                "port": self.http_port,
                "username": self.admin_name,
                "password": self.admin_password,
                "interface": "http",
                "secure": False,
            }
        )
        return config

    def to_public_dict(self) -> dict:
        """Cluster metadata without the decrypted password, safe to return to callers."""
        d = asdict(self)
        d.pop("admin_password", None)
        return d


def _fetch_clusters_from_adminserver() -> List[ClusterInfo]:
    try:
        from oneutils.utils import aes_decrypt_admin, get_adminserver
    except ImportError as e:
        raise ImportError(
            "CLICKHOUSE_CLUSTER_REGISTRY_ENABLED=true requires the internal 'oneutils' "
            "package and its dependencies (sqlalchemy, pymysql, pycryptodome) to be "
            "importable. Install mcp-clickhouse[cluster-registry] and ensure oneutils "
            f"is on PYTHONPATH. Original error: {e}"
        ) from e

    clusters: List[ClusterInfo] = []
    with get_adminserver().get_session_scope() as session:
        rows = session.execute(_CLUSTERS_SQL).fetchall()

    for row in rows:
        (
            name,
            admin_name,
            admin_password,
            domain,
            http_port,
            host_spec,
            shard_cnt,
            replicas_per_shard,
            host_cnt,
            version,
            zk_addrs,
            zk_version,
            owner,
        ) = row
        try:
            decrypted_password = aes_decrypt_admin(admin_password)
        except Exception as e:
            logger.warning("Failed to decrypt admin_password for cluster '%s': %s", name, e)
            continue

        clusters.append(
            ClusterInfo(
                name=name,
                admin_name=admin_name,
                admin_password=decrypted_password,
                domain=domain,
                http_port=http_port,
                host_spec=host_spec,
                shard_cnt=shard_cnt,
                replicas_per_shard=replicas_per_shard,
                host_cnt=host_cnt,
                version=version,
                zk_addrs=zk_addrs,
                zk_version=zk_version,
                owner=owner,
            )
        )

    return clusters


class ClusterRegistry:
    """In-memory index of ClickHouse clusters, loaded from adminserver."""

    def __init__(self):
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None
        self._clusters_by_name: dict[str, ClusterInfo] = {}

    def load(self) -> None:
        """Load cluster info from adminserver, replacing the current index.

        Raises on failure; callers decide whether that's fatal (startup) or
        just logged (background refresh).
        """
        clusters = _fetch_clusters_from_adminserver()
        clusters_by_name = {cluster.name: cluster for cluster in clusters}
        with self._lock:
            self._clusters_by_name = clusters_by_name
        logger.info("Loaded %d ClickHouse clusters from adminserver", len(clusters))

    def get_cluster_by_name(self, name: str) -> Optional[List[ClusterInfo]]:
        """Look up clusters by name, supporting fuzzy matching on shorthand names.

        Resolution order:
        1. Exact match on the full cluster name (returns a single-element list).
        2. Regex-based fuzzy match for shorthand names of the form
           ``prefix + (pub|ts|nfs) + number`` (e.g. ``foopub01``), which are
           matched against ``prefix(cn)?_?type?_?number$`` case-insensitively.

        Returns:
            - None if no cluster matches.
            - A list with exactly one ClusterInfo on an unambiguous match.
            - A list with multiple ClusterInfo when a fuzzy pattern matches several
              clusters; the caller is responsible for disambiguation.
        """
        with self._lock:
            # 1. Exact match (for full cluster names).
            cluster = self._clusters_by_name.get(name)
            if cluster is not None:
                return [cluster]

            clusters = list(self._clusters_by_name.values())

        # 2. Regex fuzzy match (for shorthand cluster names).
        # Pattern: prefix + (pub|ts|nfs) + number
        match = re.match(r"(?i)(.*)(pub|ts|nfs)(\d{1,5})", name)
        if match:
            regexp = f"{match.group(1)}(cn)?_?{match.group(2)}?_?{match.group(3)}$"
        else:
            regexp = name

        try:
            compiled = re.compile(regexp, re.IGNORECASE)
        except re.error:
            return None

        matched = [c for c in clusters if compiled.search(c.name)]
        if not matched:
            return None
        return matched

    def get_all_clusters(self) -> List[ClusterInfo]:
        with self._lock:
            return list(self._clusters_by_name.values())

    def start_auto_refresh(self, interval_seconds: float) -> None:
        """Start periodically reloading cluster info in the background.

        A failed refresh is logged and the previously loaded data is kept; the
        next refresh is still scheduled.
        """

        def _refresh():
            try:
                self.load()
            except Exception as e:
                logger.warning("Background ClickHouse cluster registry refresh failed: %s", e)
            self._timer = threading.Timer(interval_seconds, _refresh)
            self._timer.daemon = True
            self._timer.start()

        self._timer = threading.Timer(interval_seconds, _refresh)
        self._timer.daemon = True
        self._timer.start()

    def stop_auto_refresh(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None


_CLUSTER_REGISTRY_INSTANCE: Optional[ClusterRegistry] = None


def get_cluster_registry() -> ClusterRegistry:
    """Gets the singleton instance of ClusterRegistry."""
    global _CLUSTER_REGISTRY_INSTANCE
    if _CLUSTER_REGISTRY_INSTANCE is None:
        _CLUSTER_REGISTRY_INSTANCE = ClusterRegistry()
    return _CLUSTER_REGISTRY_INSTANCE
