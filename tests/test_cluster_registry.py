"""Tests for the adminserver-backed ClickHouse cluster registry.

All tests are fully mocked: none of them talk to a real adminserver, MySQL,
or ClickHouse instance.
"""

import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from fastmcp.exceptions import ToolError

from mcp_clickhouse.mcp_server import (_check_cluster_specified,
                                       create_clickhouse_client, list_clusters)
from mcp_clickhouse.utils_clusters import ClusterInfo, ClusterRegistry

@pytest.fixture(autouse=True)
def _clear_client_cache():
    """Isolate the shared client cache (fork-specific 0893019 feature).

    The fork caches created ClickHouse clients globally; tests that mock
    clickhouse_connect would otherwise leak MagicMock clients into (or pick
    up clients from) other test modules.
    """
    import mcp_clickhouse.mcp_server as mcp_server
    mcp_server._client_cache.clear()
    yield
    mcp_server._client_cache.clear()


def _make_cluster(name="cluster-a", owner="alice") -> ClusterInfo:
    return ClusterInfo(
        name=name,
        admin_name="admin",
        admin_password="decrypted-secret",
        domain=f"{name}.example.com",
        http_port=2000,
        host_spec="host-spec",
        shard_cnt=3,
        replicas_per_shard=2,
        host_cnt=6,
        version="23.8",
        zk_addrs="zk1:2181,zk2:2181",
        zk_version="3.7",
        owner=owner,
    )


class TestClusterInfo:
    @patch("mcp_clickhouse.utils_clusters.get_config")
    def test_get_client_config_overrides_connection_fields(self, mock_get_config):
        mock_config = MagicMock()
        mock_config.get_client_config.return_value = {
            "host": "default-host",
            "port": 8443,
            "username": "default-user",
            "password": "default-pass",
            "interface": "https",
            "secure": True,
            "verify": True,
            "connect_timeout": 30,
            "send_receive_timeout": 300,
            "client_name": "mcp_clickhouse",
            "server_host_name": "sni-host",
        }
        mock_get_config.return_value = mock_config

        cluster = _make_cluster()
        client_config = cluster.get_client_config()

        assert client_config["host"] == "cluster-a.example.com"
        assert client_config["port"] == 2000
        assert client_config["username"] == "admin"
        assert client_config["password"] == "decrypted-secret"
        assert client_config["interface"] == "http"
        assert client_config["secure"] is False
        # Base settings (timeouts, etc.) are preserved from the default config.
        assert client_config["connect_timeout"] == 30
        assert client_config["send_receive_timeout"] == 300
        # SNI override doesn't apply to a plaintext HTTP connection.
        assert "server_host_name" not in client_config

    def test_to_public_dict_excludes_password(self):
        cluster = _make_cluster()
        public = cluster.to_public_dict()

        assert "admin_password" not in public
        assert public["name"] == "cluster-a"
        assert public["owner"] == "alice"


class TestClusterRegistry:
    def test_load_populates_index(self):
        registry = ClusterRegistry()
        clusters = [_make_cluster("cluster-a", "alice"), _make_cluster("cluster-b", "bob")]

        with patch("mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver", return_value=clusters):
            registry.load()

        assert registry.get_cluster_by_name("cluster-a")[0].owner == "alice"
        assert registry.get_cluster_by_name("cluster-b")[0].owner == "bob"
        assert registry.get_cluster_by_name("cluster-missing") is None
        assert {c.name for c in registry.get_all_clusters()} == {"cluster-a", "cluster-b"}

    def test_get_cluster_by_name_exact_match_takes_priority(self):
        registry = ClusterRegistry()
        clusters = [_make_cluster("foo_pub_01"), _make_cluster("foopub01")]

        with patch("mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver", return_value=clusters):
            registry.load()

        # Even though the shorthand "foopub01" would fuzzy-match "foo_pub_01",
        # an exact match on the full name must win.
        assert registry.get_cluster_by_name("foopub01")[0].name == "foopub01"

    def test_get_cluster_by_name_fuzzy_matches_shorthand(self):
        registry = ClusterRegistry()
        clusters = [_make_cluster("foo_pub_01"), _make_cluster("bar_ts_2")]

        with patch("mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver", return_value=clusters):
            registry.load()

        # Shorthand names resolve to the full cluster name via regex.
        assert registry.get_cluster_by_name("foopub01")[0].name == "foo_pub_01"
        assert registry.get_cluster_by_name("barts2")[0].name == "bar_ts_2"

    def test_get_cluster_by_name_fuzzy_matches_cn_variant(self):
        registry = ClusterRegistry()
        clusters = [_make_cluster("foocn_pub_01")]

        with patch("mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver", return_value=clusters):
            registry.load()

        # The optional "cn" segment is matched by the fuzzy pattern.
        assert registry.get_cluster_by_name("foopub01")[0].name == "foocn_pub_01"

    def test_get_cluster_by_name_is_case_insensitive(self):
        registry = ClusterRegistry()
        clusters = [_make_cluster("foo_pub_01")]

        with patch("mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver", return_value=clusters):
            registry.load()

        assert registry.get_cluster_by_name("FOOPUB01")[0].name == "foo_pub_01"

    def test_get_cluster_by_name_no_match_returns_none(self):
        registry = ClusterRegistry()
        clusters = [_make_cluster("foo_pub_01")]

        with patch("mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver", return_value=clusters):
            registry.load()

        assert registry.get_cluster_by_name("bazpub09") is None
        # A name that doesn't match the shorthand pattern and isn't an exact
        # name resolves to nothing either.
        assert registry.get_cluster_by_name("unknown-cluster") is None

    def test_get_cluster_by_name_ambiguous_match_returns_multiple(self):
        registry = ClusterRegistry()
        clusters = [_make_cluster("foo_pub_01"), _make_cluster("foo_pub01")]

        with patch("mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver", return_value=clusters):
            registry.load()

        # A fuzzy shorthand that matches several clusters now returns all of them;
        # the caller is responsible for disambiguation.
        matched = registry.get_cluster_by_name("foopub01")
        assert matched is not None
        assert {c.name for c in matched} == {"foo_pub_01", "foo_pub01"}

    def test_load_failure_propagates_and_keeps_index_empty(self):
        registry = ClusterRegistry()

        with patch(
            "mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver",
            side_effect=RuntimeError("adminserver unreachable"),
        ):
            with pytest.raises(RuntimeError):
                registry.load()

        assert registry.get_all_clusters() == []

    def test_auto_refresh_updates_index_periodically(self):
        registry = ClusterRegistry()
        call_count = 0
        done = threading.Event()

        def _fake_fetch():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                done.set()
            return [_make_cluster(f"cluster-{call_count}")]

        with patch("mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver", side_effect=_fake_fetch):
            registry.start_auto_refresh(0.02)
            try:
                assert done.wait(timeout=2), "background refresh did not run enough times"
            finally:
                registry.stop_auto_refresh()

        assert call_count >= 3

    def test_auto_refresh_failure_keeps_stale_data_and_reschedules(self):
        registry = ClusterRegistry()
        clusters = [_make_cluster("cluster-a", "alice")]
        call_count = 0
        failed_once = threading.Event()
        recovered = threading.Event()

        def _fake_fetch():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return clusters
            if call_count == 2:
                failed_once.set()
                raise RuntimeError("adminserver blip")
            recovered.set()
            return clusters

        with patch("mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver", side_effect=_fake_fetch):
            registry.start_auto_refresh(0.02)
            try:
                assert failed_once.wait(timeout=2)
                # Stale data from the first successful load must still be there.
                assert registry.get_cluster_by_name("cluster-a") is not None
                assert recovered.wait(timeout=2), "timer did not reschedule after a failure"
            finally:
                registry.stop_auto_refresh()

    def test_stop_auto_refresh_cancels_pending_timer(self):
        registry = ClusterRegistry()
        with patch(
            "mcp_clickhouse.utils_clusters._fetch_clusters_from_adminserver",
            return_value=[_make_cluster()],
        ) as mock_fetch:
            registry.start_auto_refresh(60)
            registry.stop_auto_refresh()
            time.sleep(0.05)
            # Only the initial scheduled call could have fired; cancel prevents any run.
            assert mock_fetch.call_count == 0


class TestCreateClickhouseClientWithClusterName:
    @patch("mcp_clickhouse.mcp_server.get_cluster_registry")
    @patch("mcp_clickhouse.mcp_server.clickhouse_connect")
    def test_known_cluster_name_uses_cluster_config(self, mock_cc, mock_get_registry):
        cluster = _make_cluster("cluster-a", "alice")
        mock_registry = MagicMock()
        mock_registry.get_cluster_by_name.return_value = [cluster]
        mock_get_registry.return_value = mock_registry
        mock_cc.get_client.return_value = MagicMock(server_version="23.8")

        with patch("mcp_clickhouse.mcp_server.get_config") as mock_get_config:
            create_clickhouse_client(cluster_name="cluster-a")

        mock_registry.get_cluster_by_name.assert_called_once_with("cluster-a")
        mock_get_config.assert_not_called()
        call_kwargs = mock_cc.get_client.call_args[1]
        assert call_kwargs["host"] == "cluster-a.example.com"
        assert call_kwargs["username"] == "admin"

    @patch("mcp_clickhouse.mcp_server.get_cluster_registry")
    def test_unknown_cluster_name_raises_tool_error(self, mock_get_registry):
        mock_registry = MagicMock()
        mock_registry.get_cluster_by_name.return_value = None
        mock_get_registry.return_value = mock_registry

        with pytest.raises(ToolError):
            create_clickhouse_client(cluster_name="does-not-exist")

    @patch("mcp_clickhouse.mcp_server.get_cluster_registry")
    def test_ambiguous_cluster_name_raises_tool_error(self, mock_get_registry):
        mock_registry = MagicMock()
        mock_registry.get_cluster_by_name.return_value = [
            _make_cluster("foo_pub_01"),
            _make_cluster("foo_pub01"),
        ]
        mock_get_registry.return_value = mock_registry

        with pytest.raises(ToolError, match="ambiguous"):
            create_clickhouse_client(cluster_name="foopub01")

    @patch("mcp_clickhouse.mcp_server.clickhouse_connect")
    def test_no_cluster_name_uses_default_config(self, mock_cc):
        mock_cc.get_client.return_value = MagicMock(server_version="23.8")

        create_clickhouse_client()

        call_kwargs = mock_cc.get_client.call_args[1]
        assert "host" in call_kwargs


class TestCheckClusterSpecified:
    def test_registry_disabled_never_raises(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_CLUSTER_REGISTRY_ENABLED", "false")
        _check_cluster_specified(None)  # should not raise

    def test_registry_enabled_fallback_mode_does_not_raise(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_CLUSTER_REGISTRY_ENABLED", "true")
        monkeypatch.setenv("CLICKHOUSE_CLUSTER_UNSPECIFIED_MODE", "fallback")
        _check_cluster_specified(None)  # should not raise

    def test_registry_enabled_prompt_mode_raises_without_cluster_name(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_CLUSTER_REGISTRY_ENABLED", "true")
        monkeypatch.setenv("CLICKHOUSE_CLUSTER_UNSPECIFIED_MODE", "prompt")

        with pytest.raises(ToolError):
            _check_cluster_specified(None)

    def test_registry_enabled_prompt_mode_allows_explicit_cluster_name(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_CLUSTER_REGISTRY_ENABLED", "true")
        monkeypatch.setenv("CLICKHOUSE_CLUSTER_UNSPECIFIED_MODE", "prompt")

        _check_cluster_specified("cluster-a")  # should not raise


class TestListClustersTool:
    @patch("mcp_clickhouse.mcp_server.get_cluster_registry")
    def test_filters_by_name(self, mock_get_registry):
        cluster = _make_cluster("cluster-a", "alice")
        mock_registry = MagicMock()
        mock_registry.get_cluster_by_name.return_value = [cluster]
        mock_get_registry.return_value = mock_registry

        result = json.loads(list_clusters(cluster_name="cluster-a", verbose=True))

        assert len(result) == 1
        assert result[0]["name"] == "cluster-a"
        assert "admin_password" not in result[0]

    @patch("mcp_clickhouse.mcp_server.get_cluster_registry")
    def test_filters_by_name_returns_names_by_default(self, mock_get_registry):
        cluster = _make_cluster("cluster-a", "alice")
        mock_registry = MagicMock()
        mock_registry.get_cluster_by_name.return_value = [cluster]
        mock_get_registry.return_value = mock_registry

        result = json.loads(list_clusters(cluster_name="cluster-a"))

        assert result == ["cluster-a"]

    @patch("mcp_clickhouse.mcp_server.get_cluster_registry")
    def test_no_filter_returns_all(self, mock_get_registry):
        mock_registry = MagicMock()
        mock_registry.get_all_clusters.return_value = [
            _make_cluster("cluster-a"),
            _make_cluster("cluster-b"),
        ]
        mock_get_registry.return_value = mock_registry

        result = json.loads(list_clusters())

        mock_registry.get_all_clusters.assert_called_once()
        assert result == ["cluster-a", "cluster-b"]

    @patch("mcp_clickhouse.mcp_server.get_cluster_registry")
    def test_unknown_name_returns_empty_list(self, mock_get_registry):
        mock_registry = MagicMock()
        mock_registry.get_cluster_by_name.return_value = None
        mock_get_registry.return_value = mock_registry

        result = json.loads(list_clusters(cluster_name="does-not-exist"))

        assert result == []
