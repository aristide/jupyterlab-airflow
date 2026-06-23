"""Tests for provider-availability gating (PRD §6.2.1)."""

import pytest

from jupyterlab_airflow import providers, registry
from jupyterlab_airflow.client import AirflowError

# A target index: http installed, snowflake NOT, Airflow 3.0.2.
INDEX = {
    "providers": {"apache-airflow-providers-http": "5.0.0"},
    "airflow_version": "3.0.2",
}


@pytest.fixture(autouse=True)
def _reset():
    providers.reset_cache()
    registry._cache["signature"] = None
    registry._cache["operators"] = None
    registry._notifier_cache["signature"] = None
    registry._notifier_cache["notifiers"] = None
    yield
    providers.reset_cache()


def test_availability_states():
    # Standard provider is never gated.
    assert (
        providers.availability("apache-airflow-providers-standard", "3.0", INDEX)
        == "available"
    )
    # An unspecified provider is treated as bundled/available.
    assert providers.availability("", "3.0", INDEX) == "available"
    # Installed provider -> available.
    assert (
        providers.availability("apache-airflow-providers-http", "3.0", INDEX)
        == "available"
    )
    # Missing provider -> missing-provider.
    assert (
        providers.availability("apache-airflow-providers-snowflake", "3.0", INDEX)
        == "missing-provider"
    )
    # Op needs a newer Airflow than the target runs -> version-too-old.
    assert (
        providers.availability("apache-airflow-providers-standard", "3.1", INDEX)
        == "version-too-old"
    )
    # Target unreachable -> unknown (never blocks).
    assert (
        providers.availability("apache-airflow-providers-snowflake", "3.0", None)
        == "unknown"
    )


def test_version_compare():
    assert providers._version_lt("2.10.5", "3.0") is True
    assert providers._version_lt("3.0.2", "3.0") is False  # 3.0.2 >= 3.0
    assert providers._version_lt("3.0", "3.0.0") is False
    assert providers._version_lt("3.0.0", "3.1") is True


def test_annotate_view_adds_pip_hint_for_missing():
    entries = [
        {"id": "a", "provider": "apache-airflow-providers-snowflake",
         "airflowMinVersion": "3.0"},
        {"id": "b", "provider": "apache-airflow-providers-standard",
         "airflowMinVersion": "3.0"},
        {"id": "c", "provider": "apache-airflow-providers-http",
         "airflowMinVersion": "3.0"},
    ]
    out = {e["id"]: e for e in providers.annotate_view(entries, INDEX)}
    assert out["a"]["availability"] == "missing-provider"
    assert out["a"]["pipInstall"] == "pip install apache-airflow-providers-snowflake"
    assert out["b"]["availability"] == "available" and "pipInstall" not in out["b"]
    assert out["c"]["availability"] == "available"


def test_annotate_view_unknown_when_target_down():
    entries = [{"id": "a", "provider": "apache-airflow-providers-snowflake"}]
    out = providers.annotate_view(entries, None)
    assert out[0]["availability"] == "unknown"
    assert "pipInstall" not in out[0]


def test_availability_third_party_never_blocks(tmp_path):
    # A third-party (off-constraints) op is always `third-party` — independent of
    # whether the package happens to be in the index, and even when the target is
    # unreachable (PRD §6.2.2 ¹ / §13 Q13). It is shown but never gate-blocked.
    assert (
        providers.availability("airflow-provider-great-expectations", "3.0",
                               INDEX, third_party=True)
        == "third-party"
    )
    assert (
        providers.availability("openmetadata-ingestion", "9.9",
                               None, third_party=True)
        == "third-party"
    )
    # The version pin rides the install hint.
    assert (
        providers.pip_install_hint("openmetadata-ingestion", "1.13.0.0")
        == "pip install openmetadata-ingestion==1.13.0.0"
    )


def test_annotate_view_third_party_pins_version():
    entries = [
        {"id": "ge", "provider": "airflow-provider-great-expectations",
         "thirdParty": True, "version": "1.0.0", "airflowMinVersion": "3.0"},
    ]
    # Even with the target reachable and the package NOT in it, a third-party op
    # is `third-party` (not `missing-provider`) with a version-pinned hint.
    out = providers.annotate_view(entries, INDEX)[0]
    assert out["availability"] == "third-party"
    assert out["pipInstall"] == (
        "pip install airflow-provider-great-expectations==1.0.0"
    )


def test_provider_block_errors_skips_third_party(tmp_path, monkeypatch):
    # A third-party op (off-constraints) is NOT hard-blocked even when its package
    # is absent from the target; a normal missing-provider op alongside it still
    # is (so the skip is surgical). /importErrors is the third-party verdict.
    (tmp_path / "tp.yaml").write_text(
        "id: tp\nlabel: GX checkpoint\ncategory: Data Quality\n"
        "provider: airflow-provider-great-expectations\nthird_party: true\n"
        "version: '1.0.0'\nairflow_min_version: '3.0'\n"
    )
    (tmp_path / "snow.yaml").write_text(
        "id: snow\nlabel: Snowflake query\ncategory: SQL\n"
        "provider: apache-airflow-providers-snowflake\nairflow_min_version: '3.0'\n"
    )
    monkeypatch.setenv("AIRFLOW_OPERATORS_DIR", str(tmp_path))
    registry.load_registry(force=True)

    ir = {"nodes": [{"op": "tp"}, {"op": "snow"}]}
    errors = providers.provider_block_errors(ir, INDEX)
    assert len(errors) == 1  # only the normal missing provider blocks
    assert "apache-airflow-providers-snowflake" in errors[0]
    assert all("great-expectations" not in e for e in errors)


def test_provider_block_errors(tmp_path, monkeypatch):
    # A synthetic op on a provider the target lacks.
    (tmp_path / "snow.yaml").write_text(
        "id: snow\nlabel: Snowflake query\ncategory: SQL\n"
        "provider: apache-airflow-providers-snowflake\nairflow_min_version: '3.0'\n"
    )
    monkeypatch.setenv("AIRFLOW_OPERATORS_DIR", str(tmp_path))
    registry.load_registry(force=True)

    ir = {"nodes": [{"op": "snow"}, {"op": "bash"}, {"op": "snow"}]}
    errors = providers.provider_block_errors(ir, INDEX)
    # bash (standard) doesn't block; snow blocks once (deduped).
    assert len(errors) == 1
    assert "apache-airflow-providers-snowflake" in errors[0]
    assert "pip install" in errors[0]

    # Target unreachable -> no block (importErrors stays authoritative).
    assert providers.provider_block_errors(ir, None) == []


def test_provider_block_errors_version_too_old(tmp_path, monkeypatch):
    (tmp_path / "newop.yaml").write_text(
        "id: newop\nlabel: New op\ncategory: X\n"
        "provider: apache-airflow-providers-standard\nairflow_min_version: '3.5'\n"
    )
    monkeypatch.setenv("AIRFLOW_OPERATORS_DIR", str(tmp_path))
    registry.load_registry(force=True)
    errors = providers.provider_block_errors({"nodes": [{"op": "newop"}]}, INDEX)
    assert len(errors) == 1
    assert "Airflow >= 3.5" in errors[0] and "3.0.2" in errors[0]


def test_provider_block_errors_gates_notifier_callbacks():
    # A DAG with Slack + SMTP notifiers on its callbacks; neither provider is in
    # the target index -> both blocked pre-write (PRD §6.8), not just operators.
    ir = {
        "dag": {
            "callbacks": {
                "on_failure": [
                    {"notifier_id": "slack", "params": {"text": "x"}},
                    {"notifier_id": "smtp", "params": {"to": "a@b.com"}},
                ]
            }
        },
        "nodes": [],
    }
    errors = providers.provider_block_errors(ir, INDEX)
    assert len(errors) == 2
    assert any("Notifier" in e and "providers-slack" in e for e in errors)
    assert any("Notifier" in e and "providers-smtp" in e for e in errors)
    # Target unreachable -> no block.
    assert providers.provider_block_errors(ir, None) == []


def test_provider_block_errors_gates_per_task_notifier_callbacks():
    # A per-task callback (node.callbacks, PRD §6.8) on an uninstalled provider is
    # gated pre-write too — not only DAG-level callbacks.
    ir = {
        "dag": {},
        "nodes": [
            {"op": "bash", "callbacks": {
                "on_retry": [{"notifier_id": "slack", "params": {"text": "x"}}]}},
        ],
    }
    errors = providers.provider_block_errors(ir, INDEX)
    assert len(errors) == 1
    assert "Notifier" in errors[0] and "providers-slack" in errors[0]
    assert providers.provider_block_errors(ir, None) == []


class _FakeClient:
    def __init__(self, providers_payload=None, version_payload=None, fail=False):
        self._providers = providers_payload
        self._version = version_payload
        self._fail = fail
        self.provider_calls = 0

    def list_providers(self, limit=1000):
        self.provider_calls += 1
        if self._fail:
            raise AirflowError("unreachable", status=502)
        return self._providers

    def version(self):
        return self._version


def test_get_target_index_caches_and_force_refreshes(monkeypatch):
    fake = _FakeClient(
        providers_payload={
            "providers": [
                {"package_name": "apache-airflow-providers-http", "version": "5.0"}
            ]
        },
        version_payload={"version": "3.0.2"},
    )
    from jupyterlab_airflow import client as client_module

    monkeypatch.setattr(client_module, "get_client", lambda: fake)

    idx = providers.get_target_index(force=True)
    assert idx["airflow_version"] == "3.0.2"
    assert idx["providers"]["apache-airflow-providers-http"] == "5.0"
    assert fake.provider_calls == 1
    # Within TTL: served from cache, no new call.
    providers.get_target_index()
    assert fake.provider_calls == 1
    # force re-reads.
    providers.get_target_index(force=True)
    assert fake.provider_calls == 2


def test_get_target_index_none_when_unreachable(monkeypatch):
    fake = _FakeClient(fail=True)
    from jupyterlab_airflow import client as client_module

    monkeypatch.setattr(client_module, "get_client", lambda: fake)
    assert providers.get_target_index(force=True) is None
