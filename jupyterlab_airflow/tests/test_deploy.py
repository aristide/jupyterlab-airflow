"""Tests for SharedVolumeTarget + deploy_dag (atomic write, provenance safety)."""

import os

import pytest

from jupyterlab_airflow.client import AirflowError
from jupyterlab_airflow.deploy import (
    MANAGED_PREFIX,
    DeployError,
    SharedVolumeTarget,
    deploy_dag,
    rename_preflight,
    retire_old_dag,
)


def _ir(dag_id="dep_dag"):
    return {
        "dag": {"dag_id": dag_id, "schedule": "@daily", "start_date": "2026-01-01"},
        "nodes": [
            {"id": "n", "op": "bash", "task_id": "t",
             "params": {"bash_command": "echo hi"}}
        ],
        "edges": [],
    }


def test_deploy_writes_managed_file_with_provenance(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    res = deploy_dag(_ir(), target=target)
    assert res["deployed"] is True
    assert res["filename"] == "dep_dag.py"
    written = (tmp_path / "dep_dag.py").read_text()
    assert written.startswith(MANAGED_PREFIX)
    # Airflow absent -> a warning, but the deploy still succeeds.
    assert any("skipped" in w.lower() for w in res["warnings"])
    # .airflowignore is dropped covering temp + sidecar globs.
    ignore = (tmp_path / ".airflowignore").read_text().split()
    assert "*.afdag" in ignore and ".afdag-tmp-*" in ignore


def test_deploy_refuses_invalid_graph(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    res = deploy_dag(_ir(dag_id="1bad"), target=target)
    assert res["deployed"] is False
    assert res["errors"]
    assert not list(tmp_path.glob("*.py"))  # nothing written


def test_write_is_atomic_no_temp_left_behind(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    target.write("ok_dag.py", f"{MANAGED_PREFIX}\nx = 1\n")
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith(".afdag-tmp-")]
    assert leftovers == []


def test_refuses_to_overwrite_handwritten_file(tmp_path):
    (tmp_path / "hand.py").write_text("print('hand written, no header')\n")
    target = SharedVolumeTarget(str(tmp_path))
    with pytest.raises(DeployError):
        target.write("hand.py", f"{MANAGED_PREFIX}\nx = 1\n")
    # Original content is untouched.
    assert "hand written" in (tmp_path / "hand.py").read_text()


def test_overwrites_managed_file(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    target.write("m.py", f"{MANAGED_PREFIX}  dag_id=m\nx = 1\n")
    target.write("m.py", f"{MANAGED_PREFIX}  dag_id=m\nx = 2\n")
    assert "x = 2" in (tmp_path / "m.py").read_text()


@pytest.mark.parametrize("bad", ["../evil.py", "/etc/evil.py", "a/b.py", "evil"])
def test_rejects_unsafe_paths(tmp_path, bad):
    target = SharedVolumeTarget(str(tmp_path))
    with pytest.raises(DeployError):
        target.path_for(bad)


def test_list_and_verify(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    deploy_dag(_ir(), target=target)
    listed = target.list()
    assert listed and listed[0]["filename"] == "dep_dag.py"
    assert listed[0]["dag_id"] == "dep_dag"
    assert target.verify("dep_dag.py")
    assert target.verify("dep_dag.py", ir_hash=listed[0]["ir_hash"])
    assert not target.verify("dep_dag.py", ir_hash="sha256:wrong")
    # A hand-written file (no header) is not listed and does not verify.
    (tmp_path / "plain.py").write_text("x = 1\n")
    assert all(item["filename"] != "plain.py" for item in target.list())
    assert target.verify("plain.py") is False


# -- rename migration (PRD §6.1.8(B)) ---------------------------------------


class _FakeClient:
    """Minimal Airflow client stub for rename_preflight / retire_old_dag."""

    def __init__(self, *, registered=True, runs=None):
        self._registered = registered
        self._runs = runs or []
        self.paused = []
        self.deleted = []

    def get_dag(self, dag_id):
        if not self._registered:
            raise AirflowError("not found", status=404)
        return {"dag_id": dag_id}

    def list_dag_runs(self, dag_id, limit=10):
        return {"dag_runs": self._runs}

    def set_paused(self, dag_id, is_paused):
        self.paused.append((dag_id, is_paused))
        return {}

    def delete_dag(self, dag_id):
        self.deleted.append(dag_id)
        return {}


def test_rename_preflight_draft(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "jupyterlab_airflow.client.get_client", lambda: _FakeClient(registered=False)
    )
    out = rename_preflight("draft_dag", SharedVolumeTarget(str(tmp_path)))
    assert out == {
        "dag_id": "draft_dag",
        "file_exists": False,
        "registered": False,
        "active_runs": 0,
    }


def test_rename_preflight_counts_active_runs(monkeypatch, tmp_path):
    fake = _FakeClient(
        registered=True,
        runs=[{"state": "running"}, {"state": "success"}, {"state": "queued"}],
    )
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)
    target = SharedVolumeTarget(str(tmp_path))
    target.write("live_dag.py", f"{MANAGED_PREFIX}  dag_id=live_dag\nx = 1\n")
    out = rename_preflight("live_dag", target)
    assert out["file_exists"] is True
    assert out["registered"] is True
    assert out["active_runs"] == 2


def test_retire_old_dag_keep_history(monkeypatch, tmp_path):
    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)
    target = SharedVolumeTarget(str(tmp_path))
    target.write("old_dag.py", f"{MANAGED_PREFIX}  dag_id=old_dag\nx = 1\n")
    out = retire_old_dag("old_dag", purge=False, target=target)
    assert out["removed_file"] is True
    assert out["paused"] is True
    assert out["purged_history"] is False
    assert not (tmp_path / "old_dag.py").exists()
    assert fake.paused == [("old_dag", True)]
    assert fake.deleted == []  # history kept


def test_retire_old_dag_purge(monkeypatch, tmp_path):
    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)
    target = SharedVolumeTarget(str(tmp_path))
    target.write("gone_dag.py", f"{MANAGED_PREFIX}  dag_id=gone_dag\nx = 1\n")
    out = retire_old_dag("gone_dag", purge=True, target=target)
    assert out["removed_file"] is True
    assert out["purged_history"] is True
    assert fake.deleted == ["gone_dag"]
    assert not (tmp_path / "gone_dag.py").exists()
