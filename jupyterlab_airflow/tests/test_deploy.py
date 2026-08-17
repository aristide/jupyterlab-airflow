"""Tests for SharedVolumeTarget + deploy_dag (atomic write, provenance safety)."""

import os
import stat

import pytest

from jupyterlab_airflow.client import AirflowError
from jupyterlab_airflow.deploy import (
    MANAGED_PREFIX,
    DeployError,
    SharedVolumeTarget,
    deploy_dag,
    drop_retired,
    find_orphans,
    find_source_path,
    is_drifted,
    rename_preflight,
    retire_old_dag,
    rollback_dag,
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


def test_written_file_is_world_readable(tmp_path):
    # mkstemp() forces 0600; the Airflow dag-processor runs as a different uid on
    # a shared volume and must be able to read the file, else the DAG never
    # registers and the deploy hangs on "waiting for Airflow to pick it up".
    target = SharedVolumeTarget(str(tmp_path))
    path = target.write("perm_dag.py", f"{MANAGED_PREFIX}\nx = 1\n")
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode & stat.S_IROTH, f"deployed DAG not other-readable: {oct(mode)}"
    assert mode & stat.S_IRGRP, f"deployed DAG not group-readable: {oct(mode)}"


def test_backup_created_only_on_overwrite_and_rollback_restores(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    v1 = f"{MANAGED_PREFIX}  dag_id=demo  v1\nx = 1\n"
    v2 = f"{MANAGED_PREFIX}  dag_id=demo  v2\nx = 2\n"

    # First write: no prior version, so no backup.
    target.write("demo.py", v1)
    assert target.has_backup("demo.py") is False

    # Overwrite: the prior version is saved as a `.bak` the dag-processor ignores.
    target.write("demo.py", v2)
    assert target.has_backup("demo.py") is True
    assert (tmp_path / "demo.py.bak").exists()
    assert (tmp_path / "demo.py").read_text() == v2

    # Rollback restores the previous version and drops the backup.
    res = rollback_dag("demo", target=target)
    assert res == {"dag_id": "demo", "rolled_back": True, "filename": "demo.py"}
    assert (tmp_path / "demo.py").read_text() == v1
    assert target.has_backup("demo.py") is False

    # No backup left -> rollback is a no-op.
    assert rollback_dag("demo", target=target)["rolled_back"] is False


def test_delete_removes_the_backup_too(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    target.write("demo.py", f"{MANAGED_PREFIX}\nx = 1\n")
    target.write("demo.py", f"{MANAGED_PREFIX}\nx = 2\n")  # creates a backup
    assert target.has_backup("demo.py")
    target.delete("demo.py")
    assert not (tmp_path / "demo.py").exists()
    assert not (tmp_path / "demo.py.bak").exists()


def test_deploy_reports_backed_up_on_re_deploy(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    first = deploy_dag(_ir(), target=target)
    assert first["backed_up"] is False  # nothing to back up yet
    second = deploy_dag(_ir(), target=target)
    assert second["backed_up"] is True  # the first version was saved
    assert target.has_backup("dep_dag.py")


def test_airflowignore_covers_backups(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    target.ensure_airflowignore()
    ignore = (tmp_path / ".airflowignore").read_text().split()
    assert "*.bak" in ignore


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


def test_uncreatable_dags_dir_raises_actionable_error(tmp_path):
    # Parent is a regular file, so the dags dir cannot be created — the deploy
    # must surface an actionable AIRFLOW_DAGS_DIR hint, not a raw [Errno 13].
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("x")
    target = SharedVolumeTarget(str(blocker / "dags"))
    with pytest.raises(DeployError) as exc:
        target.write("my_dag.py", f"{MANAGED_PREFIX}\nx = 1\n")
    assert "AIRFLOW_DAGS_DIR" in str(exc.value)


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
    """Minimal Airflow client stub for rename_preflight / retire_old_dag.

    Models the variables endpoints too (PRD §6.10) because the teardown paths
    reclaim the variables a flow owns — ``variables`` is ``{key: {key, value,
    description}}``, where the description carries the ownership marker.
    """

    def __init__(self, *, registered=True, runs=None, variables=None):
        self._registered = registered
        self._runs = runs or []
        self.paused = []
        self.deleted = []
        self.variables = dict(variables or {})
        self.conns = {}
        self.deleted_variables = []

    def get_dag(self, dag_id):
        if not self._registered:
            raise AirflowError("not found", status=404)
        return {"dag_id": dag_id}

    # The provider-availability gate runs on every deploy; answering it here
    # keeps a test that deploys through this fake independent of whether some
    # earlier test happened to populate the process-wide provider cache.
    def list_providers(self, limit=1000):
        return {
            "providers": [
                {"package_name": "apache-airflow-providers-standard", "version": "1.0"}
            ],
            "total_entries": 1,
        }

    def version(self):
        return {"version": "3.0.2", "git_version": "abc"}

    def list_dag_runs(self, dag_id, limit=10):
        return {"dag_runs": self._runs}

    def set_paused(self, dag_id, is_paused):
        self.paused.append((dag_id, is_paused))
        return {}

    def delete_dag(self, dag_id):
        self.deleted.append(dag_id)
        return {}

    # -- variables ---------------------------------------------------------

    def list_variables(self, limit=1000, offset=0, key_pattern=None):
        return {
            "variables": list(self.variables.values()),
            "total_entries": len(self.variables),
        }

    def get_variable(self, key):
        if key not in self.variables:
            raise AirflowError("not found", status=404)
        return self.variables[key]

    def create_variable(self, key, value, description=None):
        if key in self.variables:
            raise AirflowError("exists", status=409)
        self.variables[key] = {
            "key": key,
            "value": value,
            "description": description,
        }
        return self.variables[key]

    def update_variable(self, key, value, description=None):
        entry = self.variables.setdefault(key, {"key": key})
        entry["value"] = value
        if description is not None:
            entry["description"] = description
        return entry

    def delete_variable(self, key):
        if key not in self.variables:
            raise AirflowError("not found", status=404)
        del self.variables[key]
        self.deleted_variables.append(key)
        return {}

    # -- connections -------------------------------------------------------

    def list_connections(self, limit=1000, offset=0):
        return {
            "connections": list(self.conns.values()),
            "total_entries": len(self.conns),
        }

    def get_connection(self, conn_id):
        if conn_id not in self.conns:
            raise AirflowError("not found", status=404)
        return self.conns[conn_id]

    def create_connection(self, conn_id, conn_type, **fields):
        if conn_id in self.conns:
            raise AirflowError("exists", status=409)
        entry = {"connection_id": conn_id, "conn_type": conn_type}
        entry.update({k: v for k, v in fields.items() if v not in (None, "")})
        self.conns[conn_id] = entry
        return entry

    def update_connection(self, conn_id, conn_type, **fields):
        entry = self.conns.setdefault(conn_id, {"connection_id": conn_id})
        entry["conn_type"] = conn_type
        entry.update({k: v for k, v in fields.items() if v not in (None, "")})
        return entry

    def delete_connection(self, conn_id):
        if conn_id not in self.conns:
            raise AirflowError("not found", status=404)
        del self.conns[conn_id]
        return {}


def test_rename_preflight_draft(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "jupyterlab_airflow.client.get_client", lambda: _FakeClient(registered=False)
    )
    out = rename_preflight("draft_dag", SharedVolumeTarget(str(tmp_path)))
    assert out == {
        "dag_id": "draft_dag",
        "file_exists": False,
        "drifted": False,
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


# -- a retired dag_id leaves the manager list at once (PRD §6.1.8(B)) --------


def _retired_listing():
    return {"dags": [{"dag_id": "hello"}, {"dag_id": "helloo"}], "total_entries": 2}


def test_a_keep_history_retire_hides_the_old_dag_id_from_the_list(
    tmp_path, journaling, monkeypatch
):
    """Rename `hello` → `helloo` and keep the history: only `helloo` may be listed.

    A keep-history retire deletes the `.py` and pauses the DAG, but it must NOT
    delete the DagModel row — that row *is* the history the user chose to keep.
    So Airflow goes on returning BOTH ids from `GET /dags` until its dag-processor
    next scans the folder and marks the now-fileless DAG stale: a full
    `dag_dir_list_interval` (~300 s in the shipped compose), and **never** when
    the deploy target is not the folder that processor scans. The state was
    invisible to every safety net too — `find_orphans` joins on the deployed file,
    which is exactly what the retire just removed — leaving Delete (which purges
    the history) as the only remedy in the UI.
    """
    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)
    target = SharedVolumeTarget(str(tmp_path / "dags"))
    target.write("hello.py", f"{MANAGED_PREFIX}  dag_id=hello\nx = 1\n")
    assert drop_retired(_retired_listing(), target) == _retired_listing()

    retire_old_dag("hello", purge=False, target=target)

    filtered = drop_retired(_retired_listing(), target)
    assert [dag["dag_id"] for dag in filtered["dags"]] == ["helloo"]
    assert filtered["total_entries"] == 1
    # …and the history really is kept: the row was never deleted, only paused.
    assert fake.deleted == []
    assert fake.paused == [("hello", True)]


def test_a_redeployed_dag_id_is_listed_again(tmp_path, journaling, monkeypatch):
    """The suppression can never outlive its cause: deploying that id again — a
    rename back to it, or a new flow claiming the freed name — releases it."""
    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)
    target = SharedVolumeTarget(str(tmp_path / "dags"))
    target.write("hello.py", f"{MANAGED_PREFIX}  dag_id=hello\nx = 1\n")
    retire_old_dag("hello", purge=False, target=target)
    assert journaling.retired_ids() == {"hello"}

    assert deploy_dag(_ir("hello"), target=target)["deployed"] is True

    assert journaling.retired_ids() == set()
    assert len(drop_retired(_retired_listing(), target)["dags"]) == 2


def test_a_marker_is_released_when_a_file_takes_the_id_again(
    tmp_path, journaling, monkeypatch
):
    """Second, independent release: a `{dag_id}.py` exists again (restored from a
    backup, pushed by git, hand-written). The id is live, so it is shown — and the
    stale marker is dropped rather than re-tested on every poll."""
    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)
    target = SharedVolumeTarget(str(tmp_path / "dags"))
    target.write("hello.py", f"{MANAGED_PREFIX}  dag_id=hello\nx = 1\n")
    retire_old_dag("hello", purge=False, target=target)
    target.write("hello.py", f"{MANAGED_PREFIX}  dag_id=hello\nx = 2\n")

    assert len(drop_retired(_retired_listing(), target)["dags"]) == 2
    assert journaling.retired_ids() == set()


def test_an_expired_marker_stops_hiding_the_dag_id(tmp_path, journaling, monkeypatch):
    """The TTL backstop: even if every release path failed, a marker cannot hide
    a dag_id forever."""
    from datetime import timedelta

    from jupyterlab_airflow.journal import utcnow

    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)
    target = SharedVolumeTarget(str(tmp_path / "dags"))
    journaling.mark_retired("hello", now=utcnow() - timedelta(days=2))

    assert len(drop_retired(_retired_listing(), target)["dags"]) == 2
    assert journaling.retired_ids() == set()  # and the marker is gone


def test_a_purge_does_not_need_a_marker(tmp_path, journaling, monkeypatch):
    """`purge=True` deletes the DagModel row itself, so Airflow stops listing the
    id immediately — no suppression needed, and none is recorded."""
    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)
    target = SharedVolumeTarget(str(tmp_path / "dags"))
    target.write("hello.py", f"{MANAGED_PREFIX}  dag_id=hello\nx = 1\n")

    retire_old_dag("hello", purge=True, target=target)

    assert fake.deleted == ["hello"]
    assert journaling.retired_ids() == set()


# -- out-of-band drift detection (PRD §6.5.3) -------------------------------


def test_deploy_stamps_code_hash_in_header(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    deploy_dag(_ir(), target=target)
    first_line = (tmp_path / "dep_dag.py").read_text().splitlines()[0]
    assert "code=sha256:" in first_line


def test_is_drifted_false_for_fresh_deploy(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    deploy_dag(_ir(), target=target)
    assert is_drifted("dep_dag.py", target) is False


def test_is_drifted_true_after_hand_edit(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    deploy_dag(_ir(), target=target)
    path = tmp_path / "dep_dag.py"
    path.write_text(path.read_text() + "\n# hand-edited out of band\n")
    assert is_drifted("dep_dag.py", target) is True


def test_is_drifted_false_without_code_hash(tmp_path):
    # A managed file from before the code-hash feature -> can't tell, no alarm.
    target = SharedVolumeTarget(str(tmp_path))
    target.write("old.py", f"{MANAGED_PREFIX}  dag_id=old\nx = 1\n")
    assert is_drifted("old.py", target) is False


def test_is_drifted_false_for_absent_or_unmanaged(tmp_path):
    target = SharedVolumeTarget(str(tmp_path))
    assert is_drifted("missing.py", target) is False
    (tmp_path / "hand.py").write_text("print('no header')\n")
    assert is_drifted("hand.py", target) is False


def _write_afdag(root, name, afdag_id, dag_id=None):
    import json

    ir = {"provenance": {"afdag_id": afdag_id}}
    if dag_id is not None:
        # Only the superseded join needs the flow's current dag_id; the orphan
        # tests deliberately omit it, which also pins that a source without a
        # readable dag_id never produces a superseded entry.
        ir["dag"] = {"dag_id": dag_id}
    (root / name).write_text(json.dumps(ir))


def test_find_orphans_flags_deployed_with_deleted_source(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("keep.py", f"{MANAGED_PREFIX}  dag_id=keep  afdag_id=AAA\nx=1\n")
    target.write("gone.py", f"{MANAGED_PREFIX}  dag_id=gone  afdag_id=BBB\nx=1\n")
    # Only keep's source .afdag still exists -> gone is an orphan.
    _write_afdag(root, "keep.afdag", "AAA")

    orphans = find_orphans(str(root), target)["orphans"]
    assert [o["dag_id"] for o in orphans] == ["gone"]
    assert orphans[0]["afdag_id"] == "BBB"
    assert orphans[0]["filename"] == "gone.py"


def test_find_orphans_ignores_files_without_afdag_id(tmp_path):
    # A pre-provenance managed file (no afdag_id) can't be re-associated -> never
    # an orphan (we won't auto-delete what we can't match).
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("old.py", f"{MANAGED_PREFIX}  dag_id=old\nx=1\n")
    assert find_orphans(str(root), target)["orphans"] == []


def test_find_orphans_matches_nested_afdag(tmp_path):
    # The source .afdag can live in any subfolder of the Contents root.
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    (root / "sub").mkdir(parents=True)
    target = SharedVolumeTarget(str(dags))
    target.write("d.py", f"{MANAGED_PREFIX}  dag_id=d  afdag_id=CCC\nx=1\n")
    _write_afdag(root / "sub", "anything.afdag", "CCC")
    assert find_orphans(str(root), target)["orphans"] == []


def test_find_orphans_degraded_on_unreadable_afdag(tmp_path):
    # A corrupt/unreadable .afdag has an unknown afdag_id, so the sweep is
    # "degraded" — the caller must not flag a present-but-unreadable source as
    # deleted (§6.5.6). The manager suppresses the prompt when degraded.
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("d.py", f"{MANAGED_PREFIX}  dag_id=d  afdag_id=DDD\nx=1\n")
    (root / "d.afdag").write_text("{ this is not valid json")
    assert find_orphans(str(root), target)["degraded"] is True


def test_find_orphans_not_degraded_when_all_readable(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("d.py", f"{MANAGED_PREFIX}  dag_id=d  afdag_id=EEE\nx=1\n")
    _write_afdag(root, "d.afdag", "EEE")
    res = find_orphans(str(root), target)
    assert res["degraded"] is False
    assert res["orphans"] == []


# -- superseded: renamed flow, old dag_id left deployed (§6.1.8(B) / §15.11) --


def test_find_orphans_flags_superseded_after_rename(tmp_path):
    # The exact HELLO/HELLOP shape: one flow, renamed, both .py files still on
    # disk carrying the SAME live afdag_id. The orphan join cannot see this
    # (nothing is orphaned), which is why it needs its own class.
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("HELLO.py", f"{MANAGED_PREFIX}  dag_id=HELLO  afdag_id=SAME\nx=1\n")
    target.write("HELLOP.py", f"{MANAGED_PREFIX}  dag_id=HELLOP  afdag_id=SAME\nx=1\n")
    _write_afdag(root, "flow.afdag", "SAME", dag_id="HELLO")

    res = find_orphans(str(root), target)
    assert res["orphans"] == []  # the source is alive — nothing is orphaned
    assert [s["dag_id"] for s in res["superseded"]] == ["HELLOP"]
    entry = res["superseded"][0]
    assert entry["current_dag_id"] == "HELLO"
    assert entry["filename"] == "HELLOP.py"
    assert entry["source_path"] == "flow.afdag"


def test_find_orphans_no_superseded_for_current_deploy(tmp_path):
    # The normal case: the deployed dag_id matches the flow's current one.
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("d.py", f"{MANAGED_PREFIX}  dag_id=d  afdag_id=HHH\nx=1\n")
    _write_afdag(root, "d.afdag", "HHH", dag_id="d")
    res = find_orphans(str(root), target)
    assert res["orphans"] == []
    assert res["superseded"] == []


def test_find_orphans_superseded_and_orphan_stay_separate(tmp_path):
    # A deleted source is an orphan; a renamed source is superseded. They must
    # not be conflated — the orphan copy licenses a destructive purge, which
    # would be the wrong remedy for a flow whose source is still there.
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("gone.py", f"{MANAGED_PREFIX}  dag_id=gone  afdag_id=DEAD\nx=1\n")
    target.write("old.py", f"{MANAGED_PREFIX}  dag_id=old  afdag_id=LIVE\nx=1\n")
    _write_afdag(root, "live.afdag", "LIVE", dag_id="new")

    res = find_orphans(str(root), target)
    assert [o["dag_id"] for o in res["orphans"]] == ["gone"]
    assert [s["dag_id"] for s in res["superseded"]] == ["old"]


def test_find_orphans_superseded_needs_both_ids(tmp_path):
    # A source whose dag_id we can't read tells us nothing — stay quiet rather
    # than flag a file that may be perfectly current.
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("d.py", f"{MANAGED_PREFIX}  dag_id=d  afdag_id=III\nx=1\n")
    _write_afdag(root, "d.afdag", "III")  # no dag.dag_id
    assert find_orphans(str(root), target)["superseded"] == []


# -- deploy collision guard (PRD §6.5.3) --------------------------------------


def _owned_ir(dag_id, afdag_id):
    ir = _ir(dag_id=dag_id)
    ir["provenance"] = {"afdag_id": afdag_id}
    return ir


def test_deploy_refused_when_dag_id_owned_by_another_flow(tmp_path):
    # Two .afdag documents picking the same dag_id both map to {dag_id}.py, so
    # every filename-scoped check passes and the second silently overwrites the
    # first. This is the gate that stops it.
    target = SharedVolumeTarget(str(tmp_path))
    assert deploy_dag(_owned_ir("dep_dag", "FLOW-A"), target=target)["deployed"] is True

    res = deploy_dag(_owned_ir("dep_dag", "FLOW-B"), target=target)
    assert res["deployed"] is False
    assert any("different flow" in e for e in res["errors"])
    # Refused before anything is touched: the first flow's file is intact and no
    # backup was taken (a backup would mean the write had started).
    assert "afdag_id=FLOW-A" in (tmp_path / "dep_dag.py").read_text()
    assert not list(tmp_path.glob("*.bak"))


def test_deploy_allows_reploy_of_own_dag_id(tmp_path):
    # The normal path: same flow, same id, re-deployed. Must be unaffected.
    target = SharedVolumeTarget(str(tmp_path))
    assert deploy_dag(_owned_ir("dep_dag", "FLOW-A"), target=target)["deployed"] is True
    res = deploy_dag(_owned_ir("dep_dag", "FLOW-A"), target=target)
    assert res["deployed"] is True
    assert res["backed_up"] is True


def test_deploy_allows_overwriting_pre_provenance_file(tmp_path):
    # A managed file with no afdag_id can't be attributed. Blocking would strand
    # pre-provenance flows with no route forward, and the overwrite is
    # recoverable via the backup — so we allow it, as find_orphans does.
    target = SharedVolumeTarget(str(tmp_path))
    target.write("dep_dag.py", f"{MANAGED_PREFIX}  dag_id=dep_dag\nx=1\n")
    res = deploy_dag(_owned_ir("dep_dag", "FLOW-B"), target=target)
    assert res["deployed"] is True


def test_deploy_refused_when_airflow_serves_dag_id_from_another_file(monkeypatch, tmp_path):
    # The file check is filename-scoped, so it cannot see a hand-written DAG that
    # declares the same dag_id from some other file. Airflow can.
    class _Serving(_FakeClient):
        def get_dag(self, dag_id):
            return {"dag_id": dag_id, "fileloc": "/opt/airflow/dags/handwritten.py"}

    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: _Serving())
    target = SharedVolumeTarget(str(tmp_path))
    res = deploy_dag(_owned_ir("dep_dag", "FLOW-A"), target=target)
    assert res["deployed"] is False
    assert any("handwritten.py" in e for e in res["errors"])
    assert not list(tmp_path.glob("*.py"))  # nothing written


def test_deploy_not_refused_when_airflow_serves_it_from_our_file(monkeypatch, tmp_path):
    class _Serving(_FakeClient):
        def get_dag(self, dag_id):
            return {"dag_id": dag_id, "fileloc": f"/opt/airflow/dags/{dag_id}.py"}

    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: _Serving())
    target = SharedVolumeTarget(str(tmp_path))
    assert deploy_dag(_owned_ir("dep_dag", "FLOW-A"), target=target)["deployed"] is True


def test_deploy_not_refused_when_airflow_unreachable(monkeypatch, tmp_path):
    # An infrastructure blip must never block a deploy — same contract as the
    # variables/provider gates. /importErrors stays the post-deploy verdict.
    class _Down(_FakeClient):
        def get_dag(self, dag_id):
            raise AirflowError("connection refused", status=None)

    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: _Down())
    target = SharedVolumeTarget(str(tmp_path))
    assert deploy_dag(_owned_ir("dep_dag", "FLOW-A"), target=target)["deployed"] is True


def test_deploy_new_flow_without_afdag_id_still_deploys(tmp_path):
    # A brand-new flow that has never deployed has no provenance yet; a clean
    # dags folder must not be treated as a conflict.
    target = SharedVolumeTarget(str(tmp_path))
    assert deploy_dag(_ir(dag_id="dep_dag"), target=target)["deployed"] is True


def test_deploy_new_flow_refused_against_owned_file(tmp_path):
    # ...but it still cannot claim an id a named owner already holds. This is the
    # likeliest collision in practice: a fresh flow picking a name in use.
    target = SharedVolumeTarget(str(tmp_path))
    target.write("dep_dag.py", f"{MANAGED_PREFIX}  dag_id=dep_dag  afdag_id=FLOW-A\nx=1\n")
    res = deploy_dag(_ir(dag_id="dep_dag"), target=target)
    assert res["deployed"] is False
    assert any("different flow" in e for e in res["errors"])


def test_find_source_path_resolves_by_filename(tmp_path):
    # "Open in Studio to fix" (§7): a deployed file -> its source `.afdag` path,
    # Contents-relative, even when the source lives in a subfolder.
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    (root / "sub").mkdir(parents=True)
    target = SharedVolumeTarget(str(dags))
    target.write("d.py", f"{MANAGED_PREFIX}  dag_id=d  afdag_id=FFF\nx=1\n")
    _write_afdag(root / "sub", "d.afdag", "FFF")
    res = find_source_path(filename="d.py", contents_root=str(root), target=target)
    assert res["path"] == "sub/d.afdag"


def test_find_source_path_resolves_by_dag_id(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("d.py", f"{MANAGED_PREFIX}  dag_id=mydag  afdag_id=GGG\nx=1\n")
    _write_afdag(root, "d.afdag", "GGG")
    res = find_source_path(dag_id="mydag", contents_root=str(root), target=target)
    assert res["path"] == "d.afdag"


def test_find_source_path_none_when_source_deleted(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    target.write("d.py", f"{MANAGED_PREFIX}  dag_id=d  afdag_id=HHH\nx=1\n")
    # No matching .afdag under root.
    res = find_source_path(filename="d.py", contents_root=str(root), target=target)
    assert res["path"] is None


def test_find_source_path_none_for_pre_provenance_deploy(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    root = tmp_path / "workspace"
    root.mkdir()
    target = SharedVolumeTarget(str(dags))
    # A managed file with no afdag_id can't be re-associated.
    target.write("d.py", f"{MANAGED_PREFIX}  dag_id=d\nx=1\n")
    res = find_source_path(filename="d.py", contents_root=str(root), target=target)
    assert res["path"] is None


# --------------------------------------------------------------------------- #
# GitDeployTarget (PRD §6.5.1 / §8.7) — verified against a real local git repo.
# --------------------------------------------------------------------------- #
import subprocess  # noqa: E402

from jupyterlab_airflow.deploy import (  # noqa: E402
    GitDeployTarget,
    get_deploy_target,
    purge_dag,
)

MANAGED = "# airflow-studio: managed  dag_id=demo  afdag_id=abc\nprint('v1')\n"


def _git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)


def _init_repo(path, bare=False):
    args = ["git", "init", "-b", "main"] + (["--bare"] if bare else []) + [str(path)]
    subprocess.run(args, capture_output=True)
    if not bare:
        _git(path, "-c", "user.email=t@t", "-c", "user.name=t",
             "commit", "--allow-empty", "-m", "init")
    return path


def _git_target(tmp_path, **kw):
    repo = _init_repo(tmp_path / "repo")
    return GitDeployTarget(repo=str(repo), subdir="dags", branch="main", **kw)


def test_git_target_commits_writes_with_provenance(tmp_path):
    t = _git_target(tmp_path)
    path = t.write("demo.py", MANAGED)
    assert os.path.isfile(path)
    assert t.exists("demo.py") and "print('v1')" in t.read("demo.py")
    # Committed: the last commit is the deploy, and HEAD has the file content.
    log = _git(tmp_path / "repo", "log", "--oneline").stdout
    assert "airflow-studio: deploy demo.py" in log.splitlines()[0]
    assert "print('v1')" in _git(tmp_path / "repo", "show", "HEAD:dags/demo.py").stdout
    # list() reads the working tree like the shared volume.
    listed = t.list()
    assert listed == [{"filename": "demo.py", "dag_id": "demo", "afdag_id": "abc"}]


def test_git_target_pushes_when_remote_configured(tmp_path):
    bare = _init_repo(tmp_path / "remote.git", bare=True)
    repo = _init_repo(tmp_path / "repo")
    _git(repo, "remote", "add", "origin", str(bare))
    _git(repo, "push", "origin", "main")
    t = GitDeployTarget(repo=str(repo), subdir="dags", branch="main", remote="origin")
    t.write("demo.py", MANAGED)
    # The deploy commit reached the bare remote.
    assert "deploy demo.py" in _git(bare, "log", "--oneline").stdout.splitlines()[0]


def test_git_target_backup_untracked_and_rollback_commits(tmp_path):
    t = _git_target(tmp_path)
    t.write("demo.py", MANAGED)
    t.write("demo.py", MANAGED.replace("v1", "v2"))
    assert t.has_backup("demo.py")
    # The backup is NOT committed (only the .py is tracked).
    tracked = _git(tmp_path / "repo", "ls-files", "dags/").stdout.split()
    assert "dags/demo.py" in tracked and "dags/demo.py.bak" not in tracked
    # Rollback restores v1 and commits it.
    assert rollback_dag("demo", target=t)["rolled_back"]
    assert "print('v1')" in t.read("demo.py")
    assert "roll back demo.py" in _git(tmp_path / "repo", "log", "--oneline").stdout.splitlines()[0]


def test_git_target_delete_commits_removal(tmp_path):
    t = _git_target(tmp_path)
    t.write("demo.py", MANAGED)
    t.delete("demo.py")
    assert not t.exists("demo.py")
    log = _git(tmp_path / "repo", "log", "--oneline").stdout
    assert "undeploy demo.py" in log.splitlines()[0]
    # Gone from HEAD.
    assert _git(tmp_path / "repo", "show", "HEAD:dags/demo.py").returncode != 0


def test_git_purge_does_not_mask_wrong_branch_failure(monkeypatch, tmp_path):
    # An operational (wrong-branch) failure in GitDeployTarget.delete must NOT be
    # swallowed as a successful purge: the .py is still live, so purge_dag must
    # raise AND must not go on to purge history (regression for the
    # purge/retire DeployError-swallow bug — only unsafe filenames are skipped).
    repo = _init_repo(tmp_path / "repo")
    t = GitDeployTarget(repo=str(repo), subdir="dags", branch="main")
    t.write("demo.py", MANAGED)
    _git(repo, "checkout", "-b", "other")  # move off the configured branch

    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)

    with pytest.raises(DeployError, match="configured for branch"):
        purge_dag("demo", target=t)
    assert t.exists("demo.py")  # file still live — not falsely removed
    assert fake.deleted == []  # history NOT purged after the failed file removal


def test_retire_does_not_mask_wrong_branch_failure(monkeypatch, tmp_path):
    # Same guarantee for the rename-retire path (purge=False).
    repo = _init_repo(tmp_path / "repo")
    t = GitDeployTarget(repo=str(repo), subdir="dags", branch="main")
    t.write("demo.py", MANAGED)
    _git(repo, "checkout", "-b", "other")

    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)

    with pytest.raises(DeployError, match="configured for branch"):
        retire_old_dag("demo", purge=False, target=t)
    assert t.exists("demo.py")
    assert fake.paused == []  # not falsely paused after the failed file removal


def test_purge_skips_file_for_unmanaged_dag_id_but_purges_history(monkeypatch, tmp_path):
    # An unsafe/unmanaged dag_id (not a Studio-managed `<id>.py`) skips ONLY the
    # file step and still purges history — the legitimately-narrowed path.
    fake = _FakeClient()
    monkeypatch.setattr("jupyterlab_airflow.client.get_client", lambda: fake)
    out = purge_dag("weird-name", target=SharedVolumeTarget(str(tmp_path)))
    assert out["removed_file"] is False
    assert out["purged_history"] is True
    assert fake.deleted == ["weird-name"]


def test_git_target_requires_a_repo(tmp_path):
    t = GitDeployTarget(repo="", subdir="dags")
    with pytest.raises(DeployError, match="AIRFLOW_GIT_DAGS_REPO"):
        t.write("demo.py", MANAGED)


def test_git_target_rejects_non_git_dir(tmp_path):
    plain = tmp_path / "plain"
    plain.mkdir()
    t = GitDeployTarget(repo=str(plain), subdir="dags")
    with pytest.raises(DeployError, match="not a git repository"):
        t.write("demo.py", MANAGED)


def test_git_target_inherits_collision_safety(tmp_path):
    repo = _init_repo(tmp_path / "repo")
    (repo / "dags").mkdir()
    (repo / "dags" / "hand.py").write_text("print('hand-written')\n")
    t = GitDeployTarget(repo=str(repo), subdir="dags")
    with pytest.raises(DeployError, match="not a Studio-managed"):
        t.write("hand.py", MANAGED)


def test_get_deploy_target_factory(monkeypatch, tmp_path):
    monkeypatch.delenv("AIRFLOW_DEPLOY_TARGET", raising=False)
    assert type(get_deploy_target()).__name__ == "SharedVolumeTarget"
    monkeypatch.setenv("AIRFLOW_DEPLOY_TARGET", "git")
    monkeypatch.setenv("AIRFLOW_GIT_DAGS_REPO", str(tmp_path / "repo"))
    assert type(get_deploy_target()).__name__ == "GitDeployTarget"


def test_deploy_dag_through_git_target(tmp_path):
    # End-to-end: deploy_dag validates then writes+commits via an injected git target.
    t = _git_target(tmp_path)
    res = deploy_dag(_ir("gitdag"), target=t)
    assert res["deployed"], res["errors"]
    assert t.exists("gitdag.py")
    assert "deploy gitdag.py" in _git(tmp_path / "repo", "log", "--oneline").stdout.splitlines()[0]


# --- Regression tests for the GitDeployTarget adversarial-review findings ----- #
def test_git_target_refuses_wrong_branch(tmp_path):
    # HIGH: a commit on the current branch + push HEAD:<branch> would silently
    # cross-wire branches. The target must refuse when HEAD != the configured
    # branch (no file written, no commit) rather than diverge the bundle branch.
    repo = _init_repo(tmp_path / "repo")
    _git(repo, "checkout", "-b", "feature")
    t = GitDeployTarget(repo=str(repo), subdir="dags", branch="main")
    with pytest.raises(DeployError, match="configured for branch 'main'"):
        t.write("demo.py", MANAGED)
    assert not (repo / "dags" / "demo.py").exists()  # nothing leaked onto feature


def test_git_target_commit_is_path_scoped(tmp_path):
    # HIGH (security): `git commit` with no pathspec sweeps the WHOLE index. A
    # deploy must commit only its own files, never unrelated/secret pre-staged work.
    repo = _init_repo(tmp_path / "repo")
    (repo / "secret.env").write_text("API_KEY=supersecret\n")
    _git(repo, "add", "secret.env")  # unrelated, pre-staged
    t = GitDeployTarget(repo=str(repo), subdir="dags", branch="main")
    t.write("demo.py", MANAGED)
    touched = _git(repo, "show", "--name-only", "--format=", "HEAD").stdout.split()
    assert "dags/demo.py" in touched and "secret.env" not in touched, touched
    # The secret is left staged (untouched), not committed.
    assert _git(repo, "diff", "--cached", "--name-only").stdout.strip() == "secret.env"


def test_git_target_push_failure_rolls_back_commit(tmp_path):
    # HIGH: a rejected push must not leave the repo ahead/divergent, and retries
    # must not stack commits.
    bare = _init_repo(tmp_path / "remote.git", bare=True)
    repo = _init_repo(tmp_path / "repo")
    _git(repo, "remote", "add", "origin", str(bare))
    _git(repo, "push", "origin", "main")
    # Advance the remote from a second clone so our push is non-fast-forward.
    clone = tmp_path / "clone"
    subprocess.run(["git", "clone", str(bare), str(clone)], capture_output=True)
    _git(clone, "-c", "user.email=x@x", "-c", "user.name=x", "commit", "--allow-empty", "-m", "adv")
    _git(clone, "push", "origin", "main")
    t = GitDeployTarget(repo=str(repo), subdir="dags", branch="main", remote="origin")
    before = _git(repo, "rev-list", "--count", "HEAD").stdout.strip()
    with pytest.raises(DeployError, match="rolled back"):
        t.write("demo.py", MANAGED)
    assert _git(repo, "rev-list", "--count", "HEAD").stdout.strip() == before  # rolled back
    with pytest.raises(DeployError):
        t.write("demo.py", MANAGED)  # retry
    assert _git(repo, "rev-list", "--count", "HEAD").stdout.strip() == before  # no stacking


def test_git_target_gitignores_backups(tmp_path):
    # LOW: the rollback .bak must not pollute `git status` (and can never be staged).
    t = _git_target(tmp_path)
    t.write("demo.py", MANAGED)
    t.write("demo.py", MANAGED.replace("v1", "v2"))  # overwrite -> creates a .bak
    assert "*.bak" in (tmp_path / "repo" / "dags" / ".gitignore").read_text()
    assert ".bak" not in _git(tmp_path / "repo", "status", "--porcelain").stdout


# --------------------------------------------------------------------------- #
# S3DeployTarget (PRD §6.5.1 / §8.7) — exercised against a faithful in-memory S3
# client whose method/response shapes match the botocore S3 service model
# (PutObject/GetObject/ListObjectsV2/DeleteObject/HeadObject), since boto3 is not
# installed here. The live MinIO/S3 deploy is the env-gated step (like Git).
# --------------------------------------------------------------------------- #
from jupyterlab_airflow.deploy import S3DeployTarget, get_deploy_target  # noqa: E402


class _S3ClientError(Exception):
    """A botocore-shaped ClientError (404) for a missing key."""

    def __init__(self, code="404"):
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class FakeS3:
    """Minimal faithful S3 client: keys are (Bucket, Key) -> bytes; ListObjectsV2
    paginates with a small page size to exercise the pagination loop."""

    def __init__(self, page=2):
        self.store = {}
        self.page = page

    def put_object(self, Bucket, Key, Body):
        self.store[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.store:
            raise _S3ClientError("NoSuchKey")
        return {"Body": _Body(self.store[(Bucket, Key)])}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.store:
            raise _S3ClientError("404")
        return {}

    def delete_object(self, Bucket, Key):
        self.store.pop((Bucket, Key), None)  # S3 delete of a missing key is a no-op

    def list_objects_v2(self, Bucket, Prefix="", ContinuationToken=None):
        keys = sorted(k for (b, k) in self.store if b == Bucket and k.startswith(Prefix))
        start = int(ContinuationToken) if ContinuationToken else 0
        chunk = keys[start : start + self.page]
        nxt = start + self.page
        out = {"Contents": [{"Key": k} for k in chunk], "IsTruncated": nxt < len(keys)}
        if out["IsTruncated"]:
            out["NextContinuationToken"] = str(nxt)
        return out


def _s3_target(page=2):
    fake = FakeS3(page=page)
    return S3DeployTarget(bucket="dagbucket", prefix="dags", client=fake), fake


def test_s3_target_puts_object_with_provenance_and_path():
    t, fake = _s3_target()
    path = t.write("demo.py", MANAGED)
    assert path == "s3://dagbucket/dags/demo.py"
    assert ("dagbucket", "dags/demo.py") in fake.store
    assert t.exists("demo.py") and "print('v1')" in t.read("demo.py")
    assert t.list() == [{"filename": "demo.py", "dag_id": "demo", "afdag_id": "abc"}]


def test_s3_target_overwrite_backs_up_and_rollback_restores():
    t, fake = _s3_target()
    t.write("demo.py", MANAGED)
    t.write("demo.py", MANAGED.replace("v1", "v2"))
    assert t.has_backup("demo.py")
    assert ("dagbucket", "dags/demo.py.bak") in fake.store
    assert "print('v2')" in t.read("demo.py")
    assert rollback_dag("demo", target=t)["rolled_back"]
    assert "print('v1')" in t.read("demo.py")
    assert not t.has_backup("demo.py")  # backup consumed by the rollback


def test_s3_target_refuses_non_managed_object():
    t, fake = _s3_target()
    fake.store[("dagbucket", "dags/hand.py")] = b"print('hand-written')\n"
    with pytest.raises(DeployError, match="not a Studio-managed"):
        t.write("hand.py", MANAGED)


def test_s3_target_list_paginates_and_skips_nested_and_unmanaged():
    t, fake = _s3_target(page=2)
    for i in range(3):
        t.write(f"d{i}.py", f"# airflow-studio: managed  dag_id=d{i}\nprint({i})\n")
    fake.store[("dagbucket", "dags/notes.txt")] = b"x"  # non-.py
    fake.store[("dagbucket", "dags/nested/sub.py")] = b"# airflow-studio: managed\nx\n"  # nested
    fake.store[("dagbucket", "dags/hand.py")] = b"print('no header')\n"  # unmanaged
    names = sorted(e["filename"] for e in t.list())
    assert names == ["d0.py", "d1.py", "d2.py"]  # paginated, nested/non-.py/unmanaged skipped


def test_s3_target_delete_removes_object_and_backup():
    t, fake = _s3_target()
    t.write("demo.py", MANAGED)
    t.write("demo.py", MANAGED.replace("v1", "v2"))  # makes a .bak
    t.delete("demo.py")
    assert not t.exists("demo.py")
    assert ("dagbucket", "dags/demo.py.bak") not in fake.store


def test_s3_target_ensure_airflowignore_get_modify_put():
    t, fake = _s3_target()
    t.ensure_airflowignore()
    ignore = fake.store[("dagbucket", "dags/.airflowignore")].decode().split()
    assert "*.afdag" in ignore and "*.bak" in ignore
    # Idempotent: a second call doesn't duplicate.
    t.ensure_airflowignore()
    again = fake.store[("dagbucket", "dags/.airflowignore")].decode().split()
    assert again.count("*.afdag") == 1


def test_s3_target_rejects_unsafe_filename():
    t, _ = _s3_target()
    for bad in ("../evil.py", "a/b.py", "evil"):
        with pytest.raises(DeployError, match="Unsafe filename"):
            t.exists(bad)


def test_s3_target_requires_a_bucket():
    t = S3DeployTarget(bucket="", prefix="dags", client=None)
    with pytest.raises(DeployError, match="AIRFLOW_S3_DAGS_BUCKET"):
        t.write("demo.py", MANAGED)


def test_s3_factory_selection(monkeypatch):
    monkeypatch.setenv("AIRFLOW_DEPLOY_TARGET", "s3")
    monkeypatch.setenv("AIRFLOW_S3_DAGS_BUCKET", "b")
    assert type(get_deploy_target()).__name__ == "S3DeployTarget"


def test_deploy_dag_through_s3_target():
    t, fake = _s3_target()
    res = deploy_dag(_ir("s3dag"), target=t)
    assert res["deployed"], res["errors"]
    assert t.exists("s3dag.py")
    assert ("dagbucket", "dags/s3dag.py") in fake.store


def test_s3_target_missing_bucket_surfaces_not_masked():
    # A missing/mistyped bucket (NoSuchBucket) must NOT be masked as a missing
    # key, so delete()/purge don't silently report success (review finding).
    class _NoBucket(FakeS3):
        def delete_object(self, Bucket, Key):
            raise _S3ClientError("NoSuchBucket")
        def head_object(self, Bucket, Key):
            raise _S3ClientError("NoSuchBucket")
    t = S3DeployTarget(bucket="missing", prefix="dags", client=_NoBucket())
    assert S3DeployTarget._is_not_found(_S3ClientError("NoSuchBucket")) is False
    assert S3DeployTarget._is_not_found(_S3ClientError("NoSuchKey")) is True
    with pytest.raises(Exception) as exc:  # the NoSuchBucket error surfaces
        t.delete("demo.py")
    assert not isinstance(exc.value, DeployError) or "NoSuchBucket" in str(exc.value)


def test_s3_target_list_propagates_unexpected_error_but_skips_vanished():
    t, fake = _s3_target()
    t.write("a.py", MANAGED)
    t.write("b.py", MANAGED)

    # An object that vanished between list and get (404) is skipped, not fatal.
    class _Vanish(FakeS3):
        def __init__(self, inner):
            self.store = inner.store; self.page = inner.page; self._miss = "dags/b.py"
        def get_object(self, Bucket, Key):
            if Key == self._miss:
                raise _S3ClientError("NoSuchKey")
            return super().get_object(Bucket, Key)
    t._client = _Vanish(fake)
    assert [e["filename"] for e in t.list()] == ["a.py"]  # b.py skipped, no raise

    # A non-not-found error (e.g. 403 AccessDenied) on one object propagates
    # rather than silently dropping a managed DAG.
    class _Denied(FakeS3):
        def __init__(self, inner):
            self.store = inner.store; self.page = inner.page
        def get_object(self, Bucket, Key):
            err = _S3ClientError("AccessDenied"); err.response["Error"]["Code"] = "AccessDenied"
            err.response["ResponseMetadata"]["HTTPStatusCode"] = 403
            raise err
    t._client = _Denied(fake)
    with pytest.raises(Exception):
        t.list()


def test_deploy_stamps_correlation_id_in_header_and_returns_it(tmp_path):
    # The deploy correlation_id (PRD §8.9/§10) is stamped into the `.py` header
    # and returned, so the deploy's audit record can carry the same trace id.
    import re as _re

    from jupyterlab_airflow.deploy import _parse_header

    target = SharedVolumeTarget(str(tmp_path))
    res = deploy_dag(_ir("cid_dag"), target=target)
    assert res["deployed"]
    cid = res["correlation_id"]
    assert _re.fullmatch(r"[0-9a-f]{32}", cid)  # a uuid4 hex
    header = _parse_header((tmp_path / "cid_dag.py").read_text())
    assert header["correlation_id"] == cid
    # The body hash (drift) is unaffected — the cid only changes the header line.
    assert header["code"].startswith("sha256:")
    assert is_drifted("cid_dag.py", target) is False
    # generate_dag stays deterministic; the per-deploy id is stamped at deploy.
    res2 = deploy_dag(_ir("cid_dag"), target=target)
    assert res2["correlation_id"] != cid  # a fresh id each deploy
    body1 = (tmp_path / "cid_dag.py.bak").read_text().split("\n", 1)[1]
    body2 = (tmp_path / "cid_dag.py").read_text().split("\n", 1)[1]
    assert body1 == body2  # identical body across deploys despite different cid


def test_rejected_deploy_returns_correlation_id_without_writing(tmp_path):
    # Even a refused deploy returns a correlation_id (so its audit has a trace id),
    # but writes nothing — no header to stamp.
    import re as _re

    target = SharedVolumeTarget(str(tmp_path))
    res = deploy_dag(_ir("1bad"), target=target)  # invalid dag_id → refused
    assert res["deployed"] is False
    assert _re.fullmatch(r"[0-9a-f]{32}", res["correlation_id"])
    assert not list(tmp_path.glob("*.py"))


# --------------------------------------------------------------------------- #
# The durable deploy lifecycle (PRD §6.5.4): a deploy journals the work the
# editor used to perform, so the server can finish it after the tab closes.
# --------------------------------------------------------------------------- #
@pytest.fixture
def journaling(monkeypatch, tmp_path):
    """Turn journaling on for one test, into its own directory."""
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_RECONCILER", "on")
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_JOURNAL_DIR", str(tmp_path / "journal"))
    from jupyterlab_airflow.journal import get_journal

    return get_journal()


_LIFECYCLE = {"retire": None, "run_on_deploy": True}


def test_a_deploy_journals_its_remaining_lifecycle(tmp_path, journaling):
    target = SharedVolumeTarget(str(tmp_path / "dags"))
    res = deploy_dag(_ir("jrn_dag"), target=target, requested_by="aristide",
                     lifecycle={"retire": {"dag_id": "jrn_old", "purge": False},
                                "run_on_deploy": True})
    assert res["deployed"] is True
    assert res["lifecycle"]["reconciled"] is True
    entry = journaling.get(res["correlation_id"])
    # One id ties the entry, the response, the audit record and the `.py` header.
    assert entry["deploy_id"] == res["correlation_id"] == res["lifecycle"]["deploy_id"]
    assert entry["run_id"] == f"studio__{res['correlation_id']}" == res["lifecycle"]["run_id"]
    assert entry["retire"] == {"dag_id": "jrn_old", "purge": False}
    assert entry["user"] == "aristide" and entry["role_at_deploy"] == "editor"
    assert entry["phase"] == "awaiting_registration"


def test_a_failed_write_leaves_no_journal_entry(tmp_path, journaling, monkeypatch):
    # Write-then-journal: the entry can never describe a file that isn't there.
    target = SharedVolumeTarget(str(tmp_path / "dags"))

    def _boom(filename, content):
        raise DeployError("disk on fire")

    monkeypatch.setattr(target, "write", _boom)
    with pytest.raises(DeployError):
        deploy_dag(_ir("jrn_fail"), target=target, lifecycle=_LIFECYCLE)
    assert journaling.list_pending() == []


def test_a_bare_ir_client_is_never_journaled(tmp_path, journaling):
    # A browser tab from before the server upgrade still performs the steps
    # itself; journaling would give one deploy two performers.
    target = SharedVolumeTarget(str(tmp_path / "dags"))
    res = deploy_dag(_ir("jrn_legacy"), target=target)  # lifecycle=None
    assert res["deployed"] is True
    assert res["lifecycle"]["reconciled"] is False
    assert journaling.list_pending() == []


def test_the_kill_switch_disables_journaling(tmp_path, journaling, monkeypatch):
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_RECONCILER", "off")
    target = SharedVolumeTarget(str(tmp_path / "dags"))
    res = deploy_dag(_ir("jrn_off"), target=target, lifecycle=_LIFECYCLE)
    assert res["deployed"] is True
    assert res["lifecycle"]["reconciled"] is False
    assert journaling.list_pending() == []


def test_an_unwritable_journal_warns_but_never_fails_the_deploy(tmp_path, monkeypatch):
    # Degradation is always to yesterday's behaviour, never to a lost deploy.
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_RECONCILER", "on")
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_JOURNAL_DIR", str(tmp_path / "journal"))
    from jupyterlab_airflow import journal as journal_mod

    def _boom(entry):
        raise OSError("read-only file system")

    monkeypatch.setattr(journal_mod.Journal, "put", _boom)
    target = SharedVolumeTarget(str(tmp_path / "dags"))
    res = deploy_dag(_ir("jrn_ro"), target=target, lifecycle=_LIFECYCLE)
    assert res["deployed"] is True
    assert res["lifecycle"]["reconciled"] is False
    assert any("could not be journaled" in w for w in res["warnings"])


def test_retire_with_expect_afdag_id_skips_a_stranger(tmp_path, monkeypatch):
    # The guard that makes a DELAYED retire safe: the dag_id was freed by a
    # rename and taken by another flow before the retire ran.
    from jupyterlab_airflow import client as client_module

    calls = []

    class _Client:
        def set_paused(self, dag_id, is_paused):
            calls.append((dag_id, is_paused))
            return {}

    monkeypatch.setattr(client_module, "get_client", lambda: _Client())
    target = SharedVolumeTarget(str(tmp_path))
    stranger = f"{MANAGED_PREFIX}  dag_id=old_dag  afdag_id=afd_them\nx = 1\n"
    target.write("old_dag.py", stranger)

    res = retire_old_dag("old_dag", purge=False, target=target, expect_afdag_id="afd_us")

    assert res["skipped_reason"] == "file now owned by another flow"
    assert (tmp_path / "old_dag.py").read_text() == stranger  # not deleted
    assert calls == []  # and not paused either


def _retire_client(monkeypatch, calls):
    from jupyterlab_airflow import client as client_module

    class _Client:
        def set_paused(self, dag_id, is_paused):
            calls.append(("set_paused", dag_id, is_paused))
            return {}

        def delete_dag(self, dag_id):
            calls.append(("delete_dag", dag_id))
            return {}

        def list_variables(self, limit=1000, offset=0, key_pattern=None):
            return {"variables": [], "total_entries": 0}

        def list_connections(self, limit=1000, offset=0):
            return {"connections": [], "total_entries": 0}

    monkeypatch.setattr(client_module, "get_client", lambda: _Client())
    return calls


def test_retire_allows_a_file_whose_header_predates_afdag_id(tmp_path, monkeypatch):
    """`afdag_id=` was only added to the generated header by the rename-migration
    change. A DAG deployed before that has a header without the token, and the
    ownership guard used to read that as "another flow owns it" — refusing the
    retire (delete AND pause) and telling the user a file that was theirs all
    along belonged to someone else. Unattributable is not foreign.
    """
    calls = _retire_client(monkeypatch, [])
    target = SharedVolumeTarget(str(tmp_path))
    legacy = f"{MANAGED_PREFIX}  studio=0.1.0  dag_id=old_dag  syntax=taskflow\nx = 1\n"
    target.write("old_dag.py", legacy)

    res = retire_old_dag(
        "old_dag", purge=False, target=target, expect_afdag_id="afd_us",
        verify_ownership=True,
    )

    assert res.get("skipped_reason") is None
    assert res["removed_file"] is True and res["paused"] is True
    assert not (tmp_path / "old_dag.py").exists()


def test_retire_refuses_an_unknown_owner_when_this_flow_has_no_identity(tmp_path, monkeypatch):
    """The symmetric case: `expect_afdag_id` is empty (a pre-provenance `.afdag`).
    Passing `None` there used to disable the guard entirely, so a delayed retire
    could delete and purge a DIFFERENT flow's freshly deployed DAG. An unknown
    identity is a reason to check harder, not to stop checking.
    """
    calls = _retire_client(monkeypatch, [])
    target = SharedVolumeTarget(str(tmp_path))
    stranger = f"{MANAGED_PREFIX}  dag_id=old_dag  afdag_id=afd_them\nx = 1\n"
    target.write("old_dag.py", stranger)

    res = retire_old_dag(
        "old_dag", purge=True, target=target, expect_afdag_id="", verify_ownership=True
    )

    assert res["skipped_reason"]
    assert (tmp_path / "old_dag.py").read_text() == stranger
    assert calls == []  # not deleted from Airflow, not paused


def test_retire_refuses_a_file_studio_did_not_write(tmp_path, monkeypatch):
    calls = _retire_client(monkeypatch, [])
    target = SharedVolumeTarget(str(tmp_path))
    (tmp_path / "old_dag.py").write_text("# hand written\nx = 1\n", encoding="utf-8")

    res = retire_old_dag(
        "old_dag", purge=False, target=target, expect_afdag_id="afd_us",
        verify_ownership=True,
    )

    assert "not managed by Studio" in res["skipped_reason"]
    assert (tmp_path / "old_dag.py").exists()
    assert calls == []


def test_retire_without_a_verification_request_keeps_its_old_behaviour(tmp_path, monkeypatch):
    # The editor's own immediate retire (the user is watching it) asks for no
    # ownership check — the tri-state keeps that distinct from "check requested,
    # identity unknown".
    calls = _retire_client(monkeypatch, [])
    target = SharedVolumeTarget(str(tmp_path))
    target.write("old_dag.py", f"{MANAGED_PREFIX}  dag_id=old_dag  afdag_id=afd_them\nx = 1\n")

    res = retire_old_dag("old_dag", purge=False, target=target)

    assert res.get("skipped_reason") is None
    assert res["removed_file"] is True
