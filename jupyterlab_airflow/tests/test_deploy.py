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


def _write_afdag(root, name, afdag_id):
    import json

    (root / name).write_text(json.dumps({"provenance": {"afdag_id": afdag_id}}))


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
