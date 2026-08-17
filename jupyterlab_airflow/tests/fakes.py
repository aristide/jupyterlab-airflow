"""Scriptable doubles for the deploy reconciler tests (PRD §6.5.4).

Follows the monkeypatch-``get_client`` style already used by ``test_deploy.py``
and ``test_handlers.py``: the reconciler's logic is pure and injectable, so the
whole lifecycle is exercised with no Airflow, no server, and no sleeping.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from jupyterlab_airflow.client import AirflowError
from jupyterlab_airflow.deploy import MANAGED_PREFIX, UnsafeFilenameError, _FILENAME_RE


def managed_file(dag_id: str, *, afdag_id: str, correlation_id: str, body: str = "x = 1") -> str:
    """A Studio-managed `.py` exactly as ``deploy_dag`` would have written it —
    the provenance header is what every ownership/supersession check reads."""
    return (
        f"{MANAGED_PREFIX}  dag_id={dag_id}  afdag_id={afdag_id}"
        f"  correlation_id={correlation_id}  code=sha256:deadbeef\n{body}\n"
    )


class FakeTarget:
    """An in-memory ``DeployTarget``. ``files`` is the dags folder."""

    consistency = "eventual"

    def __init__(self, files: Optional[Dict[str, str]] = None) -> None:
        self.files: Dict[str, str] = dict(files or {})
        self.deleted: List[str] = []
        #: Set to an exception to make reads fail (the "ambiguity" cases).
        self.read_error: Optional[BaseException] = None

    def _check(self, filename: str) -> None:
        if not _FILENAME_RE.match(filename):
            raise UnsafeFilenameError(f"Unsafe filename: {filename!r}")

    def exists(self, filename: str) -> bool:
        self._check(filename)
        if self.read_error is not None:
            raise self.read_error
        return filename in self.files

    def read(self, filename: str) -> str:
        self._check(filename)
        if self.read_error is not None:
            raise self.read_error
        return self.files[filename]

    def write(self, filename: str, content: str) -> str:
        self._check(filename)
        self.files[filename] = content
        return f"/fake/{filename}"

    def delete(self, filename: str) -> None:
        self._check(filename)
        self.files.pop(filename, None)
        self.deleted.append(filename)

    def list(self) -> List[Dict[str, str]]:
        return [{"filename": name} for name in sorted(self.files)]

    def verify(self, filename: str, ir_hash: Optional[str] = None) -> bool:
        return filename in self.files

    def has_backup(self, filename: str) -> bool:
        return False

    def restore_backup(self, filename: str) -> bool:
        return False

    def ensure_airflowignore(self) -> None:
        return None


class FakeClient:
    """Records every call and answers from a script.

    ``statuses`` is consumed one entry per ``deploy_status`` call; the last entry
    repeats forever (so a test can say "processing, processing, registered"). An
    entry that is an exception is raised instead of returned.
    """

    def __init__(self, statuses: Optional[List[Any]] = None) -> None:
        self.statuses: List[Any] = list(statuses or [{"state": "processing"}])
        self.calls: List[tuple] = []
        self.paused: Dict[str, bool] = {}
        self.trigger_raises: Optional[BaseException] = None
        self.runs: Dict[str, Dict[str, Any]] = {}

    # -- helpers ------------------------------------------------------------ #
    def names(self, name: str) -> List[tuple]:
        return [call for call in self.calls if call[0] == name]

    def count(self, name: str) -> int:
        return len(self.names(name))

    # -- the surface the reconciler uses ------------------------------------ #
    def deploy_status(self, dag_id: str, filename: str) -> Dict[str, Any]:
        self.calls.append(("deploy_status", dag_id, filename))
        item = self.statuses[0] if len(self.statuses) == 1 else self.statuses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def set_paused(self, dag_id: str, is_paused: bool) -> Dict[str, Any]:
        self.calls.append(("set_paused", dag_id, is_paused))
        self.paused[dag_id] = is_paused
        return {"dag_id": dag_id, "is_paused": is_paused}

    def trigger_dag(self, dag_id, conf=None, logical_date=None, dag_run_id=None):
        self.calls.append(("trigger_dag", dag_id, dag_run_id))
        if self.trigger_raises is not None:
            raise self.trigger_raises
        run = {"dag_run_id": dag_run_id or "manual__1", "dag_id": dag_id, "state": "queued"}
        self.runs[run["dag_run_id"]] = run
        return run

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        self.calls.append(("get_dag_run", dag_id, dag_run_id))
        return self.runs.get(
            dag_run_id, {"dag_run_id": dag_run_id, "dag_id": dag_id, "state": "running"}
        )

    def delete_dag(self, dag_id: str) -> Dict[str, Any]:
        self.calls.append(("delete_dag", dag_id))
        return {}

    # -- incidental surface (retire → variables.purge) ---------------------- #
    def list_variables(self, limit=1000, offset=0, key_pattern=None) -> Dict[str, Any]:
        return {"variables": [], "total_entries": 0}

    def list_connections(self, limit=1000, offset=0) -> Dict[str, Any]:
        return {"connections": [], "total_entries": 0}

    def delete_variable(self, key: str) -> Dict[str, Any]:
        raise AirflowError("not found", status=404)
