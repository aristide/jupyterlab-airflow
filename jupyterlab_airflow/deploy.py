"""Deploy a generated DAG to the Airflow dags folder (PRD §6.5, §8.7, §8.9).

``SharedVolumeTarget`` owns the atomic, collision-safe write to the shared
volume. The deploy lifecycle's *observable* tri-state (poll ``/dags`` +
``/importErrors``) is a separate concern handled by the manager; this module is
just the write side plus validation.

Trust note (PRD §9): writing a `.py` into the dags folder is equivalent to
running code as the Airflow worker. Treat ``deploy`` as privileged.
"""

from __future__ import annotations

import os
import re
import tempfile
from typing import Any, Dict, List, Optional

from .validation import validate_dag

# Every Studio-generated file starts with this provenance header; we refuse to
# overwrite any file that lacks it (it's a hand-written, read-only DAG).
MANAGED_PREFIX = "# airflow-studio: managed"

# Glob patterns (Airflow 3 `.airflowignore` is glob) for files the dag-processor
# must skip in the dags folder.
_AIRFLOWIGNORE_PATTERNS = [".afdag-tmp-*", "*.afdag"]

_FILENAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.py$")


class DeployError(Exception):
    """A deploy was refused or failed (surfaced to the UI)."""


def dags_dir() -> str:
    """The deploy root. Override with ``AIRFLOW_DAGS_DIR`` (the devcontainer
    points this at the host `airflow-dags/` mount)."""
    return os.environ.get("AIRFLOW_DAGS_DIR", "/opt/airflow/dags")


def _safe_filename(dag_id: str) -> str:
    filename = f"{dag_id}.py"
    if not _FILENAME_RE.match(filename):
        raise DeployError(f"Unsafe filename for dag_id {dag_id!r}")
    return filename


def _parse_header(text: str) -> Optional[Dict[str, str]]:
    first = text.split("\n", 1)[0]
    if not first.startswith(MANAGED_PREFIX):
        return None
    meta: Dict[str, str] = {}
    for token in first.split():
        if token.startswith("sha256:"):
            meta["ir_hash"] = token
        elif "=" in token:
            key, _, value = token.partition("=")
            meta[key] = value
    return meta


class SharedVolumeTarget:
    """A ``DeployTarget`` writing `.py` files to a local/shared-volume dags dir.

    Visible to the local filesystem synchronously, but Airflow discovery is
    delayed, so the consistency flag is ``"eventual"`` (drives the verify poll).
    """

    consistency = "eventual"

    def __init__(self, root: Optional[str] = None) -> None:
        self.root = os.path.abspath(root or dags_dir())

    def path_for(self, filename: str) -> str:
        if not _FILENAME_RE.match(filename):
            raise DeployError(f"Unsafe filename: {filename!r}")
        target = os.path.abspath(os.path.join(self.root, filename))
        # Defence in depth against traversal even though the regex forbids it.
        if os.path.dirname(target) != self.root:
            raise DeployError(f"Refusing path outside the dags dir: {filename!r}")
        return target

    def exists(self, filename: str) -> bool:
        return os.path.isfile(self.path_for(filename))

    def read(self, filename: str) -> str:
        with open(self.path_for(filename), encoding="utf-8") as fh:
            return fh.read()

    def write(self, filename: str, content: str) -> str:
        """Atomically write ``content`` to ``filename``. Refuses to clobber a
        file that lacks the Studio provenance header (collision safety, §6.5.3).
        """
        target = self.path_for(filename)
        os.makedirs(self.root, exist_ok=True)

        if os.path.isfile(target):
            existing = self.read(filename)
            if _parse_header(existing) is None:
                raise DeployError(
                    f"Refusing to overwrite {filename!r}: it is not a Studio-managed "
                    "file (no provenance header)."
                )

        # Temp file co-located so os.replace is atomic (same filesystem).
        fd, tmp = tempfile.mkstemp(dir=self.root, prefix=".afdag-tmp-", suffix=".py")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(content)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
        return target

    def delete(self, filename: str) -> None:
        target = self.path_for(filename)
        if os.path.isfile(target):
            os.unlink(target)

    def list(self) -> List[Dict[str, str]]:
        """Studio-managed files in the dags dir, with parsed provenance."""
        managed: List[Dict[str, str]] = []
        if not os.path.isdir(self.root):
            return managed
        for name in sorted(os.listdir(self.root)):
            if not name.endswith(".py"):
                continue
            try:
                with open(os.path.join(self.root, name), encoding="utf-8") as fh:
                    header = _parse_header(fh.read(512))
            except OSError:
                continue
            if header is not None:
                managed.append({"filename": name, **header})
        return managed

    def verify(self, filename: str, ir_hash: Optional[str] = None) -> bool:
        """Locally confirm the file is present and (optionally) matches a hash."""
        if not self.exists(filename):
            return False
        header = _parse_header(self.read(filename))
        if header is None:
            return False
        return ir_hash is None or header.get("ir_hash") == ir_hash

    def ensure_airflowignore(self) -> None:
        path = os.path.join(self.root, ".airflowignore")
        os.makedirs(self.root, exist_ok=True)
        existing = ""
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as fh:
                existing = fh.read()
        lines = existing.splitlines()
        missing = [p for p in _AIRFLOWIGNORE_PATTERNS if p not in lines]
        if missing:
            with open(path, "a", encoding="utf-8") as fh:
                if existing and not existing.endswith("\n"):
                    fh.write("\n")
                fh.write("\n".join(missing) + "\n")


def deploy_dag(ir: Dict[str, Any], target: Optional[SharedVolumeTarget] = None) -> Dict[str, Any]:
    """Validate, then atomically write the generated DAG to the dags folder.

    Returns ``{deployed, path?, filename?, dag_id, warnings, errors, dagbag}``.
    Does not write when validation fails.
    """
    target = target or SharedVolumeTarget()
    dag_id = (ir.get("dag") or {}).get("dag_id", "")

    result = validate_dag(ir)
    if not result["valid"]:
        dagbag = result["dagbag"]
        errors = list(result["errors"])
        if dagbag.get("status") == "error":
            errors.append(f"DagBag import failed: {dagbag.get('detail')}")
        return {
            "deployed": False,
            "dag_id": dag_id,
            "warnings": [],
            "errors": errors or ["Validation failed"],
            "dagbag": dagbag,
        }

    warnings: List[str] = []
    if result["dagbag"].get("status") == "skipped":
        warnings.append(
            "Local DagBag check skipped (Airflow not importable here); "
            "Airflow will validate the DAG when it imports the file."
        )

    filename = _safe_filename(dag_id)
    target.ensure_airflowignore()
    path = target.write(filename, result["code"])

    return {
        "deployed": True,
        "path": path,
        "filename": filename,
        "dag_id": dag_id,
        "warnings": warnings,
        "errors": [],
        "dagbag": result["dagbag"],
    }


def purge_dag(dag_id: str, target: Optional[SharedVolumeTarget] = None) -> Dict[str, Any]:
    """Delete a DAG: remove its `.py` **first** (so it isn't re-imported), then
    purge its history via ``DELETE /api/v2/dags/{id}``. Tolerates a missing file
    or a DAG that Airflow hasn't recorded yet (404)."""
    from .client import AirflowError, get_client

    target = target or SharedVolumeTarget()
    filename = f"{dag_id}.py"
    removed_file = False
    try:
        if target.exists(filename):
            target.delete(filename)
            removed_file = True
    except DeployError:
        # dag_id isn't a safe/managed filename (e.g. a hand-written DAG) — skip.
        pass

    purged_history = False
    try:
        get_client().delete_dag(dag_id)
        purged_history = True
    except AirflowError as err:
        if err.status != 404:
            raise

    return {
        "dag_id": dag_id,
        "removed_file": removed_file,
        "purged_history": purged_history,
    }


def rename_preflight(dag_id: str, target: Optional[SharedVolumeTarget] = None) -> Dict[str, Any]:
    """Report the deploy state of ``dag_id`` so the editor can pick the rename
    path (PRD §6.1.8(B)). Returns ``{dag_id, file_exists, registered, active_runs}``:

    - ``file_exists`` False **and** ``registered`` False -> a **draft** (rename is
      a plain `dag_id` set, nothing to migrate);
    - ``active_runs`` > 0 -> **block** (renaming would strand the in-flight run);
    - otherwise -> a **deployed-idle** migration.
    """
    from .client import AirflowError, get_client

    target = target or SharedVolumeTarget()
    try:
        file_exists = target.exists(f"{dag_id}.py")
    except DeployError:
        file_exists = False

    client = get_client()
    registered = False
    try:
        client.get_dag(dag_id)
        registered = True
    except AirflowError as err:
        if err.status != 404:
            raise

    active_runs = 0
    if registered:
        runs = (client.list_dag_runs(dag_id, limit=25) or {}).get("dag_runs", []) or []
        active = {"running", "queued"}
        active_runs = sum(
            1 for run in runs if str(run.get("state", "")).lower() in active
        )

    return {
        "dag_id": dag_id,
        "file_exists": bool(file_exists),
        "registered": registered,
        "active_runs": active_runs,
    }


def retire_old_dag(
    dag_id: str, *, purge: bool, target: Optional[SharedVolumeTarget] = None
) -> Dict[str, Any]:
    """Reconcile the OLD DAG after a `dag_id` rename migration (PRD §6.1.8(B)).

    - ``purge=True``  -> remove the `.py` **and** delete history
      (``DELETE /api/v2/dags/{id}``) — same as :func:`purge_dag`.
    - ``purge=False`` -> remove the `.py` (so it isn't re-imported) and **pause**
      the now-fileless/stale DAG, **keeping** its run history.

    Tolerant of a missing file / a DAG Airflow never recorded (404).
    """
    if purge:
        return purge_dag(dag_id, target)

    from .client import AirflowError, get_client

    target = target or SharedVolumeTarget()
    filename = f"{dag_id}.py"
    removed_file = False
    try:
        if target.exists(filename):
            target.delete(filename)
            removed_file = True
    except DeployError:
        # Not a safe/managed filename (e.g. a hand-written DAG) — leave it.
        pass

    paused = False
    try:
        get_client().set_paused(dag_id, True)
        paused = True
    except AirflowError as err:
        if err.status != 404:
            raise

    return {
        "dag_id": dag_id,
        "removed_file": removed_file,
        "paused": paused,
        "purged_history": False,
    }
