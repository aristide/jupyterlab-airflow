"""Deploy a generated DAG to the Airflow dags folder (PRD §6.5, §8.7, §8.9).

``SharedVolumeTarget`` owns the atomic, collision-safe write to the shared
volume. The deploy lifecycle's *observable* tri-state (poll ``/dags`` +
``/importErrors``) is a separate concern handled by the manager; this module is
just the write side plus validation.

Trust note (PRD §9): writing a `.py` into the dags folder is equivalent to
running code as the Airflow worker. Treat ``deploy`` as privileged.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from typing import Any, Dict, List, Optional, Set, Tuple

from .validation import validate_dag

# Every Studio-generated file starts with this provenance header; we refuse to
# overwrite any file that lacks it (it's a hand-written, read-only DAG).
MANAGED_PREFIX = "# airflow-studio: managed"

# Glob patterns (Airflow 3 `.airflowignore` is glob) for files the dag-processor
# must skip in the dags folder. `*.bak` covers the rollback backups below (which
# already end in `.bak`, not `.py`, but ignore them defensively).
_AIRFLOWIGNORE_PATTERNS = [".afdag-tmp-*", "*.afdag", "*.bak"]

# A deploy that overwrites a managed DAG first copies the prior version here, so
# a bad re-deploy can be rolled back to the last deployed version (PRD §6.5.5 /
# §7). `{dag_id}.py.bak` doesn't end in `.py`, so the dag-processor never parses
# it.
_BACKUP_SUFFIX = ".bak"

# Deployed `.py` files must be readable by the Airflow dag-processor, which on a
# shared dags volume typically runs as a *different* uid than the JupyterLab
# server. 0644 (world-readable) is the conventional dags-folder file mode.
_DEPLOY_FILE_MODE = 0o644

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

    def _ensure_root(self) -> None:
        """Create the dags dir; raise an *actionable* error if it cannot be
        written. The usual cause is an unset/misconfigured ``AIRFLOW_DAGS_DIR``,
        so the deploy falls back to the default ``/opt/airflow/dags`` (Airflow's
        own path) that the JupyterLab server cannot write to — which otherwise
        surfaces as a cryptic ``[Errno 13] Permission denied``.
        """
        hint = (
            f"Cannot write the DAG to the dags folder {self.root!r}. Set the "
            "AIRFLOW_DAGS_DIR environment variable on the JupyterLab server to a "
            "shared dags folder it can write to (in the devcontainer: "
            "/workspace/.devcontainer/airflow-dags), then restart the server."
        )
        try:
            os.makedirs(self.root, exist_ok=True)
        except OSError as err:
            detail = err.strerror or str(err)
            raise DeployError(f"{hint} ({detail})") from err
        if not os.access(self.root, os.W_OK):
            raise DeployError(hint)

    def _atomic_write(self, target_path: str, content: str) -> None:
        """Atomically write ``content`` to ``target_path`` (a co-located temp +
        ``os.replace``), world-readable so the dag-processor (often a different
        uid on a shared volume) can read it."""
        # Temp file co-located so os.replace is atomic (same filesystem).
        fd, tmp = tempfile.mkstemp(dir=self.root, prefix=".afdag-tmp-", suffix=".py")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(content)
                fh.flush()
                os.fsync(fh.fileno())
            # mkstemp() forces mode 0600; on a *shared* dags volume the Airflow
            # dag-processor runs as a different uid (the official image's
            # ``airflow``, uid 50000) than the JupyterLab server, so an
            # owner-only file is unreadable and the DAG never registers — the
            # deploy hangs on "waiting for Airflow to pick it up". Make the file
            # world-readable (the conventional mode for a dags-folder `.py`) so
            # discovery works across the uid boundary. os.replace preserves this
            # mode, so set it on the temp file before the atomic rename.
            os.chmod(tmp, _DEPLOY_FILE_MODE)
            os.replace(tmp, target_path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def _backup_path(self, filename: str) -> str:
        return self.path_for(filename) + _BACKUP_SUFFIX

    def has_backup(self, filename: str) -> bool:
        return os.path.isfile(self._backup_path(filename))

    def restore_backup(self, filename: str) -> bool:
        """Restore the rollback backup over the live file, then drop the backup.
        Returns False (no-op) when there is no backup (PRD §6.5.5 / §7)."""
        backup = self._backup_path(filename)
        if not os.path.isfile(backup):
            return False
        with open(backup, encoding="utf-8") as fh:
            content = fh.read()
        self._atomic_write(self.path_for(filename), content)
        os.unlink(backup)
        return True

    def write(self, filename: str, content: str) -> str:
        """Atomically write ``content`` to ``filename``. Refuses to clobber a
        file that lacks the Studio provenance header (collision safety, §6.5.3).
        When overwriting a managed file, the prior version is saved as a `.bak`
        first so a bad re-deploy can be rolled back (§6.5.5 / §7).
        """
        target = self.path_for(filename)
        self._ensure_root()

        if os.path.isfile(target):
            existing = self.read(filename)
            if _parse_header(existing) is None:
                raise DeployError(
                    f"Refusing to overwrite {filename!r}: it is not a Studio-managed "
                    "file (no provenance header)."
                )
            self._atomic_write(self._backup_path(filename), existing)

        self._atomic_write(target, content)
        return target

    def delete(self, filename: str) -> None:
        target = self.path_for(filename)
        if os.path.isfile(target):
            os.unlink(target)
        # Drop the rollback backup too (an undeploy/purge removes the DAG wholly).
        backup = self._backup_path(filename)
        if os.path.isfile(backup):
            os.unlink(backup)

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
        self._ensure_root()
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


def _body_hash(content: str) -> Optional[str]:
    """sha256 of the file body (everything after the header line)."""
    _, sep, body = content.partition("\n")
    if not sep:
        return None
    return "sha256:" + hashlib.sha256(body.encode("utf-8")).hexdigest()


def _stamp_code_hash(content: str) -> str:
    """Append ``code=sha256:<body-hash>`` to the provenance header so an
    out-of-band hand-edit of the deployed body is detectable on re-deploy
    (PRD §6.5.3). Only the header line changes, so the body hash stays stable."""
    head, sep, body = content.partition("\n")
    if not sep:
        return content
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    return f"{head}  code=sha256:{digest}\n{body}"


def is_drifted(filename: str, target: Optional[SharedVolumeTarget] = None) -> bool:
    """True if a deployed managed file was edited out of band: its body no longer
    matches the ``code=`` hash recorded in its provenance header (PRD §6.5.3).
    False when the file is absent, not Studio-managed, or pre-dates the code hash
    (an older deploy → can't tell, so don't false-alarm)."""
    target = target or SharedVolumeTarget()
    try:
        if not target.exists(filename):
            return False
        content = target.read(filename)
    except DeployError:
        return False
    header = _parse_header(content)
    if header is None:
        return False
    recorded = header.get("code")
    if not recorded:
        return False
    return _body_hash(content) != recorded


def _live_afdag_ids(contents_root: Optional[str]) -> Tuple[Set[str], bool]:
    """The ``afdag_id`` of every `.afdag` design file under the Jupyter Contents
    root — the *source* side of the orphan join (PRD §6.5.6). Hidden/checkpoint
    dirs are skipped.

    Returns ``(ids, degraded)`` where ``degraded`` is True if any `.afdag` could
    not be read or parsed: such a file's ``afdag_id`` is then unknown, so the
    caller must NOT classify the deploy it backs as deleted (a corrupt/unreadable
    source is *present*, not gone) — see :func:`find_orphans`.
    """
    ids: Set[str] = set()
    degraded = False
    if not contents_root or not os.path.isdir(contents_root):
        return ids, degraded
    for dirpath, dirnames, filenames in os.walk(contents_root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for name in filenames:
            if not name.endswith(".afdag"):
                continue
            try:
                with open(os.path.join(dirpath, name), encoding="utf-8") as fh:
                    ir = json.load(fh)
            except (OSError, ValueError):
                degraded = True
                continue
            afdag_id = ((ir or {}).get("provenance") or {}).get("afdag_id")
            if afdag_id:
                ids.add(str(afdag_id).strip())
    return ids, degraded


def find_orphans(
    contents_root: Optional[str] = None,
    target: Optional[SharedVolumeTarget] = None,
) -> Dict[str, Any]:
    """Deployed Studio DAGs whose source `.afdag` no longer exists (PRD §6.5.6).

    An *orphan* is a deployed, Studio-managed `.py` whose ``afdag_id`` provenance
    matches no `.afdag` under the Jupyter Contents root — i.e. the design file
    was deleted (in-session, or out of band via terminal/`git`/`rm`). Remediation
    is the manager-side :func:`purge_dag` (file-first, then ``DELETE /dags/{id}``).

    Returns ``{orphans: [{dag_id, filename, afdag_id}], degraded}``. Files without
    an ``afdag_id`` (pre-provenance deploys) are skipped — they can't be
    re-associated, so we never auto-delete them. ``degraded`` is True when a
    `.afdag` could not be read/parsed (its identity is unknown), so the caller
    should suppress the destructive "source deleted" prompt for that sweep rather
    than risk a false positive.
    """
    target = target or SharedVolumeTarget()
    live_ids, degraded = _live_afdag_ids(contents_root)
    orphans: List[Dict[str, str]] = []
    for entry in target.list():
        afdag_id = entry.get("afdag_id")
        if not afdag_id:
            continue
        if afdag_id not in live_ids:
            orphans.append(
                {
                    "dag_id": entry.get("dag_id", ""),
                    "filename": entry.get("filename", ""),
                    "afdag_id": afdag_id,
                }
            )
    return {"orphans": orphans, "degraded": degraded}


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

    # Hard-gate on provider availability in the TARGET Airflow (PRD §6.2.1):
    # fail fast with a plain-language message *before* writing, instead of an
    # opaque /importErrors later. A no-op when the target is unreachable.
    from .providers import get_target_index, provider_block_errors

    provider_errors = provider_block_errors(ir, get_target_index())
    if provider_errors:
        return {
            "deployed": False,
            "dag_id": dag_id,
            "warnings": [],
            "errors": provider_errors,
            "dagbag": result["dagbag"],
        }

    warnings: List[str] = []
    if result["dagbag"].get("status") == "skipped":
        warnings.append(
            "Local DagBag check skipped (Airflow not importable here); "
            "Airflow will validate the DAG when it imports the file."
        )

    filename = _safe_filename(dag_id)
    target.ensure_airflowignore()
    # An overwrite of a managed file backs up the prior version (write() refuses a
    # non-managed file, so a pre-existing file here is always Studio-managed) →
    # `backed_up` tells the editor a rollback target exists (§6.5.5 / §7).
    backed_up = target.exists(filename)
    # Stamp a body hash into the header so a later out-of-band hand-edit of the
    # deployed file is detectable on re-deploy (§6.5.3).
    path = target.write(filename, _stamp_code_hash(result["code"]))

    return {
        "deployed": True,
        "path": path,
        "filename": filename,
        "dag_id": dag_id,
        "backed_up": backed_up,
        "warnings": warnings,
        "errors": [],
        "dagbag": result["dagbag"],
    }


def rollback_dag(
    dag_id: str, target: Optional[SharedVolumeTarget] = None
) -> Dict[str, Any]:
    """Roll a deployed DAG back to its previous version (PRD §6.5.5 / §7): restore
    the `.bak` saved on the last overwrite-deploy. The restored file re-imports
    via the dag-processor (the editor re-polls the deploy lifecycle). Returns
    ``{dag_id, rolled_back, filename}``; ``rolled_back`` is False when there is no
    backup to restore."""
    target = target or SharedVolumeTarget()
    filename = _safe_filename(dag_id)
    rolled_back = target.restore_backup(filename)
    return {"dag_id": dag_id, "rolled_back": rolled_back, "filename": filename}


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
    drifted = is_drifted(f"{dag_id}.py", target) if file_exists else False

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
        "drifted": drifted,
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
