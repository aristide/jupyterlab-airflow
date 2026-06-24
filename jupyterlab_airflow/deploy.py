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
from uuid import uuid4

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


class DeployTarget:
    """The deploy-target interface (PRD §6.5.1): ``write``/``exists``/``read``/
    ``list``/``delete``/``verify`` + backup (``has_backup``/``restore_backup``) +
    ``ensure_airflowignore``, with a ``consistency`` flag (``"sync"`` vs
    ``"eventual"``) that drives the verification poll. Concrete targets:
    ``SharedVolumeTarget`` (+ its ``GitDeployTarget`` subclass) and
    ``S3DeployTarget``. Kept as a thin base so the factory + the deploy functions
    can type against the interface rather than a specific backend."""

    consistency = "eventual"


class SharedVolumeTarget(DeployTarget):
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


class GitDeployTarget(SharedVolumeTarget):
    """A ``DeployTarget`` that commits generated DAGs to a **git** working tree
    (PRD §6.5.1 / §8.7) that Airflow tracks via a ``GitDagBundle``.

    Reuses ``SharedVolumeTarget``'s namespacing, atomic write, collision safety
    (the provenance‑header guard), backup/rollback, and ``.airflowignore`` — each
    *mutating* op additionally ``git add`` + ``git commit``s the change (and
    ``git push``es when a remote is configured). The deploy root is the repo's DAG
    subdir; reads/listing operate on the working tree exactly like the shared
    volume. Discovery is eventual (Airflow's bundle polls the repo), so the
    consistency flag stays ``"eventual"`` (drives the verify poll).

    Config (env): ``AIRFLOW_GIT_DAGS_REPO`` (the local git working tree —
    required), ``AIRFLOW_GIT_DAGS_SUBDIR`` (DAG subdir, default ``dags``),
    ``AIRFLOW_GIT_DAGS_BRANCH`` (push branch, default ``main``),
    ``AIRFLOW_GIT_DAGS_REMOTE`` (remote to push to; unset → commit‑only, for a
    repo Airflow reads directly).
    """

    consistency = "eventual"

    def __init__(
        self,
        repo: Optional[str] = None,
        subdir: Optional[str] = None,
        branch: Optional[str] = None,
        remote: Optional[str] = None,
    ) -> None:
        repo = repo if repo is not None else os.environ.get("AIRFLOW_GIT_DAGS_REPO", "")
        self.repo = os.path.abspath(repo) if repo else ""
        self.subdir = (
            subdir if subdir is not None else os.environ.get("AIRFLOW_GIT_DAGS_SUBDIR", "dags")
        ) or ""
        self.branch = branch or os.environ.get("AIRFLOW_GIT_DAGS_BRANCH", "main")
        self.remote = (
            remote if remote is not None else os.environ.get("AIRFLOW_GIT_DAGS_REMOTE", "")
        )
        root = os.path.join(self.repo, self.subdir) if (self.repo and self.subdir) else self.repo
        # When unconfigured (no repo), point at a non-existent sentinel — never the
        # cwd — so read-only ops (list/exists) return empty rather than scanning the
        # server's working dir; mutating ops raise an actionable error via
        # _ensure_root (which checks self.repo first).
        super().__init__(root=root or "/nonexistent/airflow-studio-git-unconfigured")

    # Bound every git call so a push to a slow/unreachable remote can't hang the
    # deploy (these run in the Tornado thread-pool executor).
    _GIT_TIMEOUT = 60

    def _git(self, *args: str, check: bool = True) -> Tuple[int, str, str]:
        import subprocess

        try:
            proc = subprocess.run(
                ["git", "-C", self.repo, *args],
                capture_output=True,
                text=True,
                timeout=self._GIT_TIMEOUT,
            )
        except subprocess.TimeoutExpired as err:
            raise DeployError(
                f"git {' '.join(args)} timed out after {self._GIT_TIMEOUT}s "
                "(is the remote reachable?)."
            ) from err
        except OSError as err:  # git not installed / not on PATH
            raise DeployError(f"git is not available: {err}") from err
        if check and proc.returncode != 0:
            detail = (proc.stderr or proc.stdout).strip()
            raise DeployError(f"git {' '.join(args)} failed: {detail}")
        return proc.returncode, proc.stdout, proc.stderr

    def _ensure_root(self) -> None:
        if not self.repo:
            raise DeployError(
                "Git deploy target: set AIRFLOW_GIT_DAGS_REPO to the git working "
                "tree that your Airflow GitDagBundle tracks, then restart the server."
            )
        code, out, _ = self._git("rev-parse", "--is-inside-work-tree", check=False)
        if code != 0 or out.strip() != "true":
            raise DeployError(
                f"Git deploy target: {self.repo!r} is not a git repository. "
                "Clone/init it (and check out the bundle branch) first."
            )
        # The working tree MUST be on the configured bundle branch, so the commit
        # lands on (and pushes to) the branch Airflow actually reads. Otherwise a
        # commit on the current branch + a `push …:branch` would silently diverge
        # the two. Detached HEAD / a different branch are refused, not cross-wired.
        code, branch, _ = self._git("symbolic-ref", "--short", "HEAD", check=False)
        current = branch.strip()
        if code != 0 or current != self.branch:
            raise DeployError(
                f"Git deploy target: the repo {self.repo!r} is on "
                f"{current or 'a detached HEAD'!r}, but the deploy is configured for "
                f"branch {self.branch!r}. Check it out "
                f"(git -C {self.repo} checkout {self.branch}) and redeploy."
            )
        super()._ensure_root()
        self._ensure_gitignore()

    def _ensure_gitignore(self) -> None:
        """Keep the rollback ``*.bak`` and atomic-write temp files out of git (a
        belt-and-braces complement to the path-scoped commits below — they would
        never be committed anyway, but this also keeps ``git status`` clean)."""
        patterns = ["*.bak", ".afdag-tmp-*"]
        path = os.path.join(self.root, ".gitignore")
        existing = ""
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as fh:
                existing = fh.read()
        missing = [p for p in patterns if p not in existing.splitlines()]
        if missing:
            with open(path, "a", encoding="utf-8") as fh:
                if existing and not existing.endswith("\n"):
                    fh.write("\n")
                fh.write("\n".join(missing) + "\n")

    def _commit(self, message: str, paths: List[str]) -> None:
        """Commit **exactly** ``paths`` (absolute, within the repo) on the
        configured branch, then push when a remote is set.

        Both the empty-check and the commit are **pathspec-scoped** so the deploy
        commit contains only the files it touched — never unrelated/secret changes
        that happen to be pre-staged in the repo's index. Push is **transactional**:
        if it is rejected (non-fast-forward / unreachable) the local commit is
        rolled back (``reset --soft``) so a retry starts clean instead of stacking
        commits, and an actionable error is raised."""
        for path in paths:
            self._git("add", "--", path, check=False)
        # Empty-check scoped to our paths (a re-deploy of identical content, or a
        # delete of a never-committed file, stages nothing → no commit).
        staged, _, _ = self._git("diff", "--cached", "--quiet", "--", *paths, check=False)
        if staged == 0:
            return
        # Commit ONLY our paths (the trailing `-- <paths>` overrides the index for
        # other files), with a fixed identity so it works without global git config.
        self._git(
            "-c",
            "user.email=airflow-studio@localhost",
            "-c",
            "user.name=Airflow Studio",
            "commit",
            "-m",
            message,
            "--",
            *paths,
        )
        if self.remote:
            refspec = f"refs/heads/{self.branch}:refs/heads/{self.branch}"
            code, _, err = self._git("push", self.remote, refspec, check=False)
            if code != 0:
                # Undo the local commit so the repo isn't left ahead/divergent and a
                # retry doesn't stack commits (the file stays staged on disk).
                self._git("reset", "--soft", "HEAD~1", check=False)
                raise DeployError(
                    f"git push to {self.remote}/{self.branch} was rejected, so the "
                    "local deploy commit was rolled back — fetch/rebase the repo and "
                    f"redeploy. Details: {(err or '').strip()}"
                )

    def write(self, filename: str, content: str) -> str:
        target = super().write(filename, content)
        paths = [target]
        for sidecar in (".airflowignore", ".gitignore"):
            full = os.path.join(self.root, sidecar)
            if os.path.isfile(full):
                paths.append(full)
        self._commit(f"airflow-studio: deploy {filename}", paths)
        return target

    def delete(self, filename: str) -> None:
        self._ensure_root()  # refuse a wrong-branch / non-repo delete before mutating
        target = self.path_for(filename)
        super().delete(filename)
        self._commit(f"airflow-studio: undeploy {filename}", [target])

    def restore_backup(self, filename: str) -> bool:
        self._ensure_root()  # refuse a wrong-branch / non-repo rollback before mutating
        restored = super().restore_backup(filename)
        if restored:
            self._commit(
                f"airflow-studio: roll back {filename}", [self.path_for(filename)]
            )
        return restored


class S3DeployTarget(DeployTarget):
    """A ``DeployTarget`` that writes generated DAGs as **S3 objects** (PRD §6.5.1
    / §8.7) under a key prefix that an Airflow S3‑backed DAG bundle (or an S3→dags
    sync) picks up. Works against AWS S3 or any S3‑compatible store (e.g. MinIO via
    ``AIRFLOW_S3_ENDPOINT_URL``).

    Reuses the shared namespacing helpers (``_FILENAME_RE`` key safety,
    ``_parse_header`` provenance, ``MANAGED_PREFIX`` collision guard) — only the
    storage backend differs. ``put_object`` is atomic per object; an overwrite of a
    managed object first copies the prior version to a ``…​.py.bak`` object (a
    rollback target, §7); discovery is eventual (the bundle polls), so the
    consistency flag is ``"eventual"``.

    The boto3 client is created lazily (so importing this module never requires
    boto3) and is **injectable** for testing. Config (env): ``AIRFLOW_S3_DAGS_BUCKET``
    (required), ``AIRFLOW_S3_DAGS_PREFIX`` (key prefix, default ``dags``),
    ``AIRFLOW_S3_ENDPOINT_URL`` (for MinIO / S3‑compatible stores).
    """

    consistency = "eventual"

    def __init__(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        client: Any = None,
    ) -> None:
        self.bucket = (
            bucket if bucket is not None else os.environ.get("AIRFLOW_S3_DAGS_BUCKET", "")
        )
        prefix = prefix if prefix is not None else os.environ.get("AIRFLOW_S3_DAGS_PREFIX", "dags")
        self.prefix = (prefix or "").strip("/")
        self.endpoint_url = (
            endpoint_url
            if endpoint_url is not None
            else os.environ.get("AIRFLOW_S3_ENDPOINT_URL", "")
        ) or None
        self._client = client  # injectable; lazily created from boto3 otherwise

    # -- helpers ----------------------------------------------------------- #
    def _s3(self) -> Any:
        if not self.bucket:
            raise DeployError(
                "S3 deploy target: set AIRFLOW_S3_DAGS_BUCKET to the bucket your "
                "Airflow S3 DAG bundle reads, then restart the server."
            )
        if self._client is None:
            try:
                import boto3
            except ImportError as err:
                raise DeployError(
                    "S3 deploy target needs the boto3 package (pip install boto3)."
                ) from err
            self._client = boto3.client("s3", endpoint_url=self.endpoint_url)
        return self._client

    def _key(self, filename: str) -> str:
        """The S3 key for a deployed DAG file. ``filename`` must be a safe
        ``<dag_id>.py`` (no traversal); the prefix is admin‑configured."""
        if not _FILENAME_RE.match(filename):
            raise DeployError(f"Unsafe filename: {filename!r}")
        return f"{self.prefix}/{filename}" if self.prefix else filename

    def _backup_key(self, filename: str) -> str:
        return self._key(filename) + _BACKUP_SUFFIX

    def _sidecar_key(self, name: str) -> str:
        return f"{self.prefix}/{name}" if self.prefix else name

    @staticmethod
    def _is_not_found(err: Exception) -> bool:
        """Whether a boto3/botocore error is a missing *key* (404) — without
        importing botocore (which may be absent until boto3 runs).

        A missing/mistyped *bucket* (``NoSuchBucket``) is **not** treated as a
        missing key: it's a config error that must surface (e.g. so `delete` /
        `purge_dag` don't silently report success against a wrong
        ``AIRFLOW_S3_DAGS_BUCKET``). GET/DELETE return the ``NoSuchBucket`` code so
        this distinguishes them; a HEAD on a missing bucket reports only HTTP 404
        (no error body) and is indistinguishable from a missing key — an inherent
        S3 limitation."""
        resp = getattr(err, "response", None)
        if not isinstance(resp, dict):
            return False
        code = str(resp.get("Error", {}).get("Code", ""))
        if code == "NoSuchBucket":
            return False
        status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return code in ("404", "NoSuchKey", "NotFound") or status == 404

    def _get(self, key: str) -> str:
        return self._s3().get_object(Bucket=self.bucket, Key=key)["Body"].read().decode("utf-8")

    def _put(self, key: str, content: str) -> None:
        self._s3().put_object(Bucket=self.bucket, Key=key, Body=content.encode("utf-8"))

    def _delete_key(self, key: str) -> None:
        # DeleteObject on a missing key is a no-op in S3; tolerate not-found anyway.
        try:
            self._s3().delete_object(Bucket=self.bucket, Key=key)
        except Exception as err:  # noqa: BLE001
            if not self._is_not_found(err):
                raise

    def _key_exists(self, key: str) -> bool:
        try:
            self._s3().head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception as err:  # noqa: BLE001
            if self._is_not_found(err):
                return False
            raise

    # -- DeployTarget interface ------------------------------------------- #
    def exists(self, filename: str) -> bool:
        return self._key_exists(self._key(filename))

    def read(self, filename: str) -> str:
        return self._get(self._key(filename))

    def has_backup(self, filename: str) -> bool:
        return self._key_exists(self._backup_key(filename))

    def restore_backup(self, filename: str) -> bool:
        """Restore the `.bak` object over the live object, then drop the backup.
        Returns False (no-op) when there is no backup (PRD §6.5.5 / §7)."""
        backup = self._backup_key(filename)
        if not self._key_exists(backup):
            return False
        self._put(self._key(filename), self._get(backup))
        self._delete_key(backup)
        return True

    def write(self, filename: str, content: str) -> str:
        """Put ``content`` as the DAG object. Refuses to clobber an object that
        lacks the Studio provenance header (collision safety, §6.5.3); an overwrite
        of a managed object first copies the prior version to a `.bak` object so a
        bad re-deploy can be rolled back (§6.5.5 / §7)."""
        key = self._key(filename)
        if self._key_exists(key):
            existing = self._get(key)
            if _parse_header(existing) is None:
                raise DeployError(
                    f"Refusing to overwrite {filename!r}: it is not a Studio-managed "
                    "file (no provenance header)."
                )
            self._put(self._backup_key(filename), existing)
        self._put(key, content)
        return f"s3://{self.bucket}/{key}"

    def delete(self, filename: str) -> None:
        self._delete_key(self._key(filename))
        self._delete_key(self._backup_key(filename))

    def list(self) -> List[Dict[str, str]]:
        """Studio-managed DAG objects under the prefix, with parsed provenance.
        Paginates ListObjectsV2 and skips non-`.py` and any nested keys."""
        managed: List[Dict[str, str]] = []
        client = self._s3()
        list_prefix = f"{self.prefix}/" if self.prefix else ""
        token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": self.bucket, "Prefix": list_prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []) or []:
                key = obj.get("Key", "")
                name = key[len(list_prefix):] if list_prefix else key
                if not name.endswith(".py") or "/" in name:
                    continue  # non-DAG, or a nested key under the prefix
                try:
                    content = self._get(key)
                except Exception as err:  # noqa: BLE001
                    # An object listed but gone by the time we read it (a delete
                    # racing the list) is skipped; any other error (auth, decode,
                    # …) propagates rather than silently dropping a managed DAG
                    # from the listing — matching SharedVolumeTarget.list (which
                    # only swallows OSError and lets unexpected errors surface).
                    if self._is_not_found(err):
                        continue
                    raise
                header = _parse_header(content)
                if header is not None:
                    managed.append({"filename": name, **header})
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
            if not token:
                break
        return sorted(managed, key=lambda entry: entry["filename"])

    def verify(self, filename: str, ir_hash: Optional[str] = None) -> bool:
        if not self.exists(filename):
            return False
        header = _parse_header(self.read(filename))
        if header is None:
            return False
        return ir_hash is None or header.get("ir_hash") == ir_hash

    def ensure_airflowignore(self) -> None:
        """Ensure the `.airflowignore` object under the prefix carries the Studio
        ignore patterns (get-modify-put — S3 has no append)."""
        key = self._sidecar_key(".airflowignore")
        existing = self._get(key) if self._key_exists(key) else ""
        lines = existing.splitlines()
        missing = [p for p in _AIRFLOWIGNORE_PATTERNS if p not in lines]
        if missing:
            sep = "" if (not existing or existing.endswith("\n")) else "\n"
            self._put(key, existing + sep + "\n".join(missing) + "\n")


def get_deploy_target() -> DeployTarget:
    """The configured deploy target (PRD §6.5.1). ``AIRFLOW_DEPLOY_TARGET`` selects
    ``git`` (the git‑bundle target) or ``s3`` (the object‑storage target); anything
    else (default) is the shared volume."""
    kind = os.environ.get("AIRFLOW_DEPLOY_TARGET", "shared_volume").strip().lower()
    if kind in ("git", "git_bundle", "gitdagbundle"):
        return GitDeployTarget()
    if kind in ("s3", "s3_bundle", "object_storage"):
        return S3DeployTarget()
    return SharedVolumeTarget()


def _body_hash(content: str) -> Optional[str]:
    """sha256 of the file body (everything after the header line)."""
    _, sep, body = content.partition("\n")
    if not sep:
        return None
    return "sha256:" + hashlib.sha256(body.encode("utf-8")).hexdigest()


def _stamp_code_hash(content: str, correlation_id: Optional[str] = None) -> str:
    """Stamp the provenance header at deploy time: ``code=sha256:<body-hash>``
    (so an out-of-band hand-edit of the deployed body is detectable on re-deploy,
    PRD §6.5.3) and, when given, ``correlation_id=<id>`` (so a deployed `.py` —
    and a later import error on it — traces back to the deploy's audit record,
    §8.9 / §10). Only the **header line** changes, so the body hash stays stable
    (and `generate_dag` itself stays deterministic — the per-deploy id is stamped
    here, not in codegen)."""
    head, sep, body = content.partition("\n")
    if not sep:
        return content
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    cid = f"  correlation_id={correlation_id}" if correlation_id else ""
    return f"{head}{cid}  code=sha256:{digest}\n{body}"


def is_drifted(filename: str, target: Optional[DeployTarget] = None) -> bool:
    """True if a deployed managed file was edited out of band: its body no longer
    matches the ``code=`` hash recorded in its provenance header (PRD §6.5.3).
    False when the file is absent, not Studio-managed, or pre-dates the code hash
    (an older deploy → can't tell, so don't false-alarm)."""
    target = target or get_deploy_target()
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


def _afdag_paths(contents_root: Optional[str]) -> Tuple[Dict[str, str], bool]:
    """Map ``afdag_id`` → Contents-relative path for every `.afdag` design file
    under the Jupyter Contents root. The *source* side of the orphan join (PRD
    §6.5.6) and the backing index for :func:`find_source_path` ("Open in Studio
    to fix", §7). Hidden/checkpoint dirs are skipped; paths use forward slashes.

    Returns ``(id→path, degraded)`` where ``degraded`` is True if any `.afdag`
    could not be read or parsed (its ``afdag_id`` is then unknown). If two files
    share an ``afdag_id`` (a copied `.afdag`), the first walked wins.
    """
    paths: Dict[str, str] = {}
    degraded = False
    if not contents_root or not os.path.isdir(contents_root):
        return paths, degraded
    for dirpath, dirnames, filenames in os.walk(contents_root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for name in filenames:
            if not name.endswith(".afdag"):
                continue
            full = os.path.join(dirpath, name)
            try:
                with open(full, encoding="utf-8") as fh:
                    ir = json.load(fh)
            except (OSError, ValueError):
                degraded = True
                continue
            afdag_id = ((ir or {}).get("provenance") or {}).get("afdag_id")
            if afdag_id:
                key = str(afdag_id).strip()
                if key not in paths:
                    rel = os.path.relpath(full, contents_root)
                    paths[key] = rel.replace(os.sep, "/")
    return paths, degraded


def _live_afdag_ids(contents_root: Optional[str]) -> Tuple[Set[str], bool]:
    """The ``afdag_id`` of every `.afdag` design file under the Jupyter Contents
    root — the *source* side of the orphan join (PRD §6.5.6). See
    :func:`_afdag_paths`; ``degraded`` propagates unchanged.
    """
    paths, degraded = _afdag_paths(contents_root)
    return set(paths.keys()), degraded


def find_source_path(
    filename: Optional[str] = None,
    dag_id: Optional[str] = None,
    contents_root: Optional[str] = None,
    target: Optional[DeployTarget] = None,
) -> Dict[str, Any]:
    """Resolve a deployed Studio DAG back to its source `.afdag` Contents path so
    the manager can offer "Open in Studio to fix" on an import error (PRD §7).

    Matches the deployed `.py` (by ``filename`` basename, else ``dag_id``) to its
    ``afdag_id`` provenance, then finds the `.afdag` carrying that id. Returns
    ``{path}`` (Contents-relative, or ``None`` if the source can't be located —
    the deploy pre-dates ``afdag_id``, the `.afdag` was deleted, or no managed
    file matches).
    """
    target = target or get_deploy_target()
    base = os.path.basename(filename) if filename else None
    afdag_id: Optional[str] = None
    for entry in target.list():
        if base is not None and entry.get("filename") == base:
            afdag_id = entry.get("afdag_id")
            break
        if base is None and dag_id and entry.get("dag_id") == dag_id:
            afdag_id = entry.get("afdag_id")
            break
    if not afdag_id:
        return {"path": None}
    paths, _ = _afdag_paths(contents_root)
    return {"path": paths.get(str(afdag_id).strip())}


def find_orphans(
    contents_root: Optional[str] = None,
    target: Optional[DeployTarget] = None,
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
    target = target or get_deploy_target()
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


def deploy_dag(ir: Dict[str, Any], target: Optional[DeployTarget] = None) -> Dict[str, Any]:
    """Validate, then atomically write the generated DAG to the dags folder.

    Returns ``{deployed, path?, filename?, dag_id, correlation_id, warnings,
    errors, dagbag}``. Does not write when validation fails. The
    ``correlation_id`` (a per-deploy id) is stamped into the written `.py`
    provenance header and returned so the deploy's audit record carries the same
    id — tracing a deployed DAG (and a later import error) back to the deploy
    session (§8.9 / §10). It is returned on every path (incl. a refusal) so the
    audit always has a trace id, even when nothing is written.
    """
    target = target or get_deploy_target()
    dag_id = (ir.get("dag") or {}).get("dag_id", "")
    correlation_id = uuid4().hex

    result = validate_dag(ir)
    if not result["valid"]:
        dagbag = result["dagbag"]
        errors = list(result["errors"])
        if dagbag.get("status") == "error":
            errors.append(f"DagBag import failed: {dagbag.get('detail')}")
        return {
            "deployed": False,
            "dag_id": dag_id,
            "correlation_id": correlation_id,
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
            "correlation_id": correlation_id,
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
    # Stamp the body hash (drift detection, §6.5.3) + the per-deploy
    # correlation_id (audit↔provenance trace loop, §8.9/§10) into the header.
    path = target.write(filename, _stamp_code_hash(result["code"], correlation_id))

    return {
        "deployed": True,
        "path": path,
        "filename": filename,
        "dag_id": dag_id,
        "correlation_id": correlation_id,
        "backed_up": backed_up,
        "warnings": warnings,
        "errors": [],
        "dagbag": result["dagbag"],
    }


def rollback_dag(
    dag_id: str, target: Optional[DeployTarget] = None
) -> Dict[str, Any]:
    """Roll a deployed DAG back to its previous version (PRD §6.5.5 / §7): restore
    the `.bak` saved on the last overwrite-deploy. The restored file re-imports
    via the dag-processor (the editor re-polls the deploy lifecycle). Returns
    ``{dag_id, rolled_back, filename}``; ``rolled_back`` is False when there is no
    backup to restore."""
    target = target or get_deploy_target()
    filename = _safe_filename(dag_id)
    rolled_back = target.restore_backup(filename)
    return {"dag_id": dag_id, "rolled_back": rolled_back, "filename": filename}


def purge_dag(dag_id: str, target: Optional[DeployTarget] = None) -> Dict[str, Any]:
    """Delete a DAG: remove its `.py` **first** (so it isn't re-imported), then
    purge its history via ``DELETE /api/v2/dags/{id}``. Tolerates a missing file
    or a DAG that Airflow hasn't recorded yet (404)."""
    from .client import AirflowError, get_client

    target = target or get_deploy_target()
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


def rename_preflight(dag_id: str, target: Optional[DeployTarget] = None) -> Dict[str, Any]:
    """Report the deploy state of ``dag_id`` so the editor can pick the rename
    path (PRD §6.1.8(B)). Returns ``{dag_id, file_exists, registered, active_runs}``:

    - ``file_exists`` False **and** ``registered`` False -> a **draft** (rename is
      a plain `dag_id` set, nothing to migrate);
    - ``active_runs`` > 0 -> **block** (renaming would strand the in-flight run);
    - otherwise -> a **deployed-idle** migration.
    """
    from .client import AirflowError, get_client

    target = target or get_deploy_target()
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
    dag_id: str, *, purge: bool, target: Optional[DeployTarget] = None
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

    target = target or get_deploy_target()
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
