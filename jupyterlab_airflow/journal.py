"""The durable deploy journal — one file per in-flight deploy lifecycle (PRD §6.5.4).

A Studio deploy is not finished when the `.py` lands: Airflow still has to
discover it, the renamed-away ``dag_id`` still has to be retired, and the new DAG
still has to be unpaused and triggered. Until now that tail ran **in the browser
tab**, so closing the tab (or reloading the page) mid-flight left the old DAG
live and the new one paused forever. This module is the server-side memory that
makes the tail survive a closed tab, a page reload, and a server restart: the
deploy handler writes one entry here, and ``reconciler.py`` performs the
remaining steps from it.

Design notes worth keeping:

* **Not in the dags folder.** That directory is scanned by the dag-processor,
  is a git working tree under the git target, and is not a filesystem at all
  under the S3 target. The journal lives under the server's ``data_dir``
  (override with ``JUPYTERLAB_AIRFLOW_JOURNAL_DIR``).
* **Files, not SQLite.** One small JSON file per deploy: no migrations, no
  shared page that a single corruption can take down, and a corrupt entry can be
  quarantined and inspected instead of blocking every other deploy.
* **``pending`` / ``inflight`` / ``done`` / ``quarantine`` subdirs.** Claiming an
  entry is ``os.rename(pending → inflight)``, which is atomic and gives
  at-most-one active worker for free — and crash detection for free too, since
  anything sitting in ``inflight/`` at startup is orphaned.
* **The entry never carries the IR, a path, a trigger ``conf`` or any
  credential** — same rule ``audit.py`` states. It is self-contained, so a
  deleted/moved/renamed `.afdag` can neither strand nor misdirect the reconciler.

**Invariant (load-bearing for authorization, PRD §9):** entries are created only
by ``deploy.deploy_dag``, i.e. only behind ``_AirflowHandler.respond``'s role
gate. See :meth:`Journal.put`.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

_log = logging.getLogger(__name__)

#: Bump only for an incompatible entry shape. A *higher* version found on disk is
#: left alone rather than quarantined — a downgraded server must not destroy the
#: work of a newer one it cannot understand. A *lower* one is upgraded in memory
#: on read (:func:`_upgrade`), never quarantined: an unreadable journal after an
#: upgrade would strand exactly the in-flight deploys the journal exists for.
#:
#: v2 split the single ``retire`` step into a reversible ``quiesce`` (pause the
#: old DAG) and the irreversible ``retire`` (delete its file / purge history), and
#: moved the latter to the END of the lifecycle — see ``reconciler``'s I1.
JOURNAL_VERSION = 2

#: An entry is ~1.5 KB. Anything past this is junk (or an attack) — refuse to
#: parse it rather than pulling an arbitrary blob into memory.
MAX_ENTRY_BYTES = 65_536

_DEPLOY_ID_RE = re.compile(r"^[0-9a-f]{32}$")  # uuid4().hex
# A dag_id is interpolated straight into the Airflow API path by
# `client._request`, so this regex is a security check, not a nicety.
_DAG_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: The lifecycle order, and the order is the safety property. ``retire`` is the
#: only irreversible step, so it runs LAST — after the new DAG is registered,
#: unpaused and triggered. It used to sit at position 2 of 4, which is how a real
#: rename purged the old DAG and then abandoned the new one, leaving the user
#: with no DAG at all. ``quiesce`` (pause the old DAG) takes the old position: it
#: is what actually prevented two DAGs running the same pipeline, and it costs
#: nothing if the entry is abandoned afterwards because pausing is reversible.
_PHASES = (
    "awaiting_registration",
    "quiescing",
    "unpausing",
    "triggering",
    "retiring",
    "terminal",
)
_OUTCOMES = (
    "completed",
    "import_failed",
    "expired",
    "failed",
    "denied",
    "superseded",
    "cancelled",
    #: Something irreversible committed and the lifecycle then stopped early.
    #: Never a *request's* outcome — see :data:`_STOPPABLE_OUTCOMES`.
    "needs_repair",
)
#: Outcomes that mean "stop, nothing further is needed". If anything
#: irreversible has already committed that statement is false, so
#: ``reconciler._terminate`` rewrites these to ``needs_repair``.
ABANDON_OUTCOMES = ("superseded", "expired", "denied", "cancelled")
#: What an outside caller may ask :meth:`Journal.request_stop` for. Deliberately
#: a subset: a request handler (or a forged entry) must not be able to mint
#: ``completed`` or ``needs_repair``, which are conclusions the reconciler draws.
_STOPPABLE_OUTCOMES = ("cancelled", "superseded")
_STEPS = ("registered", "quiesce", "unpause", "trigger", "retire")
_STEP_STATES = ("pending", "done", "skipped", "failed")

PENDING, INFLIGHT, DONE, QUARANTINE = "pending", "inflight", "done", "quarantine"
#: Markers for dag_ids a keep-history retire removed the file of. Not a lifecycle
#: state — see :meth:`Journal.mark_retired`.
RETIRED = "retired"
_SUBDIRS = (PENDING, INFLIGHT, DONE, QUARANTINE, RETIRED)

#: How long a retired-dag_id marker suppresses the id in the manager list. It
#: only has to outlive Airflow's `dag_dir_list_interval` (~300 s by default, i.e.
#: how long a fileless DagModel row survives before the dag-processor marks it
#: stale); a day is three orders of magnitude of slack, and the marker self-heals
#: the moment a file for that id reappears.
RETIRED_TTL_S = 86_400

#: Step skips that mean "the clock ran out", i.e. the step was never attempted —
#: written by ``reconciler._expire_registration`` / ``_do_retire``. A retire
#: skipped for one of these reasons is still *owed*, which is what makes it
#: inheritable by a later deploy (:meth:`Journal.orphaned_retires`) and re-armable
#: by ``reconciler.reopen_expired``. Any other skip was a *decision* and must
#: survive.
BUDGET_SKIP_REASONS = ("the action budget elapsed", "the new DAG never registered")

#: Stamped over a budget skip once a newer deploy has taken the intent over, so
#: it is inherited exactly once and "Keep waiting" cannot re-arm it in parallel.
INHERITED_SKIP_REASON = "inherited by a newer deploy of this flow"

#: How far back :meth:`Journal.orphaned_retires` looks. Bounded so a very old
#: expired rename cannot suddenly retire a dag_id someone has since revived by
#: hand; entries are pruned at ``retention_s`` anyway.
ORPHANED_RETIRE_MAX_AGE_S = 86_400

#: Control requests recorded on a *claimed* entry by someone who is not its
#: holder (a cancel, a supersede, a pause veto). The holder cannot see them — it
#: is working from the in-memory copy it read at claim time — so they are merged
#: forward when it releases and honoured by ``reconciler.advance`` on the next
#: pass. Writing the request instead of the *result* is what makes this safe: two
#: writers never race over the step block, only over a flag that is idempotent.
_REQUEST_KEYS = ("stop_requested", "veto_unpause_requested")

VETO_REASON = "paused from Studio while the deploy was still in flight"

ENV_JOURNAL_DIR = "JUPYTERLAB_AIRFLOW_JOURNAL_DIR"


class InvalidEntry(ValueError):
    """A journal entry is malformed/forged — it is quarantined, never executed."""


class FutureEntry(ValueError):
    """The entry was written by a NEWER server. Left untouched (see JOURNAL_VERSION)."""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(moment: datetime) -> str:
    return moment.isoformat()


def parse_ts(raw: Any) -> datetime:
    """Parse a stored ISO-8601 timestamp, always tz-aware (naive → UTC)."""
    moment = datetime.fromisoformat(str(raw))
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment


# The server's data_dir, captured once at extension load. A module-level setter
# rather than passing `server_app` around: the reconciler runs in a background
# thread with no request and no handle on the application, and an admin who has
# relocated `ServerApp.data_dir` must get the journal there too.
_DATA_DIR: Optional[str] = None


def set_root(data_dir: Optional[str]) -> None:
    """Record the server's ``data_dir`` (called once from the extension loader)."""
    global _DATA_DIR
    _DATA_DIR = data_dir


def journal_dir() -> str:
    """Where entries live: env override > server ``data_dir`` > ``jupyter_data_dir()``.

    The env override exists for the JupyterHub/NFS case — ``os.replace`` is a
    weaker guarantee on NFS, so an operator may want the journal on local disk.
    """
    override = os.environ.get(ENV_JOURNAL_DIR, "").strip()
    if override:
        return os.path.abspath(override)
    base = _DATA_DIR
    if not base:
        from jupyter_core.paths import jupyter_data_dir

        base = jupyter_data_dir()
    return os.path.join(os.path.abspath(base), "airflow-studio", "deploy-journal")


_JOURNAL: Optional["Journal"] = None
_JOURNAL_LOCK = threading.Lock()


def get_journal() -> "Journal":
    """The process-wide journal. Rebuilt when the configured root changes, so a
    test (or an admin re-pointing the env var + restarting) never keeps writing
    to a stale directory."""
    global _JOURNAL
    with _JOURNAL_LOCK:
        root = journal_dir()
        if _JOURNAL is None or _JOURNAL.root != root:
            _JOURNAL = Journal(root)
        return _JOURNAL


class Journal:
    """The on-disk store. Pure filesystem + validation — no Airflow, no Tornado,
    no scheduling, so it is unit-testable standalone."""

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        for name in _SUBDIRS:
            os.makedirs(os.path.join(self.root, name), exist_ok=True)
        # The journal names DAGs a deploy is authorized to mutate; keep it owner-only.
        try:
            os.chmod(self.root, 0o700)
        except OSError:  # noqa: PERF203 - Windows/NFS may refuse; not fatal
            pass

    # -- paths -------------------------------------------------------------- #

    def _path(self, state: str, deploy_id: str) -> str:
        return os.path.join(self.root, state, f"{deploy_id}.json")

    def _listdir(self, state: str) -> List[str]:
        directory = os.path.join(self.root, state)
        try:
            return sorted(
                os.path.join(directory, name)
                for name in os.listdir(directory)
                if name.endswith(".json")
            )
        except OSError:
            return []

    # -- write protocol ----------------------------------------------------- #

    def _atomic_write(self, path: str, entry: Dict[str, Any]) -> None:
        """Temp file in the journal root (same filesystem as every subdir) →
        fsync → ``os.replace``. Owner-only, like the root."""
        payload = json.dumps(entry, default=str)
        fd, tmp = tempfile.mkstemp(dir=self.root, prefix=".afjournal-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.chmod(tmp, 0o600)
            _replace_with_retry(tmp, path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
        # Best effort: not every platform lets you fsync a directory, and a lost
        # rename degrades to "this deploy is not journaled", which is survivable.
        try:
            dir_fd = os.open(os.path.dirname(path), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)

    def _read(self, path: str) -> Dict[str, Any]:
        """Read + validate one entry file. Raises :class:`InvalidEntry` /
        :class:`FutureEntry`."""
        try:
            size = os.path.getsize(path)
        except OSError as err:
            raise InvalidEntry(f"unreadable: {err}") from err
        if size > MAX_ENTRY_BYTES:
            raise InvalidEntry(f"entry is {size} bytes (cap {MAX_ENTRY_BYTES})")
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError) as err:
            raise InvalidEntry(f"unparseable: {err}") from err
        entry = _validate(data)
        stem = os.path.splitext(os.path.basename(path))[0]
        if stem != entry["deploy_id"]:
            raise InvalidEntry("filename does not match deploy_id")
        if os.path.basename(os.path.dirname(path)) != INFLIGHT:
            # Only an `inflight/` copy can be claimed, so a token anywhere else is
            # residue (`resume`/`reclaim_stale` move the file wholesale). Leaving
            # it would make an unclaimed caller's write look conditional on a
            # claim nobody holds, and every such write would then be refused.
            entry.pop("claim_token", None)
        return entry

    # -- public API --------------------------------------------------------- #

    def put(self, entry: Dict[str, Any]) -> str:
        """Validate and atomically record a new entry in ``pending/``.

        **Only ``deploy.deploy_dag`` may call this.** That function is reachable
        only from ``DeployHandler.post``, which goes through the role gate in
        ``_AirflowHandler.respond`` — so an entry is, in capability terms, a token
        minted by an already-authorized action. The ``role_at_deploy`` refusal
        below turns that invariant into a runtime assertion instead of a comment:
        a future handler that journals work without passing the gate fails loudly.
        """
        validated = _validate(entry)
        if validated.get("role_at_deploy") == "viewer":
            raise ValueError(
                "refusing to journal a deploy made under the viewer role — a "
                "viewer cannot deploy at all, so this entry is forged"
            )
        self._atomic_write(self._path(PENDING, validated["deploy_id"]), validated)
        return validated["deploy_id"]

    def get(self, deploy_id: str) -> Optional[Dict[str, Any]]:
        """One entry by id, wherever it currently lives (pending → inflight → done)."""
        if not _DEPLOY_ID_RE.match(str(deploy_id or "")):
            return None
        for state in (PENDING, INFLIGHT, DONE):
            path = self._path(state, deploy_id)
            if not os.path.isfile(path):
                continue
            try:
                return self._read(path)
            except (InvalidEntry, FutureEntry):
                return None
        return None

    def list_pending(self) -> List[Dict[str, Any]]:
        """Every valid entry awaiting work. An unreadable/forged entry is
        quarantined here rather than raised, so one bad file cannot stop the
        sweep from finishing every good one."""
        entries: List[Dict[str, Any]] = []
        for path in self._listdir(PENDING):
            try:
                entries.append(self._read(path))
            except FutureEntry:
                continue  # a newer server owns it — leave it exactly where it is
            except InvalidEntry as err:
                self.quarantine(path, str(err))
        return entries

    def open_for(
        self, *, dag_id: Optional[str] = None, afdag_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Non-terminal entries (pending **and** inflight) for a DAG or a flow."""
        matches: List[Dict[str, Any]] = []
        for state in (PENDING, INFLIGHT):
            for path in self._listdir(state):
                try:
                    entry = self._read(path)
                except (InvalidEntry, FutureEntry):
                    continue
                if dag_id is not None and entry.get("dag_id") == dag_id:
                    matches.append(entry)
                elif afdag_id is not None and entry.get("afdag_id") == afdag_id:
                    matches.append(entry)
        return matches

    def latest_for_afdag(self, afdag_id: str) -> Optional[Dict[str, Any]]:
        """The most recent entry for a flow, terminal or not — what a reopened
        editor asks for to re-attach its banner after a reload."""
        if not afdag_id:
            return None
        best: Optional[Dict[str, Any]] = None
        for state in (PENDING, INFLIGHT, DONE):
            for path in self._listdir(state):
                try:
                    entry = self._read(path)
                except (InvalidEntry, FutureEntry):
                    continue
                if entry.get("afdag_id") != afdag_id:
                    continue
                if best is None or entry["created_at"] > best["created_at"]:
                    best = entry
        return best

    def claim(self, deploy_id: str) -> Optional[Dict[str, Any]]:
        """Take ownership of an entry: ``os.rename(pending → inflight)``, then
        stamp a fresh ``claim_token`` on it.

        The loser of a race gets ``FileNotFoundError`` on the *source* and gets
        ``None``. ``os.rename`` (never ``os.replace``) on purpose: the destination
        never pre-exists, so the POSIX-overwrite / Windows-raise divergence is off
        the path and the operation is portable.

        The rename alone only gives at-most-one *claimer*; it does not stop a
        concurrent request handler (a cancel, a supersede) from finalizing the
        entry the claimer is holding. The token closes that: :meth:`release` and
        :meth:`finish` are conditional on it, so a holder whose entry was taken
        away from it cannot write its stale copy back and resurrect a deploy that
        was cancelled or superseded seconds earlier.
        """
        source = self._path(PENDING, deploy_id)
        destination = self._path(INFLIGHT, deploy_id)
        try:
            os.rename(source, destination)
        except FileExistsError:
            # Windows only, and only from a crash that left both copies behind
            # (a normal race loses on the *source*, below). The pending copy is
            # the newer one, so drop the residue and take it.
            try:
                os.unlink(destination)
                os.rename(source, destination)
            except OSError:
                return None
        except OSError:
            return None
        try:
            entry = self._read(destination)
        except (InvalidEntry, FutureEntry) as err:
            self.quarantine(destination, str(err))
            return None
        entry["claim_token"] = uuid4().hex
        entry["updated_at"] = iso(utcnow())
        try:
            self._atomic_write(destination, entry)
        except OSError as err:
            # No token means no protected write later, so refuse the claim rather
            # than run unprotected. The entry stays in `inflight/` and
            # `reclaim_stale` returns it to `pending/`.
            _log.warning("could not stamp a claim token on %s: %s", deploy_id, err)
            return None
        return entry

    def release(self, entry: Dict[str, Any]) -> bool:
        """Persist progress and hand the entry back to ``pending/``."""
        return self._move_out(entry, PENDING)

    def finish(self, entry: Dict[str, Any], outcome: str) -> bool:
        """Mark an entry terminal and move it to ``done/`` (still observable by
        the editor until it is pruned)."""
        entry = dict(entry)
        entry["phase"] = "terminal"
        entry["outcome"] = outcome if outcome in _OUTCOMES else "failed"
        entry["terminal_at"] = iso(utcnow())
        return self._move_out(entry, DONE)

    def reopen(self, entry: Dict[str, Any]) -> bool:
        """Put a (re-armed) terminal entry back into ``pending/``. Used by the
        editor's "Keep waiting" after an ``expired`` outcome — see
        ``reconciler.reopen_expired``, which owns the re-arming rules."""
        return self._move_out(entry, PENDING)

    def _move_out(self, entry: Dict[str, Any], state: str) -> bool:
        """Write ``entry`` into ``state`` and drop every other copy.

        When the entry carries a ``claim_token`` the write is **conditional**: the
        ``inflight/`` copy must still exist and still carry that token. If it does
        not, another writer finalized the entry while we held it and *its* version
        is authoritative — we refuse rather than overwrite. Any control request it
        recorded on our copy is merged forward, so a cancel or a pause veto that
        landed mid-flight survives the release instead of being clobbered by the
        holder's stale in-memory copy.
        """
        entry = dict(entry)
        deploy_id = entry["deploy_id"]
        token = entry.pop("claim_token", None)
        if token is not None:
            try:
                current = self._read(self._path(INFLIGHT, deploy_id))
            except (InvalidEntry, FutureEntry, OSError):
                current = None
            if current is None or current.get("claim_token") != token:
                _log.warning(
                    "deploy %s was taken over while it was claimed — discarding "
                    "the stale copy rather than resurrecting it",
                    deploy_id,
                )
                return False
            for key in _REQUEST_KEYS:
                # Only a request the holder never saw. A request it already
                # consumed leaves the key present-but-falsy on its copy, which is
                # what keeps it from being merged back in forever.
                if current.get(key) and key not in entry:
                    entry[key] = current[key]
        entry["updated_at"] = iso(utcnow())
        self._atomic_write(self._path(state, deploy_id), entry)
        for other in (PENDING, INFLIGHT, DONE):
            if other == state:
                continue
            path = self._path(other, deploy_id)
            if os.path.isfile(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
        return True

    def request_stop(
        self, deploy_id: str, *, outcome: str, message: str
    ) -> Optional[str]:
        """Stop an **open** entry, whether or not a sweep currently holds it.

        Returns ``"finished"`` (it was idle: finalized on the spot),
        ``"requested"`` (a sweep holds it: the request is durably recorded and the
        holder honours it before its next transition), or ``None`` (no open entry).

        Blind-writing over a claimed entry is what the ``claim_token`` now
        forbids, and it is the right prohibition: the holder may be several
        seconds into an Airflow call and its in-memory copy would otherwise be
        written back on release, undoing the stop.
        """
        if outcome not in _OUTCOMES:
            raise ValueError(f"bad stop outcome {outcome!r}")
        for state in (PENDING, INFLIGHT):
            path = self._path(state, deploy_id)
            if not os.path.isfile(path):
                continue
            try:
                entry = self._read(path)
            except (InvalidEntry, FutureEntry):
                continue  # unreadable, or claimed out from under us mid-read
            if entry.get("outcome"):
                return None
            if state == PENDING:
                entry["message"] = message
                # Unconditional: a pending entry has no holder. If a sweep claims
                # it in this instant, that sweep's release finds the `done/` copy
                # and is refused — the stop still wins.
                return "finished" if self.finish(entry, outcome) else None
            entry["stop_requested"] = {"outcome": outcome, "message": message}
            entry["updated_at"] = iso(utcnow())
            self._atomic_write(path, entry)
            return "requested"
        return None

    def veto_unpause(self, dag_id: str) -> int:
        """A user paused this DAG — cancel the pending unpause **and** trigger on
        every open entry for it.

        Deliberate intent beats a deploy still in flight: better to leave a DAG
        paused (visible, one click to fix) than to unpause something a human just
        stopped. An entry a sweep currently holds gets the veto recorded as a
        *request* rather than as a step edit, because the holder would otherwise
        write its own (pre-veto) step block back on release and the unpause would
        happen anyway.
        """
        touched = 0
        for state in (PENDING, INFLIGHT):
            for path in self._listdir(state):
                try:
                    entry = self._read(path)
                except (InvalidEntry, FutureEntry):
                    continue
                if entry.get("dag_id") != dag_id or entry.get("outcome"):
                    continue
                changed = apply_unpause_veto(entry)
                if state == INFLIGHT and not entry.get("veto_unpause_requested"):
                    entry["veto_unpause_requested"] = True
                    changed = True
                if changed:
                    entry["updated_at"] = iso(utcnow())
                    self._atomic_write(path, entry)
                    touched += 1
        return touched

    def supersede(
        self, *, dag_id: str, afdag_id: str, except_deploy_id: str
    ) -> List[Dict[str, Any]]:
        """Finish every other open entry for this flow as ``superseded``, and
        return them so the new deploy can inherit an unfinished retire intent.

        A superseded entry a sweep is holding is stopped through
        :meth:`request_stop`, so the loser cannot be resurrected by that sweep's
        release and go on to unpause and trigger the ``dag_id`` this deploy just
        renamed away from.
        """
        superseded: List[Dict[str, Any]] = []
        # An `.afdag` without provenance (a pre-provenance file) can still be
        # matched by the dag_id it deploys — never by nothing at all.
        candidates = (
            self.open_for(afdag_id=afdag_id) if afdag_id else self.open_for(dag_id=dag_id)
        )
        for entry in candidates:
            if entry["deploy_id"] == except_deploy_id:
                continue
            message = "Superseded by a newer deploy of this flow."
            if self.request_stop(
                entry["deploy_id"], outcome="superseded", message=message
            ):
                entry["message"] = message
                superseded.append(entry)
        return superseded

    # -- retired dag_ids ---------------------------------------------------- #
    # A keep-history retire deletes `{dag_id}.py` and pauses the DAG, but it
    # deliberately does NOT delete the DagModel row (that is what "keep history"
    # means). Airflow goes on listing that row until its dag-processor notices the
    # file is gone and marks it stale — one full `dag_dir_list_interval`, ~300 s
    # by default, and never at all if the deploy target is not the directory the
    # processor scans. For those minutes the manager shows the OLD id beside the
    # new one, which is exactly the "the rename did nothing" report. These markers
    # let the list drop the id the moment Studio retires it, instead of waiting on
    # Airflow to agree.

    def _retired_path(self, dag_id: str) -> str:
        return os.path.join(self.root, RETIRED, f"{dag_id}.json")

    def mark_retired(self, dag_id: str, *, now: Optional[datetime] = None) -> bool:
        """Record that Studio retired ``dag_id`` (history kept)."""
        if not _DAG_ID_RE.match(str(dag_id or "")):
            return False
        try:
            self._atomic_write(
                self._retired_path(dag_id),
                {"dag_id": dag_id, "at": iso(now or utcnow())},
            )
        except OSError as err:  # noqa: PERF203 - never fail a retire over a marker
            _log.warning("could not record the retired dag_id %s: %s", dag_id, err)
            return False
        return True

    def clear_retired(self, dag_id: str) -> bool:
        """Forget a marker: this dag_id is live again (re-deployed, or renamed
        back to). Called on every deploy, so the suppression can never outlive the
        condition that justified it."""
        if not _DAG_ID_RE.match(str(dag_id or "")):
            return False
        try:
            os.unlink(self._retired_path(dag_id))
        except OSError:
            return False
        return True

    def retired_ids(
        self, *, now: Optional[datetime] = None, ttl_s: int = RETIRED_TTL_S
    ) -> Set[str]:
        """The dag_ids Studio has retired recently. Expired markers are pruned
        here rather than by a separate timer: the TTL is a backstop, so a marker
        can never hide a dag_id indefinitely if every other release path fails."""
        now = now or utcnow()
        ids: Set[str] = set()
        for path in self._listdir(RETIRED):
            dag_id = os.path.splitext(os.path.basename(path))[0]
            if not _DAG_ID_RE.match(dag_id):
                continue
            try:
                with open(path, encoding="utf-8") as fh:
                    age = (now - parse_ts(json.load(fh)["at"])).total_seconds()
            except (OSError, ValueError, KeyError, TypeError):
                age = ttl_s + 1  # unreadable marker: drop it rather than trust it
            if age > ttl_s:
                try:
                    os.unlink(path)
                except OSError:
                    pass
                continue
            ids.add(dag_id)
        return ids

    # -- stranded retire intents -------------------------------------------- #

    def orphaned_retires(
        self,
        *,
        afdag_id: str,
        dag_id: str,
        now: Optional[datetime] = None,
        max_age_s: int = ORPHANED_RETIRE_MAX_AGE_S,
    ) -> List[Dict[str, Any]]:
        """Terminal entries of this flow that still OWE a retire.

        :meth:`supersede` can only inherit from *open* entries, so a rename whose
        deploy ran out of budget left its retire intent in a ``done/`` entry where
        nothing could ever pick it up again — the old dag_id stayed live and
        unpaused, and ``reconciler.reopen_expired`` then refused to re-arm it
        because the newer deploy was in flight. A later deploy of the same flow
        is exactly the right moment to take that intent over.

        Only *budget* skips qualify (see :data:`BUDGET_SKIP_REASONS`): a retire
        skipped because the new DAG failed to import, or because the file turned
        out to belong to another flow, was a decision, not a timeout.
        """
        now = now or utcnow()
        found: List[Dict[str, Any]] = []
        for path in self._listdir(DONE):
            try:
                entry = self._read(path)
            except (InvalidEntry, FutureEntry):
                continue
            if afdag_id:
                if entry.get("afdag_id") != afdag_id:
                    continue
            elif entry.get("dag_id") != dag_id:
                continue
            step = entry["steps"]["retire"]
            if step.get("state") != "skipped":
                continue
            if step.get("skipped_reason") not in BUDGET_SKIP_REASONS:
                continue
            if not retire_intents(entry):
                continue
            try:
                stamp = parse_ts(entry.get("terminal_at") or entry["updated_at"])
            except (KeyError, TypeError, ValueError):
                continue
            if (now - stamp).total_seconds() > max_age_s:
                continue
            found.append(entry)
        found.sort(key=lambda item: item["created_at"])
        return found

    def consume_orphaned_retire(self, deploy_id: str) -> bool:
        """Mark a stranded intent as taken over, so it is inherited exactly once
        and "Keep waiting" cannot re-arm a retire a newer deploy now owns."""
        path = self._path(DONE, deploy_id)
        try:
            entry = self._read(path)
        except (InvalidEntry, FutureEntry, OSError):
            return False
        step = entry["steps"]["retire"]
        if step.get("skipped_reason") not in BUDGET_SKIP_REASONS:
            return False
        step["skipped_reason"] = INHERITED_SKIP_REASON
        entry["updated_at"] = iso(utcnow())
        try:
            self._atomic_write(path, entry)
        except OSError:
            return False
        return True

    def resume(self) -> int:
        """Crash recovery: after a restart nothing can still hold a claim, so
        every ``inflight/`` entry goes back to ``pending/``. Sound only because
        each step is independently idempotent — we cannot know whether the crash
        landed before or after a mutating call reached Airflow."""
        moved = 0
        for path in self._listdir(INFLIGHT):
            destination = os.path.join(self.root, PENDING, os.path.basename(path))
            try:
                _replace_with_retry(path, destination)
                moved += 1
            except OSError as err:
                _log.warning("could not resume journal entry %s: %s", path, err)
        return moved

    def reclaim_stale(self, now: datetime, stale_s: int = 600) -> int:
        """Recover a claim stranded by a crashed/killed sweep inside a live
        process. 600 s is comfortably above the worst single-entry cost (a few
        60 s Airflow calls plus a 60 s ``git push``)."""
        moved = 0
        for path in self._listdir(INFLIGHT):
            try:
                entry = self._read(path)
                age = (now - parse_ts(entry["updated_at"])).total_seconds()
            except (InvalidEntry, FutureEntry, ValueError):
                continue
            if age < stale_s:
                continue
            destination = os.path.join(self.root, PENDING, os.path.basename(path))
            try:
                _replace_with_retry(path, destination)
                moved += 1
            except OSError:
                continue
        return moved

    def prune(self, now: datetime, retention_s: int) -> int:
        """Drop finished entries past the retention window. They exist only so a
        reopened editor can say "deployed while you were away"."""
        removed = 0
        for path in self._listdir(DONE):
            try:
                entry = self._read(path)
                stamp = entry.get("terminal_at") or entry["updated_at"]
                age = (now - parse_ts(stamp)).total_seconds()
            except (InvalidEntry, FutureEntry, ValueError):
                age = retention_s + 1  # unreadable + finished: nothing to preserve
            if age <= retention_s:
                continue
            try:
                os.unlink(path)
                removed += 1
            except OSError:
                pass
        return removed

    def quarantine(self, path: str, reason: str) -> None:
        """Move a corrupt/forged entry aside. Never executed, never deleted — it
        is forensic evidence, and the sweep must not trip over it again."""
        destination = os.path.join(self.root, QUARANTINE, os.path.basename(path))
        if os.path.exists(destination):
            destination = f"{destination}.{int(time.time() * 1000)}"
        try:
            os.rename(path, destination)
        except OSError as err:
            _log.error("could not quarantine journal entry %s: %s", path, err)
            return
        _log.error("quarantined journal entry %s (%s)", os.path.basename(path), reason)


def _replace_with_retry(source: str, destination: str, attempts: int = 3) -> None:
    """``os.replace`` with a short retry.

    On Windows an antivirus/indexer holding a handle on the destination raises
    ``PermissionError`` for a few tens of milliseconds; surfacing that as a
    journal failure would drop durability for no reason.
    """
    for attempt in range(attempts):
        try:
            os.replace(source, destination)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(0.05)


def _retire_intent(raw: Any, field: str) -> Dict[str, Any]:
    """Normalize + strictly check one ``{dag_id, purge}`` retire intent.

    The ``dag_id`` is interpolated straight into an Airflow API path (and into a
    ``{dag_id}.py`` the retire *deletes*), so this is a security check.
    """
    if not isinstance(raw, dict):
        raise InvalidEntry(f"{field} must contain objects")
    dag_id = str(raw.get("dag_id", ""))
    if not _DAG_ID_RE.match(dag_id):
        raise InvalidEntry(f"bad {field}.dag_id {dag_id!r}")
    return {"dag_id": dag_id, "purge": bool(raw.get("purge"))}


def retire_intents(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every dag_id this deploy still means to retire, primary first, deduped.

    The single ``retire`` slot is kept as the primary intent (so an entry written
    by an older server reads back unchanged) and ``retire_also`` carries the ones
    inherited from deploys this one superseded.
    """
    intents: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for raw in [entry.get("retire")] + list(entry.get("retire_also") or []):
        if not isinstance(raw, dict):
            continue
        dag_id = str(raw.get("dag_id") or "")
        if not dag_id or dag_id in seen:
            continue
        seen.add(dag_id)
        intents.append({"dag_id": dag_id, "purge": bool(raw.get("purge"))})
    return intents


def _validate(data: Any) -> Dict[str, Any]:
    """Normalize + strictly check one entry. Unknown keys are kept (forward
    compatibility); every field the reconciler acts on is checked."""
    if not isinstance(data, dict):
        raise InvalidEntry("entry is not a JSON object")
    version = data.get("version")
    if not isinstance(version, int):
        raise InvalidEntry(f"bad version {version!r}")
    if version > JOURNAL_VERSION:
        raise FutureEntry(f"version {version} was written by a newer server")
    if version < JOURNAL_VERSION:
        raise InvalidEntry(f"unsupported version {version}")

    entry = dict(data)
    deploy_id = str(entry.get("deploy_id", ""))
    if not _DEPLOY_ID_RE.match(deploy_id):
        raise InvalidEntry(f"bad deploy_id {deploy_id!r}")
    entry["deploy_id"] = deploy_id

    dag_id = str(entry.get("dag_id", ""))
    if not _DAG_ID_RE.match(dag_id):
        raise InvalidEntry(f"bad dag_id {dag_id!r}")
    entry["dag_id"] = dag_id

    filename = str(entry.get("filename", ""))
    if filename != f"{dag_id}.py":
        raise InvalidEntry(f"filename {filename!r} does not match dag_id {dag_id!r}")
    entry["filename"] = filename

    phase = str(entry.get("phase", ""))
    if phase not in _PHASES:
        raise InvalidEntry(f"bad phase {phase!r}")
    entry["phase"] = phase

    outcome = entry.get("outcome")
    if outcome is not None and outcome not in _OUTCOMES:
        raise InvalidEntry(f"bad outcome {outcome!r}")

    retire = entry.get("retire")
    if retire is not None:
        entry["retire"] = _retire_intent(retire, "retire")
    # Additional dag_ids this deploy owes a retire for — a rename chained on top
    # of a rename whose deploy had not finished yet. One slot used to mean the
    # second intent was dropped, leaving that dag_id deployed AND unpaused beside
    # the new one; a list means an inherited intent is queued instead.
    extra = entry.get("retire_also")
    if extra is not None:
        if not isinstance(extra, list):
            raise InvalidEntry("retire_also must be a list or null")
        entry["retire_also"] = [_retire_intent(item, "retire_also") for item in extra]

    for field in ("created_at", "updated_at", "deadline_at", "action_deadline_at",
                  "next_attempt_at"):
        try:
            parse_ts(entry[field])
        except (KeyError, TypeError, ValueError) as err:
            raise InvalidEntry(f"bad {field}: {err}") from err

    entry["run_on_deploy"] = bool(entry.get("run_on_deploy"))
    entry["polls"] = int(entry.get("polls") or 0)
    entry["user"] = str(entry.get("user") or "unknown")
    entry["afdag_id"] = str(entry.get("afdag_id") or "")
    entry["run_id"] = str(entry.get("run_id") or "")
    entry["message"] = str(entry.get("message") or "")

    # A recorded stop request decides an OUTCOME, so it is checked as strictly as
    # `outcome` itself — a forged entry must not be able to name an arbitrary one.
    stop = entry.get("stop_requested")
    if stop is not None:
        if not isinstance(stop, dict) or stop.get("outcome") not in _OUTCOMES:
            raise InvalidEntry(f"bad stop_requested {stop!r}")
        entry["stop_requested"] = {
            "outcome": stop["outcome"],
            "message": str(stop.get("message") or ""),
        }
    if "veto_unpause_requested" in entry:
        entry["veto_unpause_requested"] = bool(entry["veto_unpause_requested"])

    steps = entry.get("steps")
    if not isinstance(steps, dict):
        raise InvalidEntry("steps must be an object")
    normalized: Dict[str, Any] = {}
    for name in _STEPS:
        step = steps.get(name)
        if not isinstance(step, dict):
            raise InvalidEntry(f"steps.{name} must be an object")
        state = str(step.get("state", ""))
        if state not in _STEP_STATES:
            raise InvalidEntry(f"bad steps.{name}.state {state!r}")
        normalized[name] = dict(step)
        normalized[name]["state"] = state
        normalized[name]["attempts"] = int(step.get("attempts") or 0)
    entry["steps"] = normalized
    return entry


def apply_unpause_veto(entry: Dict[str, Any]) -> bool:
    """Skip the still-pending ``unpause``/``trigger`` steps. Idempotent, so it is
    safe to apply again when a recorded veto request is honoured later."""
    changed = False
    for name in ("unpause", "trigger"):
        step = entry["steps"][name]
        if step["state"] == "pending":
            step["state"] = "skipped"
            step["skipped_reason"] = VETO_REASON
            changed = True
    return changed


def new_steps() -> Dict[str, Any]:
    """A fresh, all-pending step block."""
    return {
        name: {
            "state": "pending",
            "attempts": 0,
            "last_error": None,
            "at": None,
            "skipped_reason": None,
        }
        for name in _STEPS
    }
