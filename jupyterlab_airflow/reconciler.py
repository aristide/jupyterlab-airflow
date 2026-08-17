"""The deploy reconciler: the server, not the browser, finishes a deploy (PRD §6.5.4).

A deploy's tail — *wait for Airflow to register the file → retire the dag_id the
flow was renamed away from → unpause → trigger* — used to run entirely inside the
open `.afdag` widget. Closing the tab 85 seconds before Airflow finished its
directory scan left the old DAG live and unpaused and the new one paused forever,
with the retire intent living in a React ref that died with the page. This module
performs that tail from the journal instead, so the browser only ever *observes*.

Two layers, deliberately:

* :func:`advance` / :func:`sweep_once` are **plain synchronous functions** with
  ``now`` / ``client`` / ``target`` / ``journal`` injected. All the logic lives
  here, and every failure case is testable without a clock, a server, or a sleep.
* :class:`DeployReconciler` is the only part that touches the IOLoop. It is
  **lazily scheduled**: the ``PeriodicCallback`` exists only while ``pending/``
  is non-empty, so a server that never deploys creates no timer and makes no
  Airflow calls, and an idle server goes quiet within one interval.

Everything blocking (``os.listdir``, JSON I/O, the synchronous ``AirflowClient``,
a ``git push`` inside a retire) runs on one **dedicated** single-thread executor —
mirroring ``_AirflowHandler.run``'s discipline, but on its own pool so a sweep
stalled on an unreachable git remote cannot starve interactive requests.
"""

from __future__ import annotations

import atexit
import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from tornado.ioloop import IOLoop, PeriodicCallback

from .audit import audit_event
from .client import AirflowError, get_client
from .config import AirflowConfig, can_edit, studio_role
from .deploy import (
    DeployError,
    _parse_header,
    deploy_target_kind,
    get_deploy_target,
    retire_old_dag,
)
from .journal import (
    BUDGET_SKIP_REASONS,
    VETO_REASON,
    apply_unpause_veto,
    get_journal,
    iso,
    parse_ts,
    retire_intents,
    utcnow,
)

_log = logging.getLogger(__name__)

# -- configuration (env only, matching config.py's convention) --------------- #
# `JUPYTERLAB_AIRFLOW_*` means "how this server behaves" (precedent:
# JUPYTERLAB_AIRFLOW_ROLE); `AIRFLOW_*` means "how to reach Airflow".
ENV_ENABLED = "JUPYTERLAB_AIRFLOW_RECONCILER"
ENV_INTERVAL = "JUPYTERLAB_AIRFLOW_RECONCILE_INTERVAL_S"
ENV_BUDGET = "JUPYTERLAB_AIRFLOW_DEPLOY_BUDGET_S"
ENV_RETENTION = "JUPYTERLAB_AIRFLOW_JOURNAL_RETENTION_S"

DEFAULT_INTERVAL_S, MIN_INTERVAL_S, MAX_INTERVAL_S = 15, 5, 300
# 900s, not the widget's 420s: the browser budget is bounded by user patience,
# the server's by how long Airflow can legitimately take. `dag_dir_list_interval`
# defaults to ~300s and the measured failure registered at 296s — one full scan
# cycle. 900s covers three, plus parse and serialize.
DEFAULT_BUDGET_S, MIN_BUDGET_S, MAX_BUDGET_S = 900, 60, 7200
DEFAULT_RETENTION_S, MIN_RETENTION_S, MAX_RETENTION_S = 86_400, 300, 604_800

#: Entries advanced per sweep. Bounded so a backlog cannot monopolise the thread.
MAX_ENTRIES_PER_SWEEP = 8
#: Per-step attempt cap, alongside the deadline: a permanent 4xx must not burn
#: the whole budget logging one error every two minutes.
MAX_ATTEMPTS = 40
#: Expiry needs BOTH the wall-clock deadline and a few observations, so a forward
#: NTP jump cannot abandon a migration that has barely started.
MIN_POLLS_BEFORE_EXPIRY = 3
#: Extra time granted once the DAG is registered, so a DAG discovered at minute
#: 14 still gets its remaining steps.
POST_REGISTRATION_GRACE_S = 300
_BACKOFF_BASE_S, _BACKOFF_MAX_S = 15, 120


def _env_int(name: str, default: int, low: int, high: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        _log.warning("%s=%r is not an integer — using %s", name, raw, default)
        return default
    if value < low or value > high:
        clamped = min(max(value, low), high)
        _log.warning("%s=%s is out of range [%s, %s] — using %s", name, value, low, high, clamped)
        return clamped
    return value


def is_enabled() -> bool:
    """The kill switch. ``off`` means: no timer, no pool, and ``deploy_dag``
    writes no journal entry — the editor performs exactly as it did before."""
    raw = os.environ.get(ENV_ENABLED, "on").strip().lower()
    return raw not in ("off", "0", "false", "no")


def interval_s() -> int:
    return _env_int(ENV_INTERVAL, DEFAULT_INTERVAL_S, MIN_INTERVAL_S, MAX_INTERVAL_S)


def deploy_budget_s() -> int:
    """How long a deploy's lifecycle may take. Sets both ``deadline_at`` (waiting
    for registration) and ``action_deadline_at`` (past it, never unpause or
    trigger — an unpause arriving 40 minutes late is not "completing a deploy",
    it is fighting the user)."""
    return _env_int(ENV_BUDGET, DEFAULT_BUDGET_S, MIN_BUDGET_S, MAX_BUDGET_S)


def retention_s() -> int:
    return _env_int(ENV_RETENTION, DEFAULT_RETENTION_S, MIN_RETENTION_S, MAX_RETENTION_S)


# --------------------------------------------------------------------------- #
# Audit
# --------------------------------------------------------------------------- #
def _audit(entry: Dict[str, Any], action: str, outcome: str, *, dag_id: str,
           detail: Optional[str] = None) -> None:
    """One audit record for something the reconciler did to Airflow (PRD §9).

    ``user`` is the human who clicked Deploy — never ``"system"``. The trail
    exists to answer *who*, and "the server did it" is not an answer; the
    reconciler is a mechanism, not a principal. ``via="reconciler"`` says so in a
    structured field, so a SIEM can filter background-completed actions without
    string-matching a message. ``correlation_id`` is the deploy's, which is also
    the id stamped into the deployed `.py` header (§8.9/§10) — one grep yields the
    deploy, the retire, the unpause and the trigger, across a reload and a restart.
    """
    try:
        audit_event(
            action,
            user=entry.get("user") or "unknown",
            correlation_id=entry["deploy_id"],
            dag_id=dag_id,
            outcome=outcome,
            detail=detail,
            via="reconciler",
        )
    except Exception:  # noqa: BLE001 - same posture as respond._audit
        _log.exception("audit emission failed (action=%s)", action)


# --------------------------------------------------------------------------- #
# Step machine (pure)
# --------------------------------------------------------------------------- #
def _step(entry: Dict[str, Any], name: str) -> Dict[str, Any]:
    return entry["steps"][name]


def _quote_ids(intents: List[Dict[str, Any]]) -> str:
    """``“a”`` / ``“a”, “b”`` — the retire targets as they read in a message."""
    return ", ".join(f"“{intent['dag_id']}”" for intent in intents)


def _next_phase(entry: Dict[str, Any]) -> str:
    """Commit-last ordering (invariant I1).

    ``quiesce`` (pause the old DAG) takes the position ``retire`` used to hold,
    so the double-scheduling hazard that motivated retiring early is still
    closed before the new DAG is unpaused — but by a *reversible* act. The
    irreversible half (deleting the old ``.py``, optionally its history) is the
    LAST thing the lifecycle does, once the new DAG is actually live. An entry
    abandoned midway therefore leaves the old DAG paused and recoverable rather
    than deleted.
    """
    if retire_intents(entry) and _step(entry, "quiesce")["state"] == "pending":
        return "quiescing"
    # A *skipped* unpause (a user pause vetoed it) still routes through the
    # unpause phase — that is where the veto's "left paused at your request"
    # outcome is decided, and skipping past it would report a plain success.
    if _step(entry, "unpause")["state"] != "done":
        return "unpausing"
    if _step(entry, "trigger")["state"] == "pending":
        return "triggering"
    if retire_intents(entry) and _step(entry, "retire")["state"] == "pending":
        return "retiring"
    return "terminal"


def _backoff(entry: Dict[str, Any], now, step_name: Optional[str] = None,
             err: Optional[BaseException] = None) -> Dict[str, Any]:
    """Schedule the next attempt with jittered exponential backoff.

    The jitter matters when a restart reclaims a dozen entries that would
    otherwise stampede Airflow in lockstep.
    """
    attempts = 1
    if step_name:
        step = _step(entry, step_name)
        step["attempts"] = int(step.get("attempts") or 0) + 1
        if err is not None:
            step["last_error"] = str(err)[:500]
        attempts = step["attempts"]
    delay = min(_BACKOFF_BASE_S * (2 ** max(attempts - 1, 0)), _BACKOFF_MAX_S)
    delay = delay * random.uniform(0.8, 1.2)
    entry["next_attempt_at"] = iso(now + timedelta(seconds=delay))
    entry["updated_at"] = iso(now)
    return entry


def _finish(entry: Dict[str, Any], outcome: str, message: str, now) -> Tuple[Dict[str, Any], bool]:
    entry["outcome"] = outcome
    entry["phase"] = "terminal"
    entry["message"] = message
    entry["updated_at"] = iso(now)
    return entry, True


#: Outcomes whose plain meaning is "this deploy stopped and nothing further is
#: needed". Honest only while nothing irreversible has happened.
_ABANDON_OUTCOMES = ("superseded", "expired", "denied", "cancelled")


def _committed(entry: Dict[str, Any]) -> bool:
    """Whether this entry has already done something it cannot undo.

    Only the *irreversible* half counts. A ``quiesce`` (the old DAG paused) is
    not committing: unpausing it restores the previous state exactly.
    """
    result = _step(entry, "retire").get("result") or {}
    return bool(result.get("removed_file") or result.get("purged_history"))


def _terminate(entry: Dict[str, Any], outcome: str, message: str, now
               ) -> Tuple[Dict[str, Any], bool]:
    """The only way an entry may reach a terminal state (invariant I2).

    A deploy that has already destroyed something must never close with an
    outcome that reads as "nothing further was done" — that sentence is how the
    real incident presented: the old DAG was purged, the new one was left paused
    and untriggered, and the entry declared itself merely ``superseded``. Such an
    entry becomes ``needs_repair`` instead, names both dag_ids, and is always
    audited, because a half-finished migration is exactly what an operator needs
    told rather than filed away as a normal stop.
    """
    if outcome in _ABANDON_OUTCOMES and _committed(entry):
        retired = ", ".join(
            f"“{i['dag_id']}”" for i in retire_intents(entry)
        ) or "the old DAG"
        message = (
            f"{retired} was retired, but “{entry['dag_id']}” was left "
            f"unfinished — {message}"
        )
        outcome = "needs_repair"
        _audit(entry, "retire", "error", dag_id=entry["dag_id"], detail=message)
    return _finish(entry, outcome, message, now)


def _is_retryable(err: BaseException, step_name: str, entry: Dict[str, Any]) -> bool:
    """Whether a failed step should be tried again (see the PR's error table).

    Note ``client._request`` raises ``status=0`` for a transport failure, so an
    unreachable Airflow is distinguishable from a rejection.
    """
    if isinstance(err, DeployError):
        # Wrong git branch, rejected push, missing S3 bucket, unmanaged filename:
        # config problems that will not fix themselves. Stop and tell the user.
        return False
    if isinstance(err, AirflowError):
        if err.status == 0 or err.status >= 500:
            return True
        if err.status == 401:
            # The client already refreshes its JWT once internally, so a SECOND
            # 401 means the credentials are wrong — retrying forever is noise.
            step = _step(entry, step_name)
            if step.get("auth_retried"):
                return False
            step["auth_retried"] = True
            return True
        if err.status == 404 and step_name in ("unpause", "trigger"):
            # The DAG was deleted out of band; there is nothing left to complete.
            return False
        return True
    # An unexpected exception degrades to "keeps trying, then expires" rather
    # than "gives up on the first hiccup".
    return True


def _fail_step(entry: Dict[str, Any], step_name: str, err: BaseException, now,
               action: str, dag_id: str) -> Tuple[Dict[str, Any], bool]:
    step = _step(entry, step_name)
    step["state"] = "failed"
    step["last_error"] = str(err)[:500]
    step["at"] = iso(now)
    _audit(entry, action, "error", dag_id=dag_id, detail=str(err))
    return _finish(entry, "failed", f"{step_name} failed: {err}", now)


def _supersession(entry: Dict[str, Any], target) -> str:
    """``live`` | ``superseded`` | ``unknown`` — is this entry still about the
    deploy that is actually on disk?

    One check that retires a whole family of wrong-action bugs: the file was
    ``rm``'d, undeployed, purged as an orphan, rolled back to the `.bak`, or
    overwritten by a newer deploy — and the "am I even talking to the same
    Airflow / the same deploy target?" cases, where finishing a lifecycle would
    act on a completely different system.

    **Ambiguity is not supersession.** A read failure returns ``unknown`` and the
    entry simply waits: only definite evidence licenses a decision.
    """
    try:
        if entry.get("airflow_base_url") and entry["airflow_base_url"] != AirflowConfig.from_env().base_url:
            return "superseded"
        if entry.get("target_kind") and entry["target_kind"] != deploy_target_kind():
            return "superseded"
        if not target.exists(entry["filename"]):
            return "superseded"
        header = _parse_header(target.read(entry["filename"]))
    except Exception:  # noqa: BLE001 - a read failure is ambiguity, not evidence
        return "unknown"
    if not header:
        return "superseded"
    # Only Studio writes these files and every one carries a stamped
    # correlation_id, so "missing" genuinely means "not the deploy we journaled".
    if header.get("correlation_id") != entry["deploy_id"]:
        return "superseded"
    return "live"


def _superseded(entry: Dict[str, Any], now) -> Tuple[Dict[str, Any], bool]:
    return _finish(
        entry,
        "superseded",
        "The deployed file is no longer this deploy — nothing further was done.",
        now,
    )


def advance(entry: Dict[str, Any], *, now, client, target, log
            ) -> Tuple[Dict[str, Any], bool]:
    """Move one entry forward by at most one state transition.

    The step order is byte-for-byte what the widget did, so the behaviour is
    identical whichever actor finishes the deploy::

        awaiting_registration → retiring → unpausing → triggering → terminal

    Control requests recorded while the entry was claimed (a cancel, a
    supersede, a pause veto — see ``journal._REQUEST_KEYS``) are honoured
    **first**, before any transition: they were written by a request handler that
    could not safely edit an entry a sweep was holding, and this is where they
    take effect.
    """
    # Marked consumed (present-but-falsy) rather than deleted: an absent key means
    # "the holder never saw this request", which is what `Journal._move_out` uses
    # to decide whether to merge one forward.
    stop = entry.get("stop_requested")
    if stop:
        entry["stop_requested"] = None
        log.info("deploy %s stopped: %s", entry["deploy_id"], stop.get("outcome"))
        return _terminate(entry, stop["outcome"], str(stop.get("message") or ""), now)
    if entry.get("veto_unpause_requested"):
        entry["veto_unpause_requested"] = False
        apply_unpause_veto(entry)

    phase = entry.get("phase")
    if phase == "awaiting_registration":
        return _do_registration(entry, now, client, target, log)
    if phase == "quiescing":
        return _do_quiesce(entry, now, client, target, log)
    if phase == "unpausing":
        return _do_unpause(entry, now, client, target, log)
    if phase == "triggering":
        return _do_trigger(entry, now, client, target, log)
    if phase == "retiring":
        return _do_retire(entry, now, client, target, log)
    return _terminate(
        entry, entry.get("outcome") or "completed", entry.get("message", ""), now
    )


def _expire_registration(entry: Dict[str, Any], now, log) -> Tuple[Dict[str, Any], bool]:
    """Timing out is a diagnosis, never a licence to act.

    In particular the old ``dag_id`` of a rename is **not** retired on timeout —
    the same invariant the widget's poll documented ("retiring is gated on
    actually observing `registered`"). The residue is dangerous though (the old
    DAG is still live and unpaused), so it is audited rather than left silent.
    """
    intents = retire_intents(entry)
    message = (
        f"Airflow did not register {entry['dag_id']} within the deploy budget."
    )
    if intents and _step(entry, "retire")["state"] == "pending":
        _step(entry, "retire")["skipped_reason"] = "the new DAG never registered"
        _step(entry, "retire")["state"] = "skipped"
        was = "was" if len(intents) == 1 else "were"
        message += f" {_quote_ids(intents)} {was} left active."
        for intent in intents:
            _audit(
                entry,
                "retire",
                "error",
                dag_id=intent["dag_id"],
                detail=(
                    f"deploy of {entry['dag_id']} never registered within the deploy "
                    "budget; old DAG left active"
                ),
            )
    log.warning("deploy %s expired: %s", entry["deploy_id"], message)
    return _finish(entry, "expired", message, now)


def _do_registration(entry, now, client, target, log):
    deadline = parse_ts(entry["deadline_at"])
    if now > deadline and int(entry.get("polls") or 0) >= MIN_POLLS_BEFORE_EXPIRY:
        return _expire_registration(entry, now, log)

    entry["polls"] = int(entry.get("polls") or 0) + 1
    step = _step(entry, "registered")
    try:
        status = client.deploy_status(entry["dag_id"], entry["filename"]) or {}
    except Exception as err:  # noqa: BLE001
        if not _is_retryable(err, "registered", entry) or step["attempts"] >= MAX_ATTEMPTS:
            return _fail_step(entry, "registered", err, now, "deploy", entry["dag_id"])
        log.warning("deploy %s: registration poll failed: %s", entry["deploy_id"], err)
        return _backoff(entry, now, "registered", err), False

    state = status.get("state")
    if state == "failed":
        # A broken new DAG must never cost you the working old one — this is the
        # durable form of the widget's `pendingRetireRef.current = null`.
        entry["import_error"] = status.get("import_error")
        step["state"] = "failed"
        step["at"] = iso(now)
        if entry.get("retire"):
            _step(entry, "retire")["state"] = "skipped"
            _step(entry, "retire")["skipped_reason"] = "the new DAG failed to import"
        trace = ((status.get("import_error") or {}).get("stack_trace") or "")
        _audit(entry, "deploy", "error", dag_id=entry["dag_id"],
               detail=f"{entry['filename']} failed to import: {trace}")
        return _finish(entry, "import_failed", f"{entry['filename']} failed to import.", now)

    if state == "registered":
        step["state"] = "done"
        step["at"] = iso(now)
        entry["deadline_at"] = iso(
            max(deadline, now + timedelta(seconds=POST_REGISTRATION_GRACE_S))
        )
        entry["phase"] = _next_phase(entry)
        entry["message"] = f"Registered {entry['dag_id']}."
        entry["next_attempt_at"] = iso(now)
        entry["updated_at"] = iso(now)
        log.info(
            "deploy %s dag_id=%s registered → %s",
            entry["deploy_id"], entry["dag_id"], entry["phase"],
        )
        return entry, False

    return _backoff(entry, now, "registered"), False


def _do_quiesce(entry, now, client, target, log):
    """Pause the renamed-away DAG(s) — the reversible half of a retire.

    This runs where the destructive retire used to, and for the same reason: the
    new DAG is about to be unpaused, and two DAGs on the same schedule
    processing the same data is the hazard retiring early was defending against.
    Pausing closes that hazard completely while costing nothing if this entry is
    later abandoned — unpausing restores the previous state exactly, whereas a
    deleted file and a purged history do not come back. The irreversible half
    now waits until the new DAG is actually live (see :func:`_do_retire`).
    """
    intents = retire_intents(entry)
    step = _step(entry, "quiesce")
    if not intents or step["state"] != "pending":
        entry["phase"] = _next_phase(entry)
        return entry, False

    if now > parse_ts(entry["action_deadline_at"]):
        step["state"] = "skipped"
        step["skipped_reason"] = "the action budget elapsed"
        entry["phase"] = _next_phase(entry)
        return entry, False

    if _step(entry, "registered")["state"] != "done":
        step["state"] = "skipped"
        step["skipped_reason"] = "the new DAG was never observed as registered"
        entry["phase"] = _next_phase(entry)
        return entry, False

    # Carried over from the retire step this replaced: if the file on disk is no
    # longer the one this deploy wrote, the deploy is stale and must touch
    # nothing — not even a pause, which would be acting on another deploy's
    # behalf. `unknown` (an unreadable target) is bounded, never an infinite
    # back-off.
    verdict = _supersession(entry, target)
    if verdict == "superseded":
        return _superseded(entry, now)
    if verdict == "unknown":
        if step["attempts"] >= MAX_ATTEMPTS:
            step["state"] = "failed"
            step["last_error"] = "the deploy target could not be read"
            step["at"] = iso(now)
            entry["phase"] = _next_phase(entry)
            return entry, False
        return _backoff(entry, now, "quiesce"), False

    paused: List[str] = []
    for intent in intents:
        # Never the DAG this deploy just wrote (a rename away and back).
        if intent["dag_id"] == entry["dag_id"]:
            continue
        try:
            client.set_paused(intent["dag_id"], True)
            paused.append(intent["dag_id"])
        except Exception as err:  # noqa: BLE001
            # A 404 means it is already gone, which is the state we wanted.
            if getattr(err, "status", None) == 404:
                continue
            if step["attempts"] >= MAX_ATTEMPTS:
                step["state"] = "failed"
                step["last_error"] = str(err)[:500]
                entry["phase"] = _next_phase(entry)
                return entry, False
            return _backoff(entry, now, "quiesce", err), False

    step["state"] = "done"
    step["at"] = iso(now)
    step["result"] = {"paused": bool(paused)}
    entry["phase"] = _next_phase(entry)
    entry["next_attempt_at"] = iso(now)
    entry["updated_at"] = iso(now)
    log.info(
        "deploy %s paused %s → %s",
        entry["deploy_id"], ", ".join(paused) or "nothing", entry["phase"],
    )
    return entry, False


#: Skips that are a *decision* — a user vetoed the unpause, or run-on-deploy is
#: off. Running out of time is NOT one: a budget skip never licenses destruction.
_DELIBERATE_SKIPS = (VETO_REASON, "run-on-deploy is off for this deploy")


def _ready_to_retire(entry: Dict[str, Any]) -> Optional[str]:
    """``None`` when invariant I1 is satisfied, else why it is not.

    The irreversible act may only follow a lifecycle that actually *finished*.
    A step that timed out is not a decision, so it does not authorise deleting
    the DAG the user renamed away from.
    """
    if _step(entry, "registered")["state"] != "done":
        return "the new DAG was never observed as registered"
    for name in ("unpause", "trigger"):
        state = _step(entry, name)
        if state["state"] == "done":
            continue
        if (state["state"] == "skipped"
                and state.get("skipped_reason") in _DELIBERATE_SKIPS):
            continue
        return f"{name} did not complete ({state['state']})"
    return None


def _do_retire(entry, now, client, target, log):
    # A QUEUE, not a single target: a rename chained onto a rename whose deploy
    # was still awaiting registration hands its intent to this entry, and both
    # dag_ids have to be retired or one of them stays live and unpaused.
    intents = retire_intents(entry)
    step = _step(entry, "retire")

    # Invariant I1. Reached only after unpause and trigger, and refused unless
    # they genuinely completed — this is what stops the reported incident, where
    # the old DAG was purged and the new one then abandoned, paused and never
    # triggered, leaving the user with no working DAG at all.
    blocked = _ready_to_retire(entry)
    if blocked and step["state"] == "pending":
        step["state"] = "skipped"
        step["skipped_reason"] = blocked
        for intent in intents:
            _audit(entry, "retire", "rejected", dag_id=intent["dag_id"],
                   detail=f"refused: {blocked}; the old DAG is left intact")
        return _terminate(
            entry, entry.get("outcome") or "expired",
            f"Deployed {entry['dag_id']}, but {_quote_ids(intents)} was left "
            f"in place — {blocked}.", now,
        )
    if not intents or step["state"] != "pending":
        entry["phase"] = "unpausing"
        return entry, False

    # The same budget guard `_do_unpause`/`_do_trigger` apply. Without it the
    # `unknown` supersession verdict below backs off forever: the entry never
    # becomes terminal, the observing tab never resolves, and — because
    # `sweep_once` reports a non-zero `pending` on every tick — the reconciler's
    # PeriodicCallback never stops, losing the "an idle server schedules nothing"
    # property that justifies the feature being on by default.
    if now > parse_ts(entry["action_deadline_at"]):
        for name in ("retire", "unpause", "trigger"):
            if _step(entry, name)["state"] == "pending":
                _step(entry, name)["state"] = "skipped"
                _step(entry, name)["skipped_reason"] = "the action budget elapsed"
        for intent in intents:
            _audit(entry, "retire", "error", dag_id=intent["dag_id"],
                   detail=("the action budget elapsed before the old DAG could be "
                           "retired; it is still active"))
        it_is = "it is" if len(intents) == 1 else "they are"
        return _terminate(
            entry, "expired",
            f"Deployed {entry['dag_id']}, but the action budget elapsed before "
            f"{_quote_ids(intents)} could be retired — {it_is} still active.", now,
        )

    # Preconditions, each a hard refusal: this is the one catastrophic mis-fire
    # available here (deleting a live DAG's file), and nothing else guards it.
    if _step(entry, "registered")["state"] != "done":
        log.error("deploy %s: refusing to retire before registration", entry["deploy_id"])
        step["state"] = "skipped"
        step["skipped_reason"] = "the new DAG was never observed as registered"
        entry["phase"] = "unpausing"
        return entry, False
    # A rename away and back must not retire the DAG we just deployed. Applied
    # per queued intent: one bad target must not cancel the others.
    self_targets = [i for i in intents if i["dag_id"] == entry["dag_id"]]
    intents = [i for i in intents if i["dag_id"] != entry["dag_id"]]
    for intent in self_targets:
        log.error(
            "deploy %s: refusing to retire %s — it is the DAG this deploy wrote",
            entry["deploy_id"], intent["dag_id"],
        )
        _audit(entry, "retire", "rejected", dag_id=intent["dag_id"],
               detail="refused: the retire target is the DAG this deploy wrote")
    if not intents:
        step["state"] = "skipped"
        step["skipped_reason"] = "the retire target is the DAG this deploy wrote"
        return _terminate(entry, entry.get("outcome") or "completed",
                          entry.get("message", ""), now)

    verdict = _supersession(entry, target)
    if verdict == "superseded":
        return _superseded(entry, now)
    if verdict == "unknown":
        # Bounded like every other retry path: an unreadable deploy target (an
        # expired S3 credential, an unreachable endpoint) must not spin forever.
        if step["attempts"] >= MAX_ATTEMPTS:
            step["state"] = "failed"
            step["last_error"] = "the deploy target could not be read"
            step["at"] = iso(now)
            for intent in intents:
                _audit(entry, "retire", "error", dag_id=intent["dag_id"],
                       detail="the deploy target could not be read; old DAG left active")
            was = "was" if len(intents) == 1 else "were"
            it_is = "it is" if len(intents) == 1 else "they are"
            return _finish(
                entry, "failed",
                f"Could not read the deploy target, so {_quote_ids(intents)} {was} "
                f"not retired — {it_is} still active.", now,
            )
        return _backoff(entry, now, "retire"), False

    # Recorded on the step, so a retry after a mid-queue failure does not retire
    # (or re-audit) what already succeeded. Each retire is idempotent anyway.
    already = set(step.get("retired") or [])
    retired: List[Dict[str, Any]] = []
    skips: List[Tuple[str, str]] = []
    flags = {"removed_file": False, "paused": False, "purged_history": False}
    flags.update(step.get("result") or {})
    for intent in intents:
        if intent["dag_id"] in already:
            retired.append(intent)
            continue
        try:
            result = retire_old_dag(
                intent["dag_id"],
                purge=bool(intent.get("purge")),
                target=target,
                # NOT `or None`: an empty afdag_id means "this flow has no
                # ownership provenance", which is a reason to check MORE
                # carefully, not to skip the check. `verify_ownership` keeps "no
                # check requested" (the editor's own retire call) distinct from
                # "check requested, identity unknown" — collapsing them into
                # `None` let a delayed retire delete a different flow's freshly
                # deployed DAG.
                expect_afdag_id=entry.get("afdag_id") or "",
                verify_ownership=True,
            )
        except Exception as err:  # noqa: BLE001
            step["result"] = flags
            step["retired"] = sorted(already)
            if not _is_retryable(err, "retire", entry) or step["attempts"] >= MAX_ATTEMPTS:
                # Stopping here is the conservative branch: with the old file
                # still present and unpaused, unpausing and triggering the new
                # DAG would leave two live DAGs processing the same data.
                return _fail_step(entry, "retire", err, now, "retire", intent["dag_id"])
            log.warning("deploy %s: retire failed: %s", entry["deploy_id"], err)
            return _backoff(entry, now, "retire", err), False

        skipped = result.get("skipped_reason")
        if skipped:
            skips.append((intent["dag_id"], skipped))
            _audit(entry, "retire", "rejected", dag_id=intent["dag_id"], detail=skipped)
            continue
        already.add(intent["dag_id"])
        retired.append(intent)
        for key in flags:
            flags[key] = flags[key] or bool(result.get(key))
        _audit(entry, "retire", "ok", dag_id=intent["dag_id"],
               detail="purged" if intent.get("purge") else "history kept")

    step["at"] = iso(now)
    step["result"] = flags
    step["retired"] = sorted(already)
    left_alone = "; ".join(f"“{dag_id}” alone — {why}" for dag_id, why in skips)
    if retired:
        step["state"] = "done"
        done = ", ".join(
            f"“{intent['dag_id']}” "
            + ("purged" if intent.get("purge") else "retired, history kept")
            for intent in retired
        )
        entry["message"] = f"Registered {entry['dag_id']} — old DAG {done}."
        if skips:
            entry["message"] += f" Left {left_alone}."
    else:
        step["state"] = "skipped"
        step["skipped_reason"] = (
            skips[0][1] if len(skips) == 1
            else "; ".join(f"{dag_id}: {why}" for dag_id, why in skips)
        )
        entry["message"] = f"Left {left_alone}."
    log.info(
        "deploy %s retired %s → done",
        entry["deploy_id"], ", ".join(i["dag_id"] for i in retired) or "nothing",
    )
    return _terminate(entry, "completed", entry.get("message", ""), now)


def _do_unpause(entry, now, client, target, log):
    step = _step(entry, "unpause")
    if now > parse_ts(entry["action_deadline_at"]):
        for name in ("unpause", "trigger"):
            if _step(entry, name)["state"] == "pending":
                _step(entry, name)["state"] = "skipped"
                _step(entry, name)["skipped_reason"] = "the action budget elapsed"
        return _terminate(
            entry, "expired",
            f"Deployed {entry['dag_id']}, but the action budget elapsed before it "
            "could be unpaused — unpause it from the DAG list.", now,
        )

    if step["state"] == "skipped":
        # Vetoed: the user paused this DAG while the deploy was in flight.
        if _step(entry, "trigger")["state"] == "pending":
            _step(entry, "trigger")["state"] = "skipped"
            _step(entry, "trigger")["skipped_reason"] = step.get("skipped_reason")
        return _finish(
            entry, "completed",
            f"Deployed {entry['dag_id']} — left paused at your request.", now,
        )

    if step["state"] == "pending":
        verdict = _supersession(entry, target)
        if verdict == "superseded":
            return _superseded(entry, now)
        if verdict == "unknown":
            return _backoff(entry, now, "unpause"), False
        try:
            client.set_paused(entry["dag_id"], False)
        except Exception as err:  # noqa: BLE001
            if not _is_retryable(err, "unpause", entry) or step["attempts"] >= MAX_ATTEMPTS:
                return _fail_step(entry, "unpause", err, now, "unpause", entry["dag_id"])
            log.warning("deploy %s: unpause failed: %s", entry["deploy_id"], err)
            return _backoff(entry, now, "unpause", err), False
        step["state"] = "done"
        step["at"] = iso(now)
        _audit(entry, "unpause", "ok", dag_id=entry["dag_id"])

    entry["phase"] = "triggering"
    entry["next_attempt_at"] = iso(now)
    entry["updated_at"] = iso(now)
    log.info("deploy %s unpaused %s → triggering", entry["deploy_id"], entry["dag_id"])
    return entry, False


def _do_trigger(entry, now, client, target, log):
    step = _step(entry, "trigger")
    if step["state"] in ("skipped", "done") or not entry.get("run_on_deploy"):
        if step["state"] == "pending":
            step["state"] = "skipped"
            step["skipped_reason"] = "run-on-deploy is off for this deploy"
        return _finish(entry, "completed", f"Deployed {entry['dag_id']}.", now)

    if now > parse_ts(entry["action_deadline_at"]):
        step["state"] = "skipped"
        step["skipped_reason"] = "the action budget elapsed"
        return _terminate(
            entry, "expired",
            f"Deployed and unpaused {entry['dag_id']}, but the action budget "
            "elapsed before it could be triggered.", now,
        )

    verdict = _supersession(entry, target)
    if verdict == "superseded":
        return _superseded(entry, now)
    if verdict == "unknown":
        return _backoff(entry, now, "trigger"), False

    run_id = entry["run_id"]
    try:
        run = client.trigger_dag(entry["dag_id"], dag_run_id=run_id) or {}
    except AirflowError as err:
        if err.status == 409:
            # The deterministic run_id already exists: this deploy DID trigger,
            # and the process died before it could record that. The 409 is the
            # truth; adopt the run rather than minting a second one. (Handled
            # here, not inside `client.trigger_dag`, so the manager's own Trigger
            # button keeps reporting a duplicate as the error it is.)
            try:
                run = client.get_dag_run(entry["dag_id"], run_id) or {}
            except Exception as get_err:  # noqa: BLE001
                log.warning("deploy %s: could not read the adopted run: %s",
                            entry["deploy_id"], get_err)
                run = {"dag_run_id": run_id}
        elif not _is_retryable(err, "trigger", entry) or step["attempts"] >= MAX_ATTEMPTS:
            return _fail_step(entry, "trigger", err, now, "trigger", entry["dag_id"])
        else:
            log.warning("deploy %s: trigger failed: %s", entry["deploy_id"], err)
            return _backoff(entry, now, "trigger", err), False
    except Exception as err:  # noqa: BLE001
        if step["attempts"] >= MAX_ATTEMPTS:
            return _fail_step(entry, "trigger", err, now, "trigger", entry["dag_id"])
        return _backoff(entry, now, "trigger", err), False

    step["state"] = "done"
    step["at"] = iso(now)
    step["run_state"] = run.get("state")
    _audit(entry, "trigger", "ok", dag_id=entry["dag_id"], detail=f"run_id={run_id}")
    log.info("deploy %s triggered %s (%s)", entry["deploy_id"], entry["dag_id"], run_id)
    # Terminal at run *creation*: a run can take hours and Airflow owns its state,
    # so the reopened widget polls it with the existing getDagRun (PRD §10).
    entry["message"] = f"Deployed and triggered {entry['dag_id']}."
    entry["phase"] = _next_phase(entry)
    entry["next_attempt_at"] = iso(now)
    entry["updated_at"] = iso(now)
    if entry["phase"] == "terminal":
        return _terminate(entry, "completed", entry["message"], now)
    return entry, False


#: Step skips that mean "the clock ran out", i.e. the step was never attempted.
#: Only these are re-armed by :func:`reopen_expired` — a step skipped because the
#: new DAG failed to import, or because a user paused the DAG, was a *decision*
#: and must survive the retry. Defined in ``journal`` because the journal uses the
#: same predicate to decide which stranded retire intents a later deploy may
#: inherit (``Journal.orphaned_retires``).
_BUDGET_SKIPS = BUDGET_SKIP_REASONS
#: Steps a re-arm may put back to ``pending``. ``registered`` is not one of them:
#: it is never skipped, only observed, so it needs no reset.
_STEPS_REARMED = ("retire", "unpause", "trigger")


def reopen_expired(journal, deploy_id: str, *, now=None, budget_s: Optional[int] = None
                   ) -> Tuple[bool, str]:
    """Re-arm an ``expired`` entry so the server finishes what it ran out of time
    for. Returns ``(resumed, reason)``.

    This is the server-side half of the banner's "Keep waiting". It exists
    because the alternative — letting the editor re-run the tail itself — is
    unsound on the journaled path: the rename's retire intent lives only in the
    journal (deliberately: a ref dies with the tab), so a browser retry would
    unpause and trigger the new DAG and leave the old one live and unpaused
    beside it.

    Only a *timed-out* lifecycle is re-armed. A failure, an import error, a
    cancel or a supersede were all decisions, and re-running them is not
    "waiting longer" — it is overriding the answer.
    """
    now = now or utcnow()
    budget = timedelta(seconds=budget_s if budget_s is not None else deploy_budget_s())
    entry = journal.get(deploy_id)
    if entry is None:
        return False, "no such deploy"
    if entry.get("outcome") is None:
        return False, "still in flight"
    if entry.get("outcome") != "expired":
        return False, f"the deploy is {entry['outcome']}, not expired"
    # A newer deploy of the same flow owns these dag_ids now; re-arming would put
    # two lifecycles back on the same DAG.
    if journal.open_for(afdag_id=entry.get("afdag_id") or None) or journal.open_for(
        dag_id=entry["dag_id"]
    ):
        return False, "a newer deploy of this flow is already in flight"

    for name in _STEPS_REARMED:
        step = _step(entry, name)
        if step["state"] == "skipped" and step.get("skipped_reason") in _BUDGET_SKIPS:
            step["state"] = "pending"
            step["skipped_reason"] = None
            step["attempts"] = 0
    entry["outcome"] = None
    entry["terminal_at"] = None
    entry["polls"] = 0
    entry["deadline_at"] = iso(now + budget)
    entry["action_deadline_at"] = iso(now + budget)
    entry["next_attempt_at"] = iso(now)
    entry["phase"] = (
        "awaiting_registration"
        if _step(entry, "registered")["state"] != "done"
        else _next_phase(entry)
    )
    entry["message"] = f"Still waiting for Airflow to pick up {entry['filename']}…"
    if not journal.reopen(entry):
        return False, "the journal entry could not be re-armed"
    return True, ""


# --------------------------------------------------------------------------- #
# Sweep (pure)
# --------------------------------------------------------------------------- #
@dataclass
class SweepReport:
    pending: int = 0
    advanced: int = 0
    completed: int = 0
    expired: int = 0
    failed: int = 0
    denied: int = 0
    superseded: int = 0
    quarantined: int = 0
    pruned: int = 0
    actions: List[Dict[str, Any]] = field(default_factory=list)

    def tally(self, entry: Dict[str, Any], terminal: bool) -> None:
        self.advanced += 1
        self.actions.append(
            {
                "deploy_id": entry["deploy_id"],
                "dag_id": entry["dag_id"],
                "phase": entry["phase"],
                "outcome": entry.get("outcome"),
            }
        )
        if not terminal:
            return
        outcome = entry.get("outcome")
        if outcome in ("completed", "import_failed"):
            self.completed += 1
        elif outcome == "expired":
            self.expired += 1
        elif outcome == "denied":
            self.denied += 1
        elif outcome == "superseded":
            self.superseded += 1
        else:
            self.failed += 1


def _deny_all(journal, log, report: SweepReport) -> SweepReport:
    """Fail closed on a role downgrade.

    The role is process-wide env, so ``viewer`` here means the server was
    deliberately restarted with reduced privileges — it must not keep mutating
    shared Airflow on the strength of a pre-demotion intent. Entries are
    *finalized*, not retried forever: a demoted server must not sit in a loop
    attempting privileged work.
    """
    stranded: List[str] = []
    for entry in journal.list_pending():
        for name, action in (("retire", "retire"), ("unpause", "unpause"), ("trigger", "trigger")):
            step = entry["steps"][name]
            if step["state"] != "pending":
                continue
            if name == "retire":
                for intent in retire_intents(entry):
                    _audit(entry, action, "denied", dag_id=intent["dag_id"],
                           detail=f"role={studio_role()}")
                continue
            _audit(entry, action, "denied", dag_id=entry["dag_id"],
                   detail=f"role={studio_role()}")
        entry["message"] = "This server no longer has edit rights; the deploy was not completed."
        journal.finish(entry, "denied")
        report.denied += 1
        stranded.append(entry["dag_id"])
    if stranded:
        log.warning(
            "deploy reconciler is running as %s: %d deploy(s) were left "
            "incomplete (%s) — finish them by hand from the manager.",
            studio_role(), len(stranded), ", ".join(stranded),
        )
    return report


def sweep_once(*, now=None, journal=None, client=None, target=None, log=None,
               max_entries: int = MAX_ENTRIES_PER_SWEEP,
               retention: Optional[int] = None) -> SweepReport:
    """One pass over the journal. Synchronous, injectable, and the only thing the
    scheduling shell actually calls."""
    now = now or utcnow()
    journal = journal or get_journal()
    log = log or _log
    report = SweepReport()

    # Authorization is re-checked EVERY sweep, before any work (PRD §9).
    if not can_edit():
        return _deny_all(journal, log, report)

    # Housekeeping folded in, so there is one timer rather than two.
    journal.reclaim_stale(now)
    report.pruned = journal.prune(
        now, retention if retention is not None else retention_s()
    )

    client = client if client is not None else get_client()
    target = target if target is not None else get_deploy_target()

    entries = journal.list_pending()
    due = [e for e in entries if parse_ts(e["next_attempt_at"]) <= now]
    # Oldest first: the stranded rename is the one that matters most.
    due.sort(key=lambda e: e["created_at"])

    for candidate in due[:max_entries]:
        claimed = journal.claim(candidate["deploy_id"])
        if claimed is None:
            continue  # another sweeper (or process) has it
        try:
            updated, terminal = advance(
                claimed, now=now, client=client, target=target, log=log
            )
        except Exception as err:  # noqa: BLE001 - one poisoned entry must not abort the sweep
            log.exception("reconciler entry failed: %s", claimed["deploy_id"])
            updated, terminal = _backoff(claimed, now, None, err), False
        if terminal:
            written = journal.finish(updated, updated.get("outcome") or "failed")
        else:
            written = journal.release(updated)
        if not written:
            # The entry was taken over while we held it (a cancel or a supersede
            # finalized it). That writer's version is authoritative and already
            # terminal — writing ours back is precisely the resurrection the
            # claim token exists to prevent, so drop it silently.
            continue
        report.tally(updated, terminal)

    report.pending = len(journal.list_pending())
    return report


# --------------------------------------------------------------------------- #
# Scheduling shell (the ONLY part that touches the IOLoop)
# --------------------------------------------------------------------------- #
class DeployReconciler:
    def __init__(self, journal, log, *, interval_s: int, deadline_s: int,
                 retention_s: int) -> None:
        self._journal = journal
        self._log = log
        self.interval_s = interval_s
        self.deadline_s = deadline_s
        self.retention_s = retention_s
        self._loop: Optional[IOLoop] = None
        self._pool: Optional[ThreadPoolExecutor] = None
        self._timer: Optional[PeriodicCallback] = None
        self._in_flight = False
        self._started = False

    # -- lifecycle ---------------------------------------------------------- #
    def start(self, *, schedule: bool = True) -> None:
        """Idempotent. ``schedule=False`` wires everything up but never creates a
        ``PeriodicCallback`` — how tests get the journal without a timer firing
        into their assertions."""
        if self._started:
            return
        self._started = True
        # max_workers=1 makes slow-tick pileup structurally impossible, and a
        # DEDICATED pool means a sweep stalled on an unreachable git remote can
        # never starve the interactive requests that share the default executor.
        self._pool = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="afstudio-reconciler"
        )
        pending = 0
        try:
            resumed = self._journal.resume()
            self._journal.prune(utcnow(), self.retention_s)
            expired = self._finalize_expired()
            pending = len(self._journal.list_pending())
            if resumed or expired:
                self._log.info(
                    "deploy reconciler recovered %d in-flight and expired %d "
                    "stale deploy(s) from the journal", resumed, expired,
                )
        except OSError as err:
            self._log.warning("deploy journal is unusable (%s) — deploys will not "
                              "be completed in the background", err)
        self._log.info(
            "deploy reconciler enabled (interval=%ss, budget=%ss, journal=%s, pending=%d)",
            self.interval_s, self.deadline_s, self._journal.root, pending,
        )
        atexit.register(self.stop)
        if not schedule:
            return
        try:
            self._loop = IOLoop.current()
        except Exception:  # noqa: BLE001 - no loop here (a sync context): stay dormant
            self._loop = None
            return
        if pending:
            self._start_timer()
            # An immediate first tick: a restarted server heals stranded deploys
            # in milliseconds rather than in one interval.
            self._loop.add_callback(self._tick)

    def _finalize_expired(self) -> int:
        """Entries already past their deadline when the server came back.

        Honouring the deadline against wall clock rather than uptime is what
        stops a server that was down for a day from waking up and triggering a
        day-old run.
        """
        now = utcnow()
        count = 0
        for entry in self._journal.list_pending():
            if now <= parse_ts(entry["deadline_at"]):
                continue
            updated, _ = _expire_registration(entry, now, self._log)
            self._journal.finish(updated, updated["outcome"])
            count += 1
        return count

    def _start_timer(self) -> None:
        if self._timer is not None:
            return
        self._timer = PeriodicCallback(self._tick, self.interval_s * 1000)
        self._timer.start()

    def _stop_timer(self) -> None:
        if self._timer is not None:
            self._timer.stop()
            self._timer = None

    def stop(self) -> None:
        self._stop_timer()
        if self._pool is not None:
            # Nothing is lost by not waiting: the journal is on disk, and an
            # unfinished entry is reclaimed on the next start.
            self._pool.shutdown(wait=False)
            self._pool = None
        self._started = False

    def wake(self) -> None:
        """Called from a request's executor thread after a deploy is journaled.

        ``IOLoop.add_callback`` is the one Tornado method documented thread-safe,
        so the hop happens there and every timer decision stays on the loop.
        """
        loop = self._loop
        if loop is None:
            return
        try:
            loop.add_callback(self._on_wake)
        except Exception:  # noqa: BLE001 - a closing loop must not fail a deploy
            self._log.debug("could not wake the deploy reconciler", exc_info=True)

    def _on_wake(self) -> None:
        self._start_timer()
        if not self._in_flight:
            self._tick_soon()

    def _tick_soon(self) -> None:
        if self._loop is not None:
            self._loop.add_callback(self._tick)

    # -- the tick ----------------------------------------------------------- #
    async def _tick(self) -> None:
        """Nothing here blocks: every blocking thing happens inside ``_sweep``,
        on the dedicated thread. Tornado awaits an awaitable returned by a
        ``PeriodicCallback`` before rescheduling, so ticks cannot overlap by
        construction; ``_in_flight`` guards the other entry points (start, wake,
        ``sweep_now``)."""
        if self._in_flight or self._pool is None:
            return
        self._in_flight = True
        try:
            report = await self._run_sweep()
            if report.pending == 0:
                # An idle server schedules nothing. This is what makes the
                # feature acceptable on by default.
                self._stop_timer()
        except Exception:  # noqa: BLE001
            self._log.exception("deploy reconciler sweep failed")
        finally:
            self._in_flight = False

    def _run_sweep(self):
        loop = self._loop or IOLoop.current()
        return loop.run_in_executor(self._pool, self._sweep)

    def _sweep(self) -> SweepReport:
        report = sweep_once(
            journal=self._journal, log=self._log, retention=self.retention_s
        )
        self._log.debug("deploy reconciler sweep: %s", report)
        return report

    async def sweep_now(self) -> SweepReport:
        """One-shot sweep on the dedicated thread (used by tests)."""
        if self._pool is None:
            self._pool = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="afstudio-reconciler"
            )
        return await self._run_sweep()


_RECONCILER: Optional[DeployReconciler] = None


def configure(server_app) -> Optional[DeployReconciler]:
    """Build the reconciler for this server, or ``None`` when it is disabled or
    the journal directory cannot be used."""
    global _RECONCILER
    log = getattr(server_app, "log", None) or _log
    if not is_enabled():
        log.info("deploy reconciler disabled (%s=off)", ENV_ENABLED)
        _RECONCILER = None
        return None
    try:
        journal = get_journal()
    except OSError as err:
        # The moment an operator is most likely to notice that durability is off.
        log.warning(
            "deploy reconciler could not open its journal directory (%s) — a "
            "deploy will only be completed while its editor tab stays open", err,
        )
        _RECONCILER = None
        return None
    # The instance logs through the SERVER's logger, not the module one: its
    # messages ("enabled …", one line per state transition, every warning) are
    # operator-facing, and a bare `jupyterlab_airflow.reconciler` logger would be
    # swallowed by the default root level — the operator would never see that a
    # DAG unpaused itself minutes after a tab closed. The pure functions still
    # default to the module logger when called directly (tests inject their own).
    _RECONCILER = DeployReconciler(
        journal,
        log,
        interval_s=interval_s(),
        deadline_s=deploy_budget_s(),
        retention_s=retention_s(),
    )
    return _RECONCILER


def get_reconciler() -> Optional[DeployReconciler]:
    return _RECONCILER
