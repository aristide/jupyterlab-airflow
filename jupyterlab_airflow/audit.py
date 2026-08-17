"""Structured audit trail for mutating Studio actions (PRD §9 / §10).

Records **who did what to which DAG** — even before full per-user identity lands
(§9). Every mutating server action (deploy / trigger / pause / stop-run / clear /
delete / rollback / retire) emits one structured record::

    {ts, user, action, dag_id, correlation_id, outcome, via, detail?}

``via`` is ``"request"`` (a human clicked something) or ``"reconciler"`` (the
server finished a deploy's lifecycle in the background — PRD §6.5.4). It is
additive: its absence in an older log line means the request path. ``user`` is
the human who initiated the work in **both** cases — the reconciler is a
mechanism, not a principal, and "the server did it" is not an answer to *who*.

to a dedicated ``jupyterlab_airflow.audit`` logger as a single JSON line, so the
action is attributable and a failed import can be traced back to a Studio session
by its ``correlation_id``. The record is JSON‑serialized, so user‑controlled
fields (``dag_id``/``user``/``detail``) are escaped — no log injection. We log the
*action*, never the request payload, so a trigger ``conf`` (which may carry
secrets) is **not** recorded.

Output goes through the standard ``logging`` framework, so a deployment can route
``jupyterlab_airflow.audit`` to a file/SIEM via normal logging config without any
code change. Read-only reads are intentionally not audited (only mutations).
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

#: The dedicated logger name a deployment can target with logging config to route
#: the audit trail (e.g. to a file or SIEM).
AUDIT_LOGGER_NAME = "jupyterlab_airflow.audit"

_logger = logging.getLogger(AUDIT_LOGGER_NAME)

#: Where the trail is written. A path, or ``off`` to write no file (for a
#: deployment that routes the logger itself).
ENV_AUDIT_LOG = "JUPYTERLAB_AIRFLOW_AUDIT_LOG"
#: Rotation. An audit file that grows without bound is its own incident.
ENV_AUDIT_MAX_BYTES = "JUPYTERLAB_AIRFLOW_AUDIT_MAX_BYTES"
ENV_AUDIT_BACKUPS = "JUPYTERLAB_AIRFLOW_AUDIT_BACKUPS"
DEFAULT_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_BACKUPS = 5

#: Marks the handler as ours, so a re-load is idempotent and an operator's own
#: handlers are recognisably not ours.
_OWNED = "_jupyterlab_airflow_audit"


def audit_path(data_dir: Optional[str] = None) -> Optional[str]:
    """The audit file's path, or ``None`` when file output is switched off."""
    raw = os.environ.get(ENV_AUDIT_LOG, "").strip()
    if raw.lower() in ("off", "0", "false", "none"):
        return None
    if raw:
        return os.path.abspath(raw)
    base = data_dir
    if not base:
        from jupyter_core.paths import jupyter_data_dir

        base = jupyter_data_dir()
    return os.path.join(base, "airflow-studio", "audit.log")


def _env_int(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, "").strip())
    except ValueError:
        return default
    return value if value > 0 else default


def configure(server_app: Any = None) -> Optional[str]:
    """Give the audit trail somewhere to actually land. Returns the path, if any.

    This exists because the trail was previously **inert by default**. The
    records were emitted correctly, but ``jupyterlab_airflow.audit`` sits at
    ``NOTSET`` under a root logger that Jupyter leaves at ``WARNING`` with no
    handlers, so every ``INFO`` record was discarded. Nothing warned about it:
    the feature looked present, the tests passed (their fixture attaches its own
    handler), and an operator would only discover the gap when they went looking
    for a record that was never written — which is the worst possible moment.

    Two rules keep this from fighting a deployment that *has* configured the
    logger:

    * If handlers are already attached, we add none of our own. That is an
      operator routing the trail to their SIEM, and duplicating it into a file
      they did not ask for is not our call.
    * The level is only raised when it is ``NOTSET``. An explicit level is a
      decision; ``NOTSET`` is just the default that made handlers useless.
    """
    log = getattr(server_app, "log", None) or logging.getLogger(__name__)

    # `NOTSET` here means "nobody chose", and inheriting WARNING from root is
    # what silently dropped every record. An explicit choice is left alone.
    if _logger.level == logging.NOTSET:
        _logger.setLevel(logging.INFO)

    existing = list(_logger.handlers)
    if any(getattr(h, _OWNED, False) for h in existing):
        return getattr(next(h for h in existing if getattr(h, _OWNED, False)),
                       "baseFilename", None)
    if existing:
        log.info(
            "audit: %s already has handlers — leaving routing to them",
            AUDIT_LOGGER_NAME,
        )
        return None

    path = audit_path(getattr(server_app, "data_dir", None))
    if path is None:
        log.info("audit: file output disabled (%s)", ENV_AUDIT_LOG)
        return None

    try:
        os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
        # Create it ourselves, at 0600, BEFORE the handler opens it. Letting the
        # handler create it would apply the process umask — typically 0644 — and
        # a chmod afterwards still leaves a window where a file naming users and
        # their actions is world-readable. `delay` is therefore off: the file
        # must exist by the time we return, and an empty audit.log is a useful
        # signal in its own right that the trail is armed.
        os.close(os.open(path, os.O_CREAT | os.O_APPEND, 0o600))
        handler = logging.handlers.RotatingFileHandler(
            path,
            maxBytes=_env_int(ENV_AUDIT_MAX_BYTES, DEFAULT_MAX_BYTES),
            backupCount=_env_int(ENV_AUDIT_BACKUPS, DEFAULT_BACKUPS),
            encoding="utf-8",
        )
        # The record is already a complete JSON object; a prefix would make the
        # file no longer parseable line-by-line, which is the one property a
        # downstream consumer needs.
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler.setLevel(logging.INFO)
        setattr(handler, _OWNED, True)
        _logger.addHandler(handler)
    except OSError as err:
        # An unwritable audit path must not stop the server from starting; it
        # must, however, be loud, because "no records" now means "not recorded"
        # rather than "nothing happened".
        log.warning("audit: could not open %s (%s) — the trail is NOT being "
                    "written; set %s to a writable path", path, err, ENV_AUDIT_LOG)
        return None

    log.info("audit trail → %s", path)
    return path

# The mutating actions we audit (a closed vocabulary so the trail is consistent).
ACTIONS = frozenset(
    {
        "deploy",
        "trigger",
        "pause",
        "unpause",
        "stop_run",
        "clear",
        "delete",
        "rollback",
        "retire",
        "variable_set",
        "variable_delete",
        "connection_set",
        "connection_delete",
    }
)


def audit_event(
    action: str,
    *,
    user: str,
    correlation_id: str,
    dag_id: Optional[str] = None,
    outcome: str = "ok",
    detail: Optional[str] = None,
    via: str = "request",
) -> Dict[str, Any]:
    """Emit one audit record for a mutating action and return it.

    ``outcome`` is ``"ok"`` (the action completed and mutated), ``"rejected"``
    (it ran but mutated nothing — e.g. a deploy refused by validation / a
    missing provider), ``"denied"`` (authorization refused it before it ran —
    a view-only role attempting a privileged action, PRD §9), or ``"error"``
    (it raised); ``detail`` carries a short error/reason message for
    rejected/denied/error (never the request body).

    ``via`` names the *actor path* — ``"request"`` or ``"reconciler"``. It is a
    structured field rather than a ``detail`` prefix on purpose: a SIEM can then
    filter/alert on background-completed actions with a query instead of
    string-matching a message, and ``detail`` keeps meaning only "why".

    The record is logged as a single JSON line at ``INFO`` on the
    ``jupyterlab_airflow.audit`` logger.
    """
    record: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "user": user or "anonymous",
        "action": action,
        "dag_id": dag_id,
        "correlation_id": correlation_id,
        "outcome": outcome,
        "via": via,
    }
    if detail is not None:
        # Trim so a long traceback/message can't bloat the line.
        record["detail"] = str(detail)[:500]
    _logger.info(json.dumps(record, default=str))
    return record
