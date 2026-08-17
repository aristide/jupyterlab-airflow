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
from datetime import datetime, timezone
from typing import Any, Dict, Optional

#: The dedicated logger name a deployment can target with logging config to route
#: the audit trail (e.g. to a file or SIEM).
AUDIT_LOGGER_NAME = "jupyterlab_airflow.audit"

_logger = logging.getLogger(AUDIT_LOGGER_NAME)

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
