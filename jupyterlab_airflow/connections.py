"""Airflow **Connections** — flow-scoped (local) vs. pre-existing (remote).

PRD §6.11. The sibling of :mod:`variables`, with the same two-scope model and
the same shared ownership protocol (:mod:`managed`), but two real differences:

**How usage is detected.** A variable is referenced by *text* (``{{ var.value.k
}}``). A connection is referenced by an operator **parameter** — ``conn_id``,
``aws_conn_id``, ``http_conn_id``, … — so usage is read structurally out of the
operator registry rather than pattern-matched, which is both exact and
self-maintaining (a new operator YAML is covered with no change here). Jinja
``{{ conn.x.host }}`` and ``BaseHook.get_connection("x")`` in code bodies are
scanned too, for the code-first nodes.

**Registry defaults count.** Leaving ``aws_conn_id`` blank does not mean "no
connection" — the operator falls back to its declared default (``aws_default``),
and the task fails at run time if that connection is absent. So the *effective*
conn_id is what gets tracked, with the distinction preserved (``implicit``) so
the UI and the gates can treat a default more gently than something the author
typed deliberately.

**Enforcement.** Unlike variables, an undeclared conn_id is **not** a blocking
error: a flow authored before this feature has conn_ids and no declarations, and
those connections usually do exist in Airflow. Undeclared is a warning; the hard
gates are the ones that indicate real breakage — a *declared* remote connection
missing from Airflow, and a *local* one whose id is already taken by a
connection this flow does not own.

**Secrets.** A local connection's ``password``/``extra`` live in the ``.afdag``
in plaintext (and therefore in git). That is a deliberate, warned-about
trade-off (PRD §9); the ``remote`` scope is the sanctioned path for anything
sensitive. Airflow also *masks* secrets on read — the password and
sensitive-looking keys inside ``extra`` come back as ``***`` — so a fetched
connection is never written back wholesale.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from . import conn_types
from .managed import (  # noqa: F401  (re-exported: shared ownership protocol)
    compose_description,
    is_owned_by,
    owner_marker,
    owner_of,
    strip_marker,
)

# What Airflow substitutes for a secret it will not disclose over REST — the
# password, and any sensitive-looking key inside `extra`. Studio always writes
# from the IR (never from a fetched connection), so a masked value can never be
# round-tripped back; doing so would literally store "***" as the password.
REDACTED = "***"

# Airflow's own constraints on a connection id (ConnectionBody): letters,
# digits, underscore, dot, dash — no spaces or slashes — and at most 200 chars.
MAX_CONN_ID_LENGTH = 200
_CONN_ID_RE = re.compile(r"^[\w.-]+$")

# Fields carried on a declaration and pushed to Airflow. `conn_type` is required
# by the API; the rest are optional.
FIELDS = ("conn_type", "host", "login", "password", "schema", "port", "extra")

# A param holds a connection id when it is called `conn_id` or `<something>_conn_id`.
# Matches every connection param in the bundled registry (20 distinct names) and
# any future one following Airflow's universal naming convention.
_CONN_PARAM_RE = re.compile(r"^(?:.*_)?conn_id$")

# Code-first references, for `code`-widget nodes.
_JINJA_CONN_RE = re.compile(r"\bconn\.([A-Za-z_][A-Za-z0-9_]*)")
_JINJA_CONN_GET_RE = re.compile(r"\bconn\.get\(\s*(['\"])(.*?)\1")
_PY_CONN_RE = re.compile(
    r"\b(?:BaseHook\.get_connection|Connection\.get)\(\s*(['\"])(.*?)\1"
)


def is_conn_param(name: str) -> bool:
    """Whether an operator param name holds a connection id."""
    return bool(_CONN_PARAM_RE.match(name or ""))


def is_redacted(value: Any) -> bool:
    return value == REDACTED


# -- declarations ----------------------------------------------------------


def declared(ir: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The flow's connection declarations, normalized and de-duplicated by id.

    Tolerates a missing/None/non-list ``connections`` — a `.afdag` written
    before this feature simply has none — and skips entries without an id.
    """
    raw = ir.get("connections")
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        conn_id = str(entry.get("conn_id") or "").strip()
        if not conn_id or conn_id in seen:
            continue
        seen.add(conn_id)
        record = {
            "conn_id": conn_id,
            "scope": "remote" if entry.get("scope") == "remote" else "local",
            "description": str(entry.get("description") or ""),
        }
        for field in FIELDS:
            value = entry.get(field)
            record[field] = "" if value is None else value
        out.append(record)
    return out


def declared_ids(ir: Dict[str, Any]) -> Set[str]:
    return {entry["conn_id"] for entry in declared(ir)}


def by_scope(ir: Dict[str, Any], scope: str) -> List[Dict[str, Any]]:
    return [entry for entry in declared(ir) if entry["scope"] == scope]


# -- usage scanning --------------------------------------------------------


def _conn_param_defaults() -> Dict[str, Dict[str, Any]]:
    """``{op_id: {param_name: default}}`` for every connection param in the
    registry — the source of truth for "which params are connections, and what
    do they fall back to". Read from the registry so a new operator YAML needs
    no change here."""
    from .registry import load_registry

    index: Dict[str, Dict[str, Any]] = {}
    for op in load_registry():
        params = {
            param["name"]: param.get("default")
            for param in (op.get("params") or [])
            if isinstance(param, dict)
            and param.get("name")
            and is_conn_param(param["name"])
        }
        if params:
            index[op["id"]] = params
    return index


def _strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for nested in value.values():
            yield from _strings(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _strings(nested)


def _ids_in_code(text: str) -> Set[str]:
    """Connection ids referenced from a code body / template string."""
    found = set(_JINJA_CONN_RE.findall(text))
    found.update(match[1] for match in _JINJA_CONN_GET_RE.findall(text))
    found.update(match[1] for match in _PY_CONN_RE.findall(text))
    found.discard("get")  # `conn.get('x')` also matches the attribute form
    return {conn_id for conn_id in found if conn_id}


def usages(ir: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every connection the flow uses: ``{conn_id, where, param, implicit}``.

    ``implicit`` marks a conn_id that came from the operator's registry
    **default** because the author left the field blank — still a real runtime
    dependency, but not something they typed, so it is reported more gently.
    """
    defaults = _conn_param_defaults()
    out: List[Dict[str, Any]] = []
    nodes = ir.get("nodes")
    if not isinstance(nodes, list):
        return out

    for node in nodes:
        if not isinstance(node, dict):
            continue
        label = str(node.get("task_id") or node.get("id") or "task")
        where = f"task '{label}'"
        params = node.get("params") if isinstance(node.get("params"), dict) else {}

        for name, fallback in (defaults.get(node.get("op"), {})).items():
            value = params.get(name)
            explicit = isinstance(value, str) and value.strip() != ""
            conn_id = value.strip() if explicit else fallback
            if not conn_id:
                continue
            out.append(
                {
                    "conn_id": str(conn_id),
                    "where": where,
                    "param": name,
                    "implicit": not explicit,
                }
            )

        # A code node can reach a connection directly; and any templated field
        # can use `{{ conn.<id>.host }}`.
        texts = list(_strings(params))
        code = node.get("code")
        if isinstance(code, str) and code:
            texts.append(code)
        for text in texts:
            for conn_id in _ids_in_code(text):
                out.append(
                    {
                        "conn_id": conn_id,
                        "where": where,
                        "param": None,
                        "implicit": False,
                    }
                )
    return out


def used_ids(ir: Dict[str, Any]) -> Set[str]:
    return {use["conn_id"] for use in usages(ir)}


def references(ir: Dict[str, Any]) -> Dict[str, List[str]]:
    """``{conn_id: ["task 'x' (aws_conn_id)", …]}`` — the "used by" list, and
    the guard behind refusing to remove a connection still in use."""
    hits: Dict[str, List[str]] = {}
    for use in usages(ir):
        label = use["where"]
        if use["param"]:
            label = f"{label} ({use['param']})"
            if use["implicit"]:
                label += " — default"
        places = hits.setdefault(use["conn_id"], [])
        if label not in places:
            places.append(label)
    return hits


def blocking_usage(ir: Dict[str, Any], conn_id: str) -> List[str]:
    return references(ir).get(conn_id, [])


def undeclared(ir: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Connections a task uses that the flow does not declare.

    A **warning**, never a deploy blocker: flows written before this feature
    reference conn_ids that exist perfectly well in Airflow, and breaking them
    would be worse than the missing declaration. Each entry keeps ``implicit``
    so the UI can distinguish "you typed this" from "the operator defaults to
    it".
    """
    known = declared_ids(ir)
    out: Dict[str, Dict[str, Any]] = {}
    for use in usages(ir):
        conn_id = use["conn_id"]
        if conn_id in known:
            continue
        entry = out.setdefault(
            conn_id, {"conn_id": conn_id, "where": [], "implicit": True}
        )
        if use["where"] not in entry["where"]:
            entry["where"].append(use["where"])
        # Explicit anywhere wins: the author named it at least once.
        entry["implicit"] = entry["implicit"] and use["implicit"]
    return [out[key] for key in sorted(out)]


def unused_declarations(ir: Dict[str, Any]) -> List[str]:
    used = used_ids(ir)
    return sorted(
        entry["conn_id"] for entry in declared(ir) if entry["conn_id"] not in used
    )


# -- structural validation (pure IR, no Airflow) ---------------------------


def declaration_errors(ir: Dict[str, Any]) -> List[str]:
    """Errors in the declarations themselves. Pure and offline, so it runs in
    codegen beside the other stage-1 checks — and it catches, with a readable
    message, the three shapes Airflow itself rejects badly: an id outside its
    ``^[\\w.-]+$`` pattern (a 422), and an ``extra`` that is not a JSON *object*
    (a bare **500** with a plain-text body, for a dict, an array, or non-JSON).
    """
    errors: List[str] = []
    for entry in declared(ir):
        conn_id = entry["conn_id"]
        if len(conn_id) > MAX_CONN_ID_LENGTH:
            errors.append(
                f"Connection id '{conn_id[:40]}…' is {len(conn_id)} characters; "
                f"Airflow allows at most {MAX_CONN_ID_LENGTH}."
            )
        if not _CONN_ID_RE.match(conn_id):
            errors.append(
                f"Connection id '{conn_id}' is not valid: Airflow allows only "
                "letters, digits, underscore, dot and dash (no spaces or slashes)."
            )
        if entry["scope"] == "local":
            if not str(entry.get("conn_type") or "").strip():
                errors.append(
                    f"Connection '{conn_id}' needs a connection type (e.g. "
                    "postgres, http, aws) — Airflow requires one."
                )
            port = entry.get("port")
            if port not in ("", None):
                try:
                    int(port)
                except (TypeError, ValueError):
                    errors.append(
                        f"Connection '{conn_id}' has a non-numeric port ({port!r})."
                    )
            extra = entry.get("extra")
            if isinstance(extra, str) and extra.strip():
                try:
                    parsed = json.loads(extra)
                except ValueError:
                    errors.append(
                        f"Connection '{conn_id}' has an Extra field that is not "
                        "valid JSON. Fix it or leave it blank."
                    )
                else:
                    # Airflow requires an *object* here: a JSON array (or any
                    # other scalar) makes its own endpoint raise a plain-text
                    # 500 rather than a usable error, so catch it up front.
                    if not isinstance(parsed, dict):
                        errors.append(
                            f"Connection '{conn_id}' has an Extra field that is "
                            "valid JSON but not an object — Airflow needs "
                            'something like {"sslmode": "require"}.'
                        )
                    else:
                        # Values a known provider outright rejects — e.g. an
                        # SMTP `ssl_context` other than default/none, which
                        # raises only when the mail is actually sent (PRD §6.13).
                        for detail in conn_types.extra_errors(
                            entry.get("conn_type"), parsed
                        ):
                            errors.append(f"Connection '{conn_id}': {detail}")
    return errors


# -- Airflow-aware checks + lifecycle --------------------------------------


def _index(client) -> Optional[Dict[str, Dict[str, Any]]]:
    """``{conn_id: connection}`` for the target, or ``None`` when Airflow is
    unreachable (callers then skip their checks rather than block a deploy on an
    infrastructure blip)."""
    from .client import AirflowError

    try:
        payload = client.list_connections()
    except AirflowError:
        return None
    entries = payload.get("connections") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return {}
    return {
        str(entry.get("connection_id")): entry
        for entry in entries
        if isinstance(entry, dict) and entry.get("connection_id")
    }


def block_errors(ir: Dict[str, Any], client=None) -> List[str]:
    """Deploy-gate errors needing the live Airflow (PRD §6.11):

    * a **declared remote** connection that no longer exists there;
    * a **local** id already taken by a connection this flow does not own.

    Empty when Airflow is unreachable. Undeclared/implicit conn_ids are
    deliberately *not* blocked here — see :func:`deploy_warnings`.
    """
    entries = declared(ir)
    if not entries:
        return []
    if client is None:
        from .client import get_client

        client = get_client()
    index = _index(client)
    if index is None:
        return []

    dag_id = str((ir.get("dag") or {}).get("dag_id") or "")
    errors: List[str] = []
    for entry in entries:
        conn_id = entry["conn_id"]
        existing = index.get(conn_id)
        if entry["scope"] == "remote":
            if existing is None:
                errors.append(
                    f"Airflow connection '{conn_id}' is used by this flow but no "
                    "longer exists in Airflow. Create it in Airflow, or remove it "
                    "from the CONNECTIONS tab."
                )
        elif existing is not None and not is_owned_by(existing.get("description"), dag_id):
            owner = owner_of(existing.get("description"))
            whose = f"flow '{owner}'" if owner else "someone outside Studio"
            errors.append(
                f"Connection '{conn_id}' is defined as a flow connection here, "
                f"but one with that id already exists in Airflow (created by "
                f"{whose}). Rename it, or reference it as an Airflow connection "
                "instead of redefining it."
            )
    return errors


def deploy_warnings(ir: Dict[str, Any], client=None) -> List[str]:
    """Non-blocking deploy notes: connections a task uses that are neither
    declared here nor present in the target Airflow — i.e. the task will fail
    when it runs, but the DAG itself is fine and may be intentional."""
    pending = undeclared(ir)
    if not pending:
        return []
    if client is None:
        from .client import get_client

        client = get_client()
    index = _index(client)
    if index is None:
        return []
    warnings: List[str] = []
    for entry in pending:
        if entry["conn_id"] in index:
            continue
        where = ", ".join(entry["where"])
        how = (
            "the operator default"
            if entry["implicit"]
            else "used by " + where
        )
        warnings.append(
            f"Connection '{entry['conn_id']}' ({how}) does not exist in Airflow; "
            f"{where} will fail at run time until it is created."
        )
    return warnings


def sync(ir: Dict[str, Any], client=None) -> Tuple[List[str], List[str]]:
    """Push this flow's **local** connections into Airflow → ``(synced, warnings)``.

    Remote connections are never touched. A failure is reported as a warning
    rather than raised, matching the variables sync: the DAG file is already
    written, and a visible warning beats an exception that hides what landed.
    """
    entries = by_scope(ir, "local")
    if not entries:
        return [], []
    if client is None:
        from .client import get_client

        client = get_client()
    from .client import AirflowError

    dag_id = str((ir.get("dag") or {}).get("dag_id") or "")
    index = _index(client)
    synced: List[str] = []
    warnings: List[str] = []
    for entry in entries:
        conn_id = entry["conn_id"]
        fields = {name: entry.get(name) for name in FIELDS if name != "conn_type"}
        fields["description"] = compose_description(dag_id, entry["description"])
        conn_type = str(entry.get("conn_type") or "").strip()
        exists = index is not None and conn_id in index
        try:
            if exists:
                client.update_connection(conn_id, conn_type, **fields)
            else:
                client.create_connection(conn_id, conn_type, **fields)
            synced.append(conn_id)
        except AirflowError as err:
            if not exists and err.status == 409:
                warnings.append(
                    f"Connection '{conn_id}' already existed in Airflow and was "
                    "left unchanged."
                )
                continue
            warnings.append(f"Could not set Airflow connection '{conn_id}': {err}")
    return synced, warnings


def purge(dag_id: str, client=None) -> List[str]:
    """Delete every connection this flow owns → the ids removed.

    Driven by the ownership marker, so this works from a ``dag_id`` alone and
    can never remove a connection Studio did not create.
    """
    if client is None:
        from .client import get_client

        client = get_client()
    from .client import AirflowError

    index = _index(client)
    if not index:
        return []
    removed: List[str] = []
    for conn_id, entry in index.items():
        if not is_owned_by(entry.get("description"), dag_id):
            continue
        try:
            client.delete_connection(conn_id)
            removed.append(conn_id)
        except AirflowError as err:
            if err.status != 404:
                raise
    return removed


class ConnectionOwnershipError(Exception):
    """A write was refused because the flow does not own the target connection."""


def set_one(
    dag_id: str,
    conn_id: str,
    conn_type: str,
    fields: Optional[Dict[str, Any]] = None,
    client=None,
) -> Dict[str, Any]:
    """Create/update one connection owned by ``dag_id``, enforcing ownership
    server-side rather than trusting the UI."""
    conn_id = (conn_id or "").strip()
    if not conn_id:
        raise ConnectionOwnershipError("A connection id is required.")
    if not (conn_type or "").strip():
        raise ConnectionOwnershipError("A connection type is required.")
    if client is None:
        from .client import get_client

        client = get_client()
    from .client import AirflowError

    try:
        existing = client.get_connection(conn_id)
    except AirflowError as err:
        if err.status != 404:
            raise
        existing = None

    if existing is not None and not is_owned_by(existing.get("description"), dag_id):
        owner = owner_of(existing.get("description"))
        whose = f"the flow '{owner}'" if owner else "something outside Studio"
        raise ConnectionOwnershipError(
            f"Connection '{conn_id}' already exists in Airflow and belongs to "
            f"{whose}. This flow can use it, but cannot change it."
        )

    payload = dict(fields or {})
    payload["description"] = compose_description(dag_id, payload.get("description", ""))
    try:
        if existing is None:
            client.create_connection(conn_id, conn_type, **payload)
        else:
            client.update_connection(conn_id, conn_type, **payload)
    except AirflowError as err:
        if err.status == 409:
            # Raced with another writer between the existence check and here.
            # Airflow's 409 body is a dict carrying the raw INSERT statement —
            # never surface that; answer with the ownership language instead.
            raise ConnectionOwnershipError(
                f"Connection '{conn_id}' was just created by someone else. "
                "Reload the tab, then reference it as an Airflow connection."
            ) from err
        raise
    return {"conn_id": conn_id, "created": existing is None}


def delete_one(dag_id: str, conn_id: str, client=None) -> Dict[str, Any]:
    """Delete one connection, only if ``dag_id`` owns it."""
    conn_id = (conn_id or "").strip()
    if not conn_id:
        raise ConnectionOwnershipError("A connection id is required.")
    if client is None:
        from .client import get_client

        client = get_client()
    from .client import AirflowError

    try:
        existing = client.get_connection(conn_id)
    except AirflowError as err:
        if err.status != 404:
            raise
        return {"conn_id": conn_id, "deleted": False}

    if not is_owned_by(existing.get("description"), dag_id):
        owner = owner_of(existing.get("description"))
        whose = f"the flow '{owner}'" if owner else "something outside Studio"
        raise ConnectionOwnershipError(
            f"Connection '{conn_id}' belongs to {whose}, so this flow cannot "
            "delete it."
        )
    client.delete_connection(conn_id)
    return {"conn_id": conn_id, "deleted": True}


def annotated(ir: Dict[str, Any], client=None) -> Dict[str, Any]:
    """The CONNECTIONS tab payload: declarations reconciled against the live
    Airflow, plus everything available to reference."""
    if client is None:
        from .client import get_client

        client = get_client()
    index = _index(client)
    reachable = index is not None
    index = index or {}
    dag_id = str((ir.get("dag") or {}).get("dag_id") or "")
    used = references(ir)

    entries: List[Dict[str, Any]] = []
    for entry in declared(ir):
        conn_id = entry["conn_id"]
        existing = index.get(conn_id)
        # Advisory notes for a known connection type (PRD §6.13) — e.g. the SMTP
        # TLS defaults, which are counterintuitive enough to be worth saying out
        # loud on the connection that configures them.
        parsed_extra: Any = {}
        raw_extra = entry.get("extra")
        if isinstance(raw_extra, str) and raw_extra.strip():
            try:
                parsed_extra = json.loads(raw_extra)
            except ValueError:
                parsed_extra = {}
        entries.append(
            {
                **entry,
                "used_by": used.get(conn_id, []),
                "exists": existing is not None,
                "owned": is_owned_by((existing or {}).get("description"), dag_id),
                "redacted": is_redacted((existing or {}).get("password")),
                "airflow_conn_type": (existing or {}).get("conn_type"),
                "hints": conn_types.extra_hints(entry.get("conn_type"), parsed_extra),
            }
        )

    known = declared_ids(ir)
    available = [
        {
            "conn_id": conn_id,
            "conn_type": entry.get("conn_type"),
            "description": strip_marker(entry.get("description")),
            "owner": owner_of(entry.get("description")),
            "declared": conn_id in known,
        }
        for conn_id, entry in sorted(index.items())
    ]
    pending = [
        {**entry, "exists_in_airflow": entry["conn_id"] in index}
        for entry in undeclared(ir)
    ]
    return {
        "connections": entries,
        "available": available,
        "undeclared": pending,
        "unused": unused_declarations(ir),
        "airflow_reachable": reachable,
        # Curated per-type `extra` metadata (PRD §6.13) so the tab can offer
        # real field help and presets instead of an opaque JSON box. Keyed by
        # conn_type; only the types actually declared here are sent.
        "type_hints": {
            conn_type: conn_types.hints_for(conn_type)
            for conn_type in {
                str(entry.get("conn_type") or "").strip().lower()
                for entry in declared(ir)
            }
            if conn_types.hints_for(conn_type)
        },
    }
