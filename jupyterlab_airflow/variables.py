"""Airflow **Variables** — flow-scoped (local) vs. pre-existing (remote).

PRD §6.10. A Studio flow can declare the variables it needs on ``ir.variables``
(a top-level collection, like ``notes`` — never a node or an edge, so the task
graph, cycle check and codegen are untouched). Each declaration is one of two
**scopes**:

``local``
    Owned by *this* flow. Its value lives in the ``.afdag``, is pushed into
    Airflow on deploy, and is deleted again when the flow is undeployed or
    purged. Studio is the source of truth: a re-deploy overwrites the value in
    Airflow.

``remote``
    A variable that already existed in Airflow when the flow was authored
    (set by an operator, another flow, or a secrets backend). The flow may only
    **use** it: Studio never creates, updates or deletes a remote variable. It
    is verified to still exist at deploy time, because it can change or vanish
    while the flow is being edited.

**Ownership marker.** Local variables carry a marker in their Airflow
``description`` (``[airflow-studio dag_id=<id>] …``). That is what makes the
lifecycle safe and recoverable: ``purge_dag`` only receives a ``dag_id`` (not
the IR), so ownership has to be discoverable from Airflow itself — and it
guarantees Studio never deletes or overwrites a variable it did not create.
It also self-documents in the Airflow UI. This mirrors the ``# airflow-studio:
managed`` provenance header that ``deploy.py`` stamps into generated ``.py``.

**Secrets.** A ``local`` variable's value is stored **in plaintext** in the
``.afdag`` document (and is therefore committed to git with it). Per PRD §9,
secrets belong in a ``remote`` variable — created directly in Airflow (or via a
secrets backend) and merely referenced here, so the value never touches Studio.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Airflow's own limit on a variable key (VariableBody.key, maxLength 250).
MAX_KEY_LENGTH = 250

# What Airflow returns instead of a value it considers sensitive. Redaction is
# key-based (the key contains `password`/`secret`/`token`/`api_key`/… ) *and*
# structural (nested sensitive fields, one level deep) — verified against 3.0.2.
# A redacted value is unreadable over REST, so it must never be written back:
# doing so would replace a real secret with three asterisks.
REDACTED = "***"


def is_redacted(value: Any) -> bool:
    return value == REDACTED

_MARKER_RE = re.compile(r"^\[airflow-studio dag_id=([^\]]*)\]\s?")

# How a task references a variable. Both forms are detected so that "is this
# variable still used?" is answerable regardless of which one the author chose:
#
#   Jinja, in any templated operator field (resolved by Airflow at run time):
#       {{ var.value.my_key }}        {{ var.json.my_key }}
#       {{ var.value.get('my-key') }} {{ var.json.get("my-key") }}
#   Python, inside a code-node body (task runtime, via the Task SDK):
#       Variable.get("my_key")        Variable.get('my_key', deserialize_json=True)
#
# The dotted form only matches identifier-shaped keys, which is all Airflow's
# attribute access supports; a key with a dot/dash *must* use the .get() form.
_JINJA_DOT_RE = re.compile(r"\bvar\.(?:value|json)\.([A-Za-z_][A-Za-z0-9_]*)")
_JINJA_GET_RE = re.compile(r"\bvar\.(?:value|json)\.get\(\s*(['\"])(.*?)\1")
_PY_GET_RE = re.compile(r"\bVariable\.get\(\s*(['\"])(.*?)\1")


# -- declarations ----------------------------------------------------------


def declared(ir: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The flow's variable declarations, normalized and de-duplicated by key.

    Tolerates a missing/None/non-list ``variables`` (older ``.afdag`` files have
    no such collection at all) and skips entries without a usable key.
    """
    raw = ir.get("variables")
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("key") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        scope = "remote" if entry.get("scope") == "remote" else "local"
        out.append(
            {
                "key": key,
                "scope": scope,
                "value": "" if entry.get("value") is None else str(entry.get("value")),
                "description": str(entry.get("description") or ""),
                "var_type": "json" if entry.get("var_type") == "json" else "string",
                "default": entry.get("default"),
            }
        )
    return out


def declared_keys(ir: Dict[str, Any]) -> set:
    return {entry["key"] for entry in declared(ir)}


def by_scope(ir: Dict[str, Any], scope: str) -> List[Dict[str, Any]]:
    return [entry for entry in declared(ir) if entry["scope"] == scope]


# -- ownership marker ------------------------------------------------------


def owner_marker(dag_id: str) -> str:
    """The description prefix stamped onto every variable this flow creates."""
    return f"[airflow-studio dag_id={dag_id}]"


def compose_description(dag_id: str, description: str = "") -> str:
    """Airflow-side description for a local variable: the ownership marker plus
    the author's own text."""
    text = (description or "").strip()
    return f"{owner_marker(dag_id)} {text}".strip()


def owner_of(description: Optional[str]) -> Optional[str]:
    """The ``dag_id`` that owns a variable, read back from its description, or
    ``None`` when the variable is not Studio-managed."""
    match = _MARKER_RE.match(description or "")
    return match.group(1) if match else None


def strip_marker(description: Optional[str]) -> str:
    """The author's description with the ownership marker removed."""
    return _MARKER_RE.sub("", description or "").strip()


def is_owned_by(description: Optional[str], dag_id: str) -> bool:
    return owner_of(description) == dag_id


# -- reference scanning ----------------------------------------------------


def _strings(value: Any) -> Iterable[str]:
    """Every string nested anywhere in a JSON-ish value."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for nested in value.values():
            yield from _strings(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _strings(nested)


def _keys_in(text: str) -> set:
    """Variable keys referenced by one string (either syntax)."""
    found = set(_JINJA_DOT_RE.findall(text))
    found.update(match[1] for match in _JINJA_GET_RE.findall(text))
    found.update(match[1] for match in _PY_GET_RE.findall(text))
    # `var.value.get('x')` also matches the dotted pattern with key "get";
    # that is never a real reference.
    found.discard("get")
    return {key for key in found if key}


def references(ir: Dict[str, Any]) -> Dict[str, List[str]]:
    """Map every referenced variable key → the places that reference it.

    Scans task params and code bodies (where a reference actually resolves at
    run time) plus the DAG-level ``params``/``default_args``. Locations are
    human-readable so they can go straight into an error message or the UI's
    "used by" list.
    """
    hits: Dict[str, List[str]] = {}

    def _record(key: str, where: str) -> None:
        places = hits.setdefault(key, [])
        if where not in places:
            places.append(where)

    dag = ir.get("dag") if isinstance(ir.get("dag"), dict) else {}
    for field in ("params", "default_args"):
        for text in _strings(dag.get(field)):
            for key in _keys_in(text):
                _record(key, f"DAG {field}")

    nodes = ir.get("nodes")
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            label = str(node.get("task_id") or node.get("id") or "task")
            where = f"task '{label}'"
            for text in _strings(node.get("params")):
                for key in _keys_in(text):
                    _record(key, where)
            code = node.get("code")
            if isinstance(code, str) and code:
                for key in _keys_in(code):
                    _record(key, where)
    return hits


def referenced_keys(ir: Dict[str, Any]) -> set:
    return set(references(ir))


def uses_variable_api(ir: Dict[str, Any]) -> bool:
    """True when some code body calls ``Variable.get`` — the signal that the
    generated module needs ``from airflow.sdk import Variable``. Jinja-only
    references need no import (Airflow resolves them at run time)."""
    nodes = ir.get("nodes")
    if not isinstance(nodes, list):
        return False
    for node in nodes:
        if not isinstance(node, dict):
            continue
        code = node.get("code")
        if isinstance(code, str) and _PY_GET_RE.search(code):
            return True
    return False


# -- structural validation (pure IR, no Airflow) ---------------------------


def declaration_errors(ir: Dict[str, Any]) -> List[str]:
    """Errors in the declarations themselves — bad keys, unparseable JSON.

    Pure and offline, so it can run in codegen alongside the other stage-1
    checks. "Is it in Airflow?" lives in :func:`block_errors` instead.
    """
    errors: List[str] = []
    for entry in declared(ir):
        key = entry["key"]
        if len(key) > MAX_KEY_LENGTH:
            errors.append(
                f"Variable key '{key[:40]}…' is {len(key)} characters; "
                f"Airflow allows at most {MAX_KEY_LENGTH}."
            )
        # Airflow itself accepts whitespace in a key, so this is only a guardrail
        # on keys *we* create — a remote key must be referenceable exactly as it
        # already exists in Airflow, whatever it is called.
        if entry["scope"] == "local" and any(ch.isspace() for ch in key):
            errors.append(
                f"Variable key '{key}' cannot contain whitespace. Use letters, "
                "digits and underscores so it can be referenced as "
                "{{ var.value.<key> }}."
            )
        if entry["scope"] == "local" and entry["var_type"] == "json":
            try:
                json.loads(entry["value"] or "")
            except ValueError:
                errors.append(
                    f"Variable '{key}' is declared as JSON but its value is not "
                    "valid JSON. Fix the value or switch its type to text."
                )
    return errors


def undefined_reference_errors(ir: Dict[str, Any]) -> List[str]:
    """Errors for variables a task uses that the flow never declares (PRD §6.10).

    This is the "used but not defined" guard: without it the DAG parses fine and
    only fails at run time, inside the task, with an opaque Airflow error.
    """
    known = declared_keys(ir)
    errors: List[str] = []
    for key, places in sorted(references(ir).items()):
        if key in known:
            continue
        where = ", ".join(places)
        errors.append(
            f"Variable '{key}' is used by {where} but is not defined in this "
            "flow. Add it on the VARIABLES tab — as a flow variable if this "
            "flow should create it, or as an Airflow variable if it already "
            "exists in Airflow."
        )
    return errors


def unused_declarations(ir: Dict[str, Any]) -> List[str]:
    """Keys declared but referenced nowhere (a warning, never an error)."""
    used = referenced_keys(ir)
    return sorted(entry["key"] for entry in declared(ir) if entry["key"] not in used)


def blocking_usage(ir: Dict[str, Any], key: str) -> List[str]:
    """Where ``key`` is still used — non-empty means "refuse to remove it"."""
    return references(ir).get(key, [])


# -- Airflow-aware checks + lifecycle --------------------------------------


def _index(client) -> Optional[Dict[str, Dict[str, Any]]]:
    """``{key: variable}`` for every variable in the target, or ``None`` when
    Airflow is unreachable (callers then skip their checks rather than block a
    deploy on an infrastructure blip — the same contract as
    ``providers.get_target_index``)."""
    from .client import AirflowError

    try:
        payload = client.list_variables()
    except AirflowError:
        return None
    entries = payload.get("variables") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return {}
    return {
        str(entry.get("key")): entry
        for entry in entries
        if isinstance(entry, dict) and entry.get("key")
    }


def block_errors(ir: Dict[str, Any], client=None) -> List[str]:
    """Deploy-gate errors that need the live Airflow (PRD §6.10).

    * a **remote** variable the flow references no longer exists there;
    * a **local** variable's key is already taken by a variable this flow does
      not own — deploying would silently clobber someone else's value.

    Empty when Airflow is unreachable, so a network blip never blocks a deploy
    (``/importErrors`` remains the authoritative post-deploy verdict).
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
        key = entry["key"]
        existing = index.get(key)
        if entry["scope"] == "remote":
            if existing is None:
                errors.append(
                    f"Airflow variable '{key}' is referenced by this flow but no "
                    "longer exists in Airflow. Create it in Airflow, or remove "
                    "it from the VARIABLES tab."
                )
        elif existing is not None and not is_owned_by(existing.get("description"), dag_id):
            owner = owner_of(existing.get("description"))
            whose = f"flow '{owner}'" if owner else "someone outside Studio"
            errors.append(
                f"Variable '{key}' is defined as a flow variable here, but a "
                f"variable with that key already exists in Airflow (created by "
                f"{whose}). Rename it, or reference it as an Airflow variable "
                "instead of redefining it."
            )
    return errors


def sync(ir: Dict[str, Any], client=None) -> Tuple[List[str], List[str]]:
    """Push this flow's **local** variables into Airflow. Returns ``(created,
    warnings)``.

    Called on deploy, after the gate has confirmed no key collides with a
    variable the flow does not own. Remote variables are never touched. A
    failure is reported as a warning rather than raised: the DAG file is already
    written, and a half-deployed flow with a visible warning beats an exception
    that hides what did land.
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
        key = entry["key"]
        value = entry["value"]
        description = compose_description(dag_id, entry["description"])
        exists = index is not None and key in index
        try:
            if exists:
                client.update_variable(key, value, description)
            else:
                client.create_variable(key, value, description)
            synced.append(key)
        except AirflowError as err:
            if not exists and err.status == 409:
                # Raced with another writer between the gate and here.
                warnings.append(
                    f"Variable '{key}' already existed in Airflow and was left "
                    "unchanged."
                )
                continue
            warnings.append(f"Could not set Airflow variable '{key}': {err}")
    return synced, warnings


def purge(dag_id: str, client=None) -> List[str]:
    """Delete every variable this flow owns. Returns the keys removed.

    Driven entirely by the ownership marker, because the teardown paths
    (undeploy, orphan purge, manager delete) only know a ``dag_id`` — and
    because ownership is what makes the delete safe: a variable Studio did not
    create is never touched.
    """
    if client is None:
        from .client import get_client

        client = get_client()
    from .client import AirflowError

    index = _index(client)
    if not index:
        return []
    removed: List[str] = []
    for key, entry in index.items():
        if not is_owned_by(entry.get("description"), dag_id):
            continue
        try:
            client.delete_variable(key)
            removed.append(key)
        except AirflowError as err:
            if err.status != 404:
                raise
    return removed


class VariableOwnershipError(Exception):
    """A write was refused because the flow does not own the target variable."""


def set_one(
    dag_id: str,
    key: str,
    value: str,
    description: str = "",
    client=None,
) -> Dict[str, Any]:
    """Create or update **one** variable owned by ``dag_id``, immediately.

    Enforces the ownership rule server-side rather than trusting the UI: a
    variable that exists and is not owned by this flow (another flow's, or one
    created outside Studio) is refused. That is the "a flow may use, but never
    modify, a remote variable" guarantee.
    """
    key = (key or "").strip()
    if not key:
        raise VariableOwnershipError("A variable key is required.")
    if len(key) > MAX_KEY_LENGTH:
        raise VariableOwnershipError(
            f"Variable key is too long ({len(key)} > {MAX_KEY_LENGTH})."
        )
    if client is None:
        from .client import get_client

        client = get_client()
    from .client import AirflowError

    try:
        existing = client.get_variable(key)
    except AirflowError as err:
        if err.status != 404:
            raise
        existing = None

    if existing is not None and not is_owned_by(existing.get("description"), dag_id):
        owner = owner_of(existing.get("description"))
        whose = f"the flow '{owner}'" if owner else "something outside Studio"
        raise VariableOwnershipError(
            f"Variable '{key}' already exists in Airflow and belongs to {whose}. "
            "This flow can use it as an Airflow variable, but cannot change it."
        )

    composed = compose_description(dag_id, description)
    if existing is None:
        client.create_variable(key, value, composed)
    else:
        client.update_variable(key, value, composed)
    return {"key": key, "created": existing is None}


def delete_one(dag_id: str, key: str, client=None) -> Dict[str, Any]:
    """Delete **one** variable, only if ``dag_id`` owns it."""
    key = (key or "").strip()
    if not key:
        raise VariableOwnershipError("A variable key is required.")
    if client is None:
        from .client import get_client

        client = get_client()
    from .client import AirflowError

    try:
        existing = client.get_variable(key)
    except AirflowError as err:
        if err.status != 404:
            raise
        return {"key": key, "deleted": False}

    if not is_owned_by(existing.get("description"), dag_id):
        owner = owner_of(existing.get("description"))
        whose = f"the flow '{owner}'" if owner else "something outside Studio"
        raise VariableOwnershipError(
            f"Variable '{key}' belongs to {whose}, so this flow cannot delete it."
        )
    client.delete_variable(key)
    return {"key": key, "deleted": True}


def annotated(ir: Dict[str, Any], client=None) -> Dict[str, Any]:
    """The VARIABLES tab payload: what the flow declares, what Airflow actually
    has, and how the two line up.

    ``declared`` entries gain ``exists``/``owned``/``in_airflow_value`` and a
    ``used_by`` list (so the UI can refuse to remove a variable still in use);
    ``available`` lists every variable in the target, marked with the flow that
    owns it, for the "add an existing Airflow variable" picker.
    """
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
        key = entry["key"]
        existing = index.get(key)
        airflow_value = (existing or {}).get("value")
        entries.append(
            {
                **entry,
                "used_by": used.get(key, []),
                "exists": existing is not None,
                "owned": is_owned_by((existing or {}).get("description"), dag_id),
                # Withheld by Airflow (sensitive-looking key) — the UI shows
                # "hidden" rather than the literal ***, and must never offer to
                # copy it back into the flow.
                "redacted": is_redacted(airflow_value),
                "airflow_value": None if is_redacted(airflow_value) else airflow_value,
            }
        )

    known = declared_keys(ir)
    available = [
        {
            "key": key,
            "description": strip_marker(entry.get("description")),
            "owner": owner_of(entry.get("description")),
            "declared": key in known,
        }
        for key, entry in sorted(index.items())
    ]
    undefined = sorted(key for key in used if key not in known)
    return {
        "variables": entries,
        "available": available,
        "undefined": undefined,
        "unused": unused_declarations(ir),
        "airflow_reachable": reachable,
    }
