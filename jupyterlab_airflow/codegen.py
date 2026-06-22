"""IR → Airflow 3.x Python code generation (server-side, authoritative).

Turns an `.afdag` IR (Appendix B) into a TaskFlow DAG (Appendix C) using the
operator registry's Jinja2 templates. MVP emits **TaskFlow only** (PRD §6.3);
the traditional backend ships in v1.1.

The pipeline mirrors PRD Appendix E and is **fail-fast and side-effect free** —
no user code is ever executed. `ast.parse` and `compile(..., 'exec')` validate
syntax and scoping without running anything.

All emitted values go through a safe emitter (`pyrepr` ≈ ``repr``); templates come
only from the registry, never from user input, and Jinja runs with
``autoescape=False`` (HTML escaping would corrupt Python).
"""

from __future__ import annotations

import ast
import hashlib
import json
import keyword
from typing import Any, Dict, List, Optional, Tuple

from jinja2 import Environment, Undefined

from .registry import load_notifiers, load_registry

STUDIO_VERSION = "0.1.0"


class CodegenError(Exception):
    """A hard validation failure that prevents codegen (returned to the UI)."""


# --------------------------------------------------------------------------- #
# Safe value emitter + Jinja environment
# --------------------------------------------------------------------------- #
class _Raw(str):
    """A value emitted **verbatim** (not ``repr``'d) — e.g. a ``timedelta(...)``
    expression for a per-task ``retry_delay``."""


def _pyrepr(value: Any) -> str:
    """Emit a Python literal for a JSON-derived value. ``repr`` already yields
    valid Python for str/int/float/bool/None/list/dict, and escapes strings."""
    if isinstance(value, _Raw):
        return str(value)  # already a Python expression — emit as-is
    if isinstance(value, Undefined):
        value = None
    return repr(value)


def _pyargs(common: Any) -> str:
    """Render a common-params mapping as trailing ``key=value`` kwargs."""
    if not isinstance(common, dict) or not common:
        return ""
    return ", ".join(f"{key}={_pyrepr(val)}" for key, val in common.items())


# Per-task common settings emitted on a task (overriding the DAG defaults). Most
# pass through as literals; `retry_delay` is a ``timedelta`` and the rest are
# coerced to the type Airflow expects. Only values the user explicitly set reach
# here (see `_node_common`).
_COMMON_TIMEDELTA = ("retry_delay",)
_COMMON_INT = ("retries", "poke_interval", "timeout")
_COMMON_BOOL = ("depends_on_past",)


def _node_common(node: Dict[str, Any], op: Dict[str, Any]) -> Dict[str, Any]:
    """Build the ``common`` kwargs dict for a node from its ``common`` slot,
    restricted to the params the operator declares in ``common_params`` and in
    that order (so output stays deterministic). ``retry_delay`` becomes a
    ``timedelta`` expression; ints/bools are coerced; unset/blank are skipped."""
    declared = op.get("common_params") or []
    values = node.get("common")
    if not isinstance(values, dict):
        return {}
    out: Dict[str, Any] = {}
    for name in declared:
        if name not in values:
            continue
        val = values[name]
        if val is None or val == "":
            continue
        if name in _COMMON_TIMEDELTA or name in _COMMON_INT:
            try:
                seconds = int(val)
            except (TypeError, ValueError):
                continue
            out[name] = (
                _Raw(f"timedelta(seconds={seconds})")
                if name in _COMMON_TIMEDELTA
                else seconds
            )
        elif name in _COMMON_BOOL:
            out[name] = bool(val)
        else:  # mode (and any future string common param)
            out[name] = val
    return out


def _make_env() -> Environment:
    env = Environment(autoescape=False, keep_trailing_newline=False)
    env.filters["pyrepr"] = _pyrepr
    env.filters["pyargs"] = _pyargs
    return env


# --------------------------------------------------------------------------- #
# Graph helpers
# --------------------------------------------------------------------------- #
def _topo_order(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]
) -> Optional[List[Dict[str, Any]]]:
    """Kahn's algorithm. Returns nodes in a deterministic topological order, or
    ``None`` if the graph has a cycle. Ties break by original node order."""
    index = {node["id"]: pos for pos, node in enumerate(nodes)}
    indegree = {node["id"]: 0 for node in nodes}
    adjacency: Dict[str, List[str]] = {node["id"]: [] for node in nodes}
    for edge in edges:
        src, tgt = edge.get("source"), edge.get("target")
        if src in indegree and tgt in indegree:
            indegree[tgt] += 1
            adjacency[src].append(tgt)

    # Stable queue: always pop the ready node with the smallest original index.
    ready = sorted((nid for nid, d in indegree.items() if d == 0), key=index.get)
    order: List[str] = []
    while ready:
        nid = ready.pop(0)
        order.append(nid)
        for nxt in adjacency[nid]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                ready.append(nxt)
        ready.sort(key=index.get)

    if len(order) != len(nodes):
        return None  # cycle
    by_id = {node["id"]: node for node in nodes}
    return [by_id[nid] for nid in order]


# --------------------------------------------------------------------------- #
# @dag decorator
# --------------------------------------------------------------------------- #
def _parse_start_date(raw: Any) -> Optional[str]:
    """`"2026-01-01"` -> `datetime(2026, 1, 1)`; None when unparseable."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        year, month, day = (int(part) for part in raw.split("T")[0].split("-"))
        return f"datetime({year}, {month}, {day})"
    except (ValueError, TypeError):
        return None


def _default_args(dag: Dict[str, Any]) -> str:
    parts: List[str] = []
    retries = dag.get("retries")
    if retries is not None:
        parts.append(f'"retries": {int(retries)}')
    delay = dag.get("retry_delay_seconds")
    if delay is not None:
        parts.append(f'"retry_delay": timedelta(seconds={int(delay)})')
    owner = dag.get("owner")
    if owner:
        parts.append(f'"owner": {_pyrepr(owner)}')
    for key, val in (dag.get("default_args") or {}).items():
        parts.append(f"{_pyrepr(str(key))}: {_pyrepr(val)}")
    return "{" + ", ".join(parts) + "}"


def _dag_args(dag: Dict[str, Any], extra: Optional[List[str]] = None) -> List[str]:
    """The shared ``DAG(...)`` keyword args (used by both the TaskFlow ``@dag``
    decorator and the Traditional ``with DAG(...)`` context). ``extra`` carries
    the rendered ``on_*_callback=[...]`` notification kwargs (PRD §6.8)."""
    args: List[str] = [f'dag_id={_pyrepr(dag["dag_id"])}']
    if dag.get("description"):
        args.append(f'description={_pyrepr(dag["description"])}')
    schedule = dag.get("schedule")
    args.append(f"schedule={_pyrepr(schedule) if schedule else 'None'}")
    start_date = _parse_start_date(dag.get("start_date"))
    if start_date:
        args.append(f"start_date={start_date}")
    args.append(f"catchup={'True' if dag.get('catchup') else 'False'}")
    args.append(f"default_args={_default_args(dag)}")
    if dag.get("tags"):
        args.append(f"tags={_pyrepr(list(dag['tags']))}")
    if dag.get("params"):
        args.append(f"params={_pyrepr(dag['params'])}")
    if extra:
        args.extend(extra)
    return args


def _dag_decorator(dag: Dict[str, Any], callback_args: Optional[List[str]] = None) -> str:
    body = "".join(f"    {arg},\n" for arg in _dag_args(dag, callback_args))
    return f"@dag(\n{body})"


def _dag_context(dag: Dict[str, Any], callback_args: Optional[List[str]] = None) -> str:
    """The Traditional ``with DAG(...) as dag:`` line (PRD §6.3)."""
    body = "".join(f"    {arg},\n" for arg in _dag_args(dag, callback_args))
    return f"with DAG(\n{body}) as dag:"


# DAG-level callback events ``@dag`` / ``DAG`` accept and that still FIRE in
# Airflow 3. ``sla_miss_callback`` is excluded: the SLA feature was removed in
# Airflow 3.0 (the kwarg only emits a DeprecationWarning and never fires;
# "Deadline Alerts" in 3.1+ is the replacement).
_DAG_CALLBACK_EVENTS = ("on_success", "on_failure")

# Per-task callback events the operator (and the ``@task`` decorator, which
# forwards them) accept and that FIRE in Airflow 3 (PRD §6.8). ``on_retry`` is
# the task-only event the DAG level can't express. (``on_execute`` /
# ``on_skipped`` also fire but are deliberately not surfaced — execute is noisy
# and skipped is niche; both can be added later without an IR change.)
_TASK_CALLBACK_EVENTS = ("on_success", "on_failure", "on_retry")


def _render_callback_entries(
    entries: List[Dict[str, Any]],
    notifiers: Dict[str, Dict[str, Any]],
    env: Environment,
    used: List[Dict[str, Any]],
) -> List[str]:
    """Render a list of ``{notifier_id, params}`` callback entries to notifier
    instance expressions via each notifier's registry ``template``, appending the
    notifier defs used to ``used`` (so their imports get collected). Shared by the
    DAG-level (``_build_callbacks``) and per-task (``_node_callbacks``) paths."""
    exprs: List[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        notifier = notifiers.get(entry.get("notifier_id"))
        if not notifier:
            raise CodegenError(f"Unknown notifier: {entry.get('notifier_id')!r}")
        template = notifier.get("template")
        if not template:
            raise CodegenError(f"Notifier {notifier['id']!r} has no template")
        expr = env.from_string(template).render(
            params=entry.get("params") or {}
        ).strip()
        exprs.append(expr)
        used.append(notifier)
    return exprs


def _build_callbacks(
    dag: Dict[str, Any],
    notifiers: Dict[str, Dict[str, Any]],
    env: Environment,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Render the DAG-level ``on_*_callback`` kwargs from ``dag['callbacks']``
    (PRD §6.8). Each callback entry ``{notifier_id, params}`` is rendered to a
    notifier instance via that notifier's registry ``template``. Returns the
    ``on_X_callback=[...]`` arg strings (appended to the ``@dag`` / ``DAG(...)``
    call) and the notifier defs used (so their imports get collected)."""
    callbacks = dag.get("callbacks")
    if not isinstance(callbacks, dict):
        return [], []
    args: List[str] = []
    used: List[Dict[str, Any]] = []
    for event in _DAG_CALLBACK_EVENTS:
        entries = callbacks.get(event)
        if not isinstance(entries, list):
            continue
        exprs = _render_callback_entries(entries, notifiers, env, used)
        if exprs:
            args.append(f"{event}_callback=[{', '.join(exprs)}]")
    return args, used


def _node_callbacks(
    node: Dict[str, Any],
    notifiers: Dict[str, Dict[str, Any]],
    env: Environment,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Render per-task ``on_*_callback`` kwargs from ``node['callbacks']`` (PRD
    §6.8). Returns a mapping ``{on_X_callback: <list-literal expr>}`` (merged into
    the node's trailing ``common`` kwargs, so it rides the same ``{{ common }}``
    slot every operator template already has — TaskFlow ``@task`` forwards these
    to the underlying operator) and the notifier defs used (for imports)."""
    callbacks = node.get("callbacks")
    if not isinstance(callbacks, dict):
        return {}, []
    kwargs: Dict[str, Any] = {}
    used: List[Dict[str, Any]] = []
    for event in _TASK_CALLBACK_EVENTS:
        entries = callbacks.get(event)
        if not isinstance(entries, list):
            continue
        exprs = _render_callback_entries(entries, notifiers, env, used)
        if exprs:
            kwargs[f"{event}_callback"] = _Raw(f"[{', '.join(exprs)}]")
    return kwargs, used


# --------------------------------------------------------------------------- #
# Imports + body
# --------------------------------------------------------------------------- #
def _collect_imports(
    ops: List[Dict[str, Any]],
    traditional: bool = False,
    notifiers: Tuple[Dict[str, Any], ...] = (),
) -> List[str]:
    """datetime + airflow.sdk first, then de-duplicated provider imports.

    Traditional uses ``from airflow.sdk import DAG`` and every op's **operator
    class** import (`import:`); TaskFlow uses ``dag, task`` and a native op's
    ``import_taskflow`` (operator-style ops still use `import:`)."""
    if traditional:
        pinned = ["from datetime import datetime, timedelta", "from airflow.sdk import DAG"]
        extra: List[str] = []
        for op in ops:
            line = op.get("import")
            if line and line not in pinned and line not in extra:
                extra.append(line)
        for notifier in notifiers:
            line = notifier.get("import")
            if line and line not in pinned and line not in extra:
                extra.append(line)
        return pinned + sorted(extra)

    pinned = ["from datetime import datetime, timedelta", "from airflow.sdk import dag, task"]
    # `dag`/`task` are already pinned above, so a native operator's
    # `import_taskflow` that only re-imports them is redundant.
    covered = {
        "from airflow.sdk import task",
        "from airflow.sdk import dag",
        "from airflow.sdk import dag, task",
    }
    extra = []
    for op in ops:
        if op.get("taskflow", "native") == "operator":
            line = op.get("import")
        else:
            line = op.get("import_taskflow")
        if line and line not in pinned and line not in covered and line not in extra:
            extra.append(line)
    for notifier in notifiers:
        line = notifier.get("import")
        if line and line not in pinned and line not in extra:
            extra.append(line)
    return pinned + sorted(extra)


def _indent(block: str, spaces: int = 4) -> str:
    pad = " " * spaces
    return "\n".join((pad + line) if line.strip() else "" for line in block.splitlines())


def _has_code_param(op: Dict[str, Any]) -> bool:
    """Whether the op embeds a user-authored code body (a `code`-widget param) —
    its rendered block keeps its blank lines; every other op is a pure kwarg
    list whose template blanks are artifacts to strip."""
    return any(p.get("widget") == "code" for p in op.get("params") or [])


def _tidy(code: str) -> str:
    """Strip trailing whitespace and the blank lines left by empty template
    expansions (e.g. an empty ``{{ common | pyargs }}`` before a closing paren)."""
    lines = [line.rstrip() for line in code.splitlines()]
    out: List[str] = []
    for i, line in enumerate(lines):
        if line == "":
            nxt = next((later for later in lines[i + 1:] if later.strip()), "")
            if nxt.lstrip().startswith(")"):
                continue
            if out and out[-1] == "":
                continue  # collapse consecutive blanks
        out.append(line)
    return "\n".join(out).strip("\n") + "\n"


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #
def _ir_hash(ir: Dict[str, Any]) -> str:
    canonical = json.dumps(ir, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _validate_identifiers(dag_id: str, nodes: List[Dict[str, Any]]) -> List[str]:
    errors: List[str] = []
    if not dag_id or not dag_id.isidentifier() or keyword.iskeyword(dag_id):
        errors.append(f"Invalid dag_id (not a Python identifier): {dag_id!r}")
    seen: Dict[str, int] = {}
    for node in nodes:
        task_id = node.get("task_id", "")
        if not task_id or not task_id.isidentifier() or keyword.iskeyword(task_id):
            errors.append(f"Invalid task_id (not a Python identifier): {task_id!r}")
        seen[task_id] = seen.get(task_id, 0) + 1
    for task_id, count in seen.items():
        if count > 1:
            errors.append(f"Duplicate task_id: {task_id!r}")
    return errors


def generate_dag(ir: Dict[str, Any]) -> Dict[str, Any]:
    """Render ``ir`` to Airflow 3.x TaskFlow Python.

    Returns ``{code, valid, errors}``. ``errors`` is non-empty (and ``valid`` is
    False) on any failure in the Appendix E pipeline; ``code`` is best-effort.
    """
    try:
        code = _render(ir)
    except CodegenError as err:
        return {"code": "", "valid": False, "errors": [str(err)]}

    errors: List[str] = []
    try:
        ast.parse(code)
        compile(code, "<airflow-studio>", "exec")  # scoping check; does NOT run
    except SyntaxError as err:
        errors.append(f"Generated code is invalid (line {err.lineno}): {err.msg}")

    return {"code": code, "valid": not errors, "errors": errors}


def format_code(code: str) -> str:
    """Best-effort deterministic formatting (PRD §8.4): run black if it is
    importable, otherwise return the code unchanged. Idempotent identical IR ->
    byte-identical output once a formatter is present."""
    try:
        import black

        return black.format_str(code, mode=black.Mode())
    except Exception:  # noqa: BLE001 - black is optional in the Jupyter env
        return code


def _render(ir: Dict[str, Any]) -> str:
    dag = ir.get("dag") or {}
    nodes = ir.get("nodes") or []
    edges = ir.get("edges") or []
    dag_id = dag.get("dag_id", "")
    syntax = ir.get("syntax_style")
    if syntax not in ("taskflow", "traditional"):
        syntax = "taskflow"
    traditional = syntax == "traditional"

    # Stage 1–3: structural + identifier validation (no code executed).
    id_errors = _validate_identifiers(dag_id, nodes)
    if id_errors:
        raise CodegenError("; ".join(id_errors))

    registry = {op["id"]: op for op in load_registry()}
    for node in nodes:
        if node.get("op") not in registry:
            raise CodegenError(f"Unknown operator: {node.get('op')!r}")

    ordered = _topo_order(nodes, edges)
    if ordered is None:
        raise CodegenError("The graph has a cycle; Airflow rejects cyclic DAGs.")

    env = _make_env()
    notifier_registry = {n["id"]: n for n in load_notifiers()}
    callback_args, used_notifiers = _build_callbacks(dag, notifier_registry, env)
    used_notifiers = list(used_notifiers)  # also grows with per-task notifiers
    used_ops: List[Dict[str, Any]] = []
    definitions: List[str] = []
    instantiations: List[str] = []
    handle: Dict[str, str] = {}
    template_key = "template_traditional" if traditional else "template_taskflow"
    family = "Traditional" if traditional else "TaskFlow"

    for node in ordered:
        op = registry[node["op"]]
        used_ops.append(op)
        template = op.get(template_key)
        if not template:
            raise CodegenError(f"Operator {op['id']!r} has no {family} template")
        # Per-task callbacks (PRD §6.8) ride the operator's trailing-kwargs slot:
        # merge the rendered `on_*_callback=[...]` exprs into `common` (after the
        # declared common params) so every template's `{{ common | pyargs }}`
        # emits them — into the `@task(...)` decorator for native ops, into the
        # operator call otherwise. No per-operator template edits needed.
        node_cb_kwargs, node_cb_notifiers = _node_callbacks(
            node, notifier_registry, env
        )
        used_notifiers.extend(node_cb_notifiers)
        rendered = env.from_string(template).render(
            task_id=node["task_id"],
            params=node.get("params") or {},
            common={**_node_common(node, op), **node_cb_kwargs},
        )
        is_operator = op.get("taskflow", "native") == "operator"
        if not _has_code_param(op):
            # A block with no user-code body is only kwarg lines, so an omitted
            # optional `{% if %}` kwarg or an empty `{{ common | pyargs }}` leaves
            # a blank line that is pure artifact — drop it. (A code node's body is
            # left untouched; in Traditional even native ops like bash render as a
            # pure `Cls(...)`, so key on the code param, not the taskflow kind.)
            rendered = "\n".join(ln for ln in rendered.splitlines() if ln.strip())
        definitions.append(_indent(rendered, 4))

        # Traditional: every op renders a `task_id = Cls(...)` assignment, so the
        # task_id IS the handle. TaskFlow: an operator's assignment is the
        # instance, but a native `@task` needs a `task_id_task = task_id()` call.
        if traditional or is_operator:
            handle[node["id"]] = node["task_id"]
        else:
            inst = f"{node['task_id']}_task"
            handle[node["id"]] = inst
            instantiations.append(f"    {inst} = {node['task_id']}()")

    index = {node["id"]: pos for pos, node in enumerate(ordered)}
    wiring = [
        f"    {handle[e['source']]} >> {handle[e['target']]}"
        for e in sorted(
            (e for e in edges if e.get("source") in handle and e.get("target") in handle),
            key=lambda e: (index[e["source"]], index[e["target"]]),
        )
    ]

    # afdag_id (the stable `.afdag` identity) travels in the header so the
    # manager can re-associate a deployed DAG with its source across a dag_id
    # rename (PRD §6.1.8(B) / §8.9). Whitespace-stripped to stay one token.
    afdag_id = "".join(str((ir.get("provenance") or {}).get("afdag_id", "")).split())
    header = (
        f"# airflow-studio: managed  studio={STUDIO_VERSION}  "
        f"{_ir_hash(ir)}  dag_id={dag_id}  afdag_id={afdag_id}  syntax={syntax}"
    )
    imports = "\n".join(
        _collect_imports(used_ops, traditional, tuple(used_notifiers))
    )

    body_sections = ["\n\n".join(definitions)] if definitions else ["    pass"]
    if instantiations:
        body_sections.append("\n".join(instantiations))
    if wiring:
        body_sections.append("\n".join(wiring))
    body = "\n\n".join(section for section in body_sections if section)

    if traditional:
        # `with DAG(...) as dag:` owns the task graph — no decorator or call.
        code = f"{header}\n{imports}\n\n\n{_dag_context(dag, callback_args)}\n{body}\n"
    else:
        code = (
            f"{header}\n{imports}\n\n\n"
            f"{_dag_decorator(dag, callback_args)}\n"
            f"def {dag_id}():\n{body}\n\n\n"
            f"{dag_id}()\n"
        )
    return _tidy(code)
