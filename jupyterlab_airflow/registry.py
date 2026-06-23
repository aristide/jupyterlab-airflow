"""Operator registry — load the bundled (and optional user) operator YAML files.

The registry is the single source of truth (PRD §6.2) for the operator palette,
the NODE-tab form schema, and — in a later milestone — server-side Jinja2
codegen. It is *plain data*: adding an operator is a new YAML file, no React or
Python change (PRD goal G6).

Files are read from:
  - the bundled directory ``jupyterlab_airflow/operators/``
  - an optional user/server directory named by the ``AIRFLOW_OPERATORS_DIR``
    environment variable; its entries override bundled ones with the same ``id``.

Results are cached and transparently reloaded when any file's mtime changes, so
dropping in a new YAML file does not require a server restart (PRD §8.5).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

BUNDLED_DIR = Path(__file__).parent / "operators"

# The param fields a client needs for the palette + node form (incl. `help`, the
# inline contextual help / INFO-tab text). Import lines and code templates stay
# server-side (they only matter to codegen).
_CLIENT_PARAM_FIELDS = ("name", "label", "type", "default", "widget", "required", "help")

# Operator-level documentation + gating fields shipped to the client for the
# INFO tab and provider/third-party gating. Data-only (never executed); mapped to
# camelCase TS keys. Codegen-only fields (imports, code templates) are withheld.
# ``third_party`` flags an off-constraints package (PRD §6.2.2 ¹ / §13 Q13) and
# ``version`` is its own pin (the install hint pins it; the provider gate won't
# hard-block such ops).
_CLIENT_DOC_FIELDS = (
    ("description", "description"),
    ("docs_url", "docsUrl"),
    ("example", "example"),
    ("provider", "provider"),
    ("airflow_min_version", "airflowMinVersion"),
    ("third_party", "thirdParty"),
    ("version", "version"),
)

# Cache: signature (paths + mtimes) -> parsed operator list. A change to any
# file's mtime invalidates it, giving hot-reload without a restart.
_cache: Dict[str, Any] = {"signature": None, "operators": None}


class RegistryError(Exception):
    """Raised when an operator YAML file is missing or malformed."""


def _dirs() -> List[Path]:
    dirs = [BUNDLED_DIR]
    user = os.environ.get("AIRFLOW_OPERATORS_DIR")
    if user:
        dirs.append(Path(user))
    return dirs


def _yaml_files() -> List[Path]:
    """Bundled files first, then user files (so user entries win on id)."""
    files: List[Path] = []
    for directory in _dirs():
        if directory.is_dir():
            files.extend(sorted(directory.glob("*.yaml")))
            files.extend(sorted(directory.glob("*.yml")))
    return files


def _signature(files: List[Path]) -> Tuple:
    return tuple((str(f), f.stat().st_mtime_ns) for f in files)


def _parse_files(files: List[Path]) -> List[Dict[str, Any]]:
    """Parse YAML definition files into a list sorted by (category, label). Later
    files (and the user directory) override earlier ones sharing an ``id``."""
    by_id: Dict[str, Dict[str, Any]] = {}
    for path in files:
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as err:
            raise RegistryError(f"{path.name}: invalid YAML: {err}") from err
        if not isinstance(raw, dict):
            raise RegistryError(f"{path.name}: expected a YAML mapping at the top level")
        item_id = raw.get("id")
        if not item_id:
            raise RegistryError(f"{path.name}: missing required field 'id'")
        raw.setdefault("params", [])
        if not isinstance(raw["params"], list):
            raise RegistryError(f"{path.name}: 'params' must be a list")
        by_id[item_id] = raw
    return sorted(
        by_id.values(),
        key=lambda d: (str(d.get("category", "")), str(d.get("label", d["id"]))),
    )


def load_registry(force: bool = False) -> List[Dict[str, Any]]:
    """Return every operator definition, sorted by (category, label).

    Later files (and the user directory) override earlier ones sharing an ``id``.
    Cached between calls; reloaded automatically when a file changes on disk.
    """
    files = _yaml_files()
    signature = _signature(files)
    if (
        not force
        and _cache["operators"] is not None
        and _cache["signature"] == signature
    ):
        return _cache["operators"]
    operators = _parse_files(files)
    _cache["signature"] = signature
    _cache["operators"] = operators
    return operators


def _client_param(param: Dict[str, Any]) -> Dict[str, Any]:
    out = {key: param[key] for key in _CLIENT_PARAM_FIELDS if key in param}
    out.setdefault("required", False)
    return out


def client_view() -> List[Dict[str, Any]]:
    """The registry shaped for the frontend palette + node form + INFO tab.

    Returns only what the browser needs; codegen-only fields (imports, code
    templates) stay on the server. Operator docs fields (``description``,
    ``docs_url``, ``example``, ``provider``, ``airflow_min_version``) and per-param
    ``help`` are shipped for the INFO tab and inline field help — data-only, never
    executed. Keys are camelCased to match the TypeScript ``IOperatorDef``.
    """
    view: List[Dict[str, Any]] = []
    for op in load_registry():
        entry: Dict[str, Any] = {
            "id": op["id"],
            "label": op.get("label", op["id"]),
            "category": op.get("category", "Other"),
            "taskIdPrefix": op.get("task_id_prefix", op["id"]),
            "taskflow": op.get("taskflow", "native"),
            "handles": op.get("handles", {"in": True, "out": True}),
            "params": [_client_param(p) for p in op.get("params", [])],
            # The per-task common settings this op supports (PRD §6.1.3); the
            # client renders them as the NODE-tab "Common settings" section.
            "commonParams": list(op.get("common_params", [])),
        }
        for src, dst in _CLIENT_DOC_FIELDS:
            if op.get(src) is not None:
                entry[dst] = op[src]
        view.append(entry)
    return view


# --------------------------------------------------------------------------- #
# Notifier registry (PRD §6.8) — the callbacks counterpart to operators.
# --------------------------------------------------------------------------- #
NOTIFIERS_DIR = Path(__file__).parent / "notifiers"
_notifier_cache: Dict[str, Any] = {"signature": None, "notifiers": None}


def _notifier_files() -> List[Path]:
    dirs = [NOTIFIERS_DIR]
    user = os.environ.get("AIRFLOW_NOTIFIERS_DIR")
    if user:
        dirs.append(Path(user))
    files: List[Path] = []
    for directory in dirs:
        if directory.is_dir():
            files.extend(sorted(directory.glob("*.yaml")))
            files.extend(sorted(directory.glob("*.yml")))
    return files


def load_notifiers(force: bool = False) -> List[Dict[str, Any]]:
    """Return every notifier definition (PRD §6.8), cached/hot-reloaded like the
    operator registry. A notifier is *data*: ``id``, ``label``, ``provider``,
    ``import``, a Jinja ``template`` that renders the notifier *instance*, and
    ``params`` + ``help``. Bundled at ``jupyterlab_airflow/notifiers/`` plus an
    optional ``AIRFLOW_NOTIFIERS_DIR`` override."""
    files = _notifier_files()
    signature = _signature(files)
    if (
        not force
        and _notifier_cache["notifiers"] is not None
        and _notifier_cache["signature"] == signature
    ):
        return _notifier_cache["notifiers"]
    notifiers = _parse_files(files)
    _notifier_cache["signature"] = signature
    _notifier_cache["notifiers"] = notifiers
    return notifiers


def notifier_client_view() -> List[Dict[str, Any]]:
    """The notifier registry shaped for the frontend Notifications tab — label +
    params (with ``help``) + doc fields. The ``import``/``template`` stay
    server-side (codegen-only), exactly like the operator ``client_view``."""
    view: List[Dict[str, Any]] = []
    for notifier in load_notifiers():
        entry: Dict[str, Any] = {
            "id": notifier["id"],
            "label": notifier.get("label", notifier["id"]),
            "params": [_client_param(p) for p in notifier.get("params", [])],
        }
        for src, dst in _CLIENT_DOC_FIELDS:
            if notifier.get(src) is not None:
                entry[dst] = notifier[src]
        view.append(entry)
    return view
