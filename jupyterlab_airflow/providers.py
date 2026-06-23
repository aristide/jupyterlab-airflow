"""Provider-availability gating (PRD §6.2.1).

The operator palette is gated on what is installed in the **target Airflow**
(never the Jupyter/server env, which is false-green, R2). This module:

  - reads the target's installed providers (``GET /api/v2/providers``) and its
    Airflow version (``GET /api/v2/version``) via the shared ``AirflowClient``,
    cached with a short TTL plus a manual ``force`` refresh (installing a
    provider changes availability without a Studio restart);
  - annotates each ``client_view()`` palette entry ``available |
    missing-provider | version-too-old | unknown`` from
    (target-providers × ``provider`` × ``airflow_min_version``);
  - hard-fails a deploy whose IR uses an unavailable provider **before** the
    file is written (a fast pre-filter; ``/importErrors`` stays authoritative).

Everything degrades gracefully: if the target can't be reached the index is
``None`` → every op is ``unknown`` (shown, non-blocking) and the deploy gate is a
no-op, exactly as before this feature.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

STANDARD_PROVIDER = "apache-airflow-providers-standard"

# How long a fetched target index is reused before a refresh. Short so that
# `pip install`-ing a provider in the target shows up quickly in the palette.
_TTL_SECONDS = 60.0

# Cache of the target index: {"providers": {pkg: version}, "airflow_version": str}
# (or None when the target was unreachable). Keyed by a monotonic timestamp.
_cache: Dict[str, Any] = {"at": None, "index": None}


def reset_cache() -> None:
    """Drop the cached target index (used by tests / a forced refresh)."""
    _cache["at"] = None
    _cache["index"] = None


def _fetch_index() -> Optional[Dict[str, Any]]:
    """One live read of the target's providers + Airflow version. Returns the
    index dict, or ``None`` if the providers list can't be fetched (no gating)."""
    from .client import AirflowError, get_client

    client = get_client()
    try:
        raw = client.list_providers()
    except AirflowError:
        return None  # target unreachable -> gating is a no-op (non-blocking)

    providers: Dict[str, Any] = {}
    for entry in raw.get("providers") or []:
        name = entry.get("package_name")
        if name:
            providers[name] = entry.get("version")

    airflow_version: Optional[str] = None
    try:
        airflow_version = (client.version() or {}).get("version")
    except AirflowError:
        # Providers are known; only the version-too-old check is then skipped.
        airflow_version = None

    return {"providers": providers, "airflow_version": airflow_version}


def get_target_index(force: bool = False) -> Optional[Dict[str, Any]]:
    """The cached target index, refreshing when stale (or ``force``). ``None``
    when the target Airflow is unreachable."""
    now = time.monotonic()
    fresh = (
        _cache["at"] is not None and (now - _cache["at"]) < _TTL_SECONDS
    )
    if not force and fresh:
        return _cache["index"]
    _cache["index"] = _fetch_index()
    _cache["at"] = now
    return _cache["index"]


# --------------------------------------------------------------------------- #
# Pure availability logic
# --------------------------------------------------------------------------- #
def is_standard(provider: Optional[str]) -> bool:
    """Whether ``provider`` is the always-available standard provider (a core
    Airflow-3 dep, present even in the slim image) or unspecified/bundled — such
    ops are never gated (PRD §6.2.1)."""
    if not provider:
        return True
    value = str(provider).strip().lower()
    return value in ("", "bundled", "(bundled)", STANDARD_PROVIDER)


def _parse_version(value: Any) -> tuple:
    """Lenient dotted-version parse to an int tuple (``'3.0.2'`` -> ``(3, 0, 2)``);
    non-numeric trailing junk on a segment is ignored (``'1rc1'`` -> ``1``)."""
    parts: List[int] = []
    for chunk in str(value).split(".")[:4]:
        digits = ""
        for char in chunk.strip():
            if char.isdigit():
                digits += char
            else:
                break
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


def _version_lt(actual: Any, minimum: Any) -> bool:
    """``actual < minimum`` by dotted-version order, length-padded with zeros."""
    left, right = _parse_version(actual), _parse_version(minimum)
    width = max(len(left), len(right))
    left += (0,) * (width - len(left))
    right += (0,) * (width - len(right))
    return left < right


def availability(
    provider: Optional[str],
    airflow_min_version: Any,
    index: Optional[Dict[str, Any]],
    third_party: bool = False,
) -> str:
    """``available`` | ``missing-provider`` | ``version-too-old`` | ``unknown`` |
    ``third-party``.

    A ``third_party`` op (off the Airflow constraints file — Great Expectations,
    OpenMetadata; PRD §6.2.2 ¹ / §13 Q13) is **always** ``third-party``: it is
    shown with a pinned-install note but never blocked, because
    ``/api/v2/providers`` is not an authoritative install signal for such packages
    in general (some don't register as providers at all, and it can never confirm
    OpenMetadata's *server*-version match) — ``/importErrors`` is the verdict.

    Otherwise: ``unknown`` when the target couldn't be read (``index is None``) —
    the op is still shown and never blocked. The version check (target Airflow vs
    the op's ``airflow_min_version``) takes precedence, then the provider check;
    the standard provider is always available.
    """
    if third_party:
        return "third-party"
    if index is None:
        return "unknown"
    target_version = index.get("airflow_version")
    if (
        airflow_min_version
        and target_version
        and _version_lt(target_version, airflow_min_version)
    ):
        return "version-too-old"
    if is_standard(provider):
        return "available"
    if provider not in (index.get("providers") or {}):
        return "missing-provider"
    return "available"


def pip_install_hint(provider: str, version: Any = None) -> str:
    """``pip install <provider>``, version-pinned when given. Third-party
    (off-constraints) ops pin their own version (PRD §6.2.2 ¹)."""
    if version:
        return f"pip install {provider}=={version}"
    return f"pip install {provider}"


def annotate_view(
    entries: List[Dict[str, Any]], index: Optional[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Add ``availability`` (and a ``pipInstall`` hint for a missing provider or a
    third-party op) to each ``client_view()`` palette entry. Mutates and returns
    ``entries``."""
    for entry in entries:
        provider = entry.get("provider", "")
        third_party = bool(entry.get("thirdParty"))
        avail = availability(
            provider, entry.get("airflowMinVersion"), index, third_party=third_party
        )
        entry["availability"] = avail
        if avail == "missing-provider" and not is_standard(provider):
            entry["pipInstall"] = pip_install_hint(provider)
        elif avail == "third-party" and provider:
            entry["pipInstall"] = pip_install_hint(provider, entry.get("version"))
    return entries


def annotated_operators(force: bool = False) -> List[Dict[str, Any]]:
    """The palette payload (``client_view()``) annotated with target
    availability — what ``GET operators`` serves."""
    from .registry import client_view

    return annotate_view(client_view(), get_target_index(force=force))


def annotated_notifiers(force: bool = False) -> List[Dict[str, Any]]:
    """The notifier payload (``notifier_client_view()``) annotated with target
    provider-availability (PRD §6.8) — what ``GET notifiers`` serves."""
    from .registry import notifier_client_view

    return annotate_view(notifier_client_view(), get_target_index(force=force))


def provider_block_errors(
    ir: Dict[str, Any], index: Optional[Dict[str, Any]]
) -> List[str]:
    """Plain-language errors for any operator in ``ir`` whose provider is missing
    or too old in the target (PRD §6.2.1 hard-gate). Empty when the target is
    unreachable (``index is None``) — the deploy then proceeds and ``/importErrors``
    is the authoritative verdict."""
    if index is None:
        return []
    from .registry import load_notifiers, load_registry

    errors: List[str] = []
    seen: set = set()

    def _check(kind: str, key: str, defn: Dict[str, Any]) -> None:
        if key in seen:
            return
        if defn.get("third_party"):
            # Off-constraints (§13 Q13): never hard-block — the provider list is
            # not an authoritative install signal for these. /importErrors (with
            # the §7 friendly recovery) is the deploy-time verdict.
            return
        provider = defn.get("provider", "")
        avail = availability(provider, defn.get("airflow_min_version"), index)
        if avail not in ("missing-provider", "version-too-old"):
            return
        seen.add(key)
        label = defn.get("label", defn["id"])
        if avail == "missing-provider":
            errors.append(
                f"{kind} '{label}' needs the provider '{provider}', which is "
                f"not installed in your target Airflow. Install it "
                f"({pip_install_hint(provider)}), refresh, then deploy."
            )
        else:
            errors.append(
                f"{kind} '{label}' needs Airflow >= {defn.get('airflow_min_version')}, "
                f"but your target Airflow is {index.get('airflow_version')}."
            )

    registry = {op["id"]: op for op in load_registry()}
    for node in ir.get("nodes") or []:
        op = registry.get(node.get("op"))
        if op is not None:
            _check("Operator", op["id"], op)

    # Notifier callbacks (PRD §6.8) gate on their provider too — otherwise a DAG
    # with a Slack/SMTP notifier on an uninstalled provider would write and then
    # fail at import instead of being blocked pre-write. Both the DAG-level
    # (`dag.callbacks`) and per-task (`node.callbacks`) surfaces are scanned.
    notifiers = {n["id"]: n for n in load_notifiers()}

    def _scan_callbacks(callbacks: Any) -> None:
        if not isinstance(callbacks, dict):
            return
        for entries in callbacks.values():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                notifier = notifiers.get(entry.get("notifier_id"))
                if notifier is not None:
                    _check("Notifier", "notifier:" + notifier["id"], notifier)

    _scan_callbacks((ir.get("dag") or {}).get("callbacks"))
    for node in ir.get("nodes") or []:
        _scan_callbacks(node.get("callbacks"))
    return errors
