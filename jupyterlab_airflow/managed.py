"""Ownership marking for Airflow objects a flow creates (PRD §6.10 / §6.11).

Studio can create **variables** and **connections** in Airflow on behalf of a
flow, and must delete exactly those again when the flow is undeployed. The
teardown paths (``purge_dag``, the orphan sweep, the manager's Delete) only ever
receive a ``dag_id`` — never the IR — so ownership has to be discoverable from
Airflow itself. Both object types happen to carry a free-text ``description``,
so that is where the marker goes:

    [airflow-studio dag_id=my_flow] the author's own description

This is deliberately shared rather than duplicated per object type: ``sync``
writes the marker and ``purge`` reads it back, so the two must agree byte for
byte, and a drift between two copies would silently orphan (or worse, delete)
the wrong objects. It also mirrors the ``# airflow-studio: managed`` provenance
header ``deploy.py`` stamps into generated ``.py`` files (§8.9).

The marker doubles as documentation in the Airflow UI: an operator looking at a
variable or connection can see which flow owns it.
"""

from __future__ import annotations

import re
from typing import Optional

_MARKER_RE = re.compile(r"^\[airflow-studio dag_id=([^\]]*)\]\s?")


def owner_marker(dag_id: str) -> str:
    """The description prefix stamped onto every object a flow creates."""
    return f"[airflow-studio dag_id={dag_id}]"


def compose_description(dag_id: str, description: str = "") -> str:
    """The Airflow-side description: ownership marker + the author's own text."""
    text = (description or "").strip()
    return f"{owner_marker(dag_id)} {text}".strip()


def owner_of(description: Optional[str]) -> Optional[str]:
    """The ``dag_id`` that owns an object, read back from its description, or
    ``None`` when it is not Studio-managed (created by hand, by an operator, or
    by a secrets backend)."""
    match = _MARKER_RE.match(description or "")
    return match.group(1) if match else None


def strip_marker(description: Optional[str]) -> str:
    """The author's description with the ownership marker removed."""
    return _MARKER_RE.sub("", description or "").strip()


def is_owned_by(description: Optional[str], dag_id: str) -> bool:
    """Whether ``dag_id`` may modify/delete the object carrying ``description``.

    This is the single guard behind "a flow may use, but never change, an object
    it did not create" — so it must stay strict: an unmarked description (and a
    marker for a different flow) is never owned.
    """
    return owner_of(description) == dag_id
