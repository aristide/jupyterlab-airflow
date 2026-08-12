"""Validation for the Kubernetes pod fields that take raw K8s JSON (PRD §6.12).

`KubernetesPodOperator` wants **`kubernetes.client` model objects** for
`volumes`/`volume_mounts` — a plain dict raises at DAG-parse time — and accepts
`init_containers`/`container_resources` as dicts but performs *no validation on
them whatsoever*. Rather than make the user construct Python objects, Studio
takes standard Kubernetes JSON (exactly what you'd paste from a pod manifest)
and lets codegen funnel it through Airflow's public
``PodGenerator.deserialize_model_dict`` at DAG-parse time.

That deserializer has one sharp edge worth guarding, verified against the live
provider: it reads **camelCase only**. A snake_case or misspelled *optional* key
is dropped **in silence** — ``{"name": "scratch", "empty_dir": {}}`` yields a
volume with no source at all, and nothing anywhere reports it. The failure then
surfaces as a puzzling pod-admission error, far from the field the user typed
it into. Required fields do raise, but only those.

So this module lints the JSON *before* it is emitted: shape, the handful of
genuinely required fields, and any key that looks like snake_case. It is
deliberately schema-free — the Jupyter server has no ``kubernetes`` package to
introspect (only the Airflow side does), so this checks structure and key style
rather than pretending to know every field of every K8s type.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple

# The pod fields Studio renders from raw K8s JSON, and the shape each expects.
# `container` fields ride on the pod's single "base" container; `spec` fields sit
# on the pod spec itself.
LIST_FIELDS = ("volumes", "volume_mounts", "init_containers")
OBJECT_FIELDS = ("container_resources",)
K8S_PARAMS = LIST_FIELDS + OBJECT_FIELDS

# The camelCase key each param maps to inside a pod manifest.
MANIFEST_KEY = {
    "volumes": "volumes",
    "volume_mounts": "volumeMounts",
    "init_containers": "initContainers",
    "container_resources": "resources",
}

# Fields Kubernetes genuinely requires; anything else is optional and would be
# dropped silently rather than raise, which is what the key-style lint is for.
REQUIRED_KEYS = {
    "volumes": ("name",),
    "volume_mounts": ("name", "mountPath"),
    "init_containers": ("name", "image"),
}

_SNAKE_RE = re.compile(r"[a-z0-9]_[a-z0-9]")

# Maps whose *keys* are user-defined data, not Kubernetes field names — resource
# names (`cpu`, `nvidia.com/gpu`, `hugepages-2Mi`), label/annotation keys, node
# selectors. Underscores are legal there, so the camelCase lint must not
# descend into them or it would reject valid manifests.
_FREEFORM_MAPS = frozenset(
    {
        "limits",
        "requests",
        "labels",
        "annotations",
        "nodeSelector",
        "matchLabels",
        "capacity",
        "selector",
        "data",
        "stringData",
    }
)


def _camel(key: str) -> str:
    """`empty_dir` -> `emptyDir`, for the "did you mean" hint."""
    head, *rest = key.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest if part)


def _suspect_keys(value: Any, *, inside_freeform: bool = False) -> Iterable[str]:
    """Every snake_case-looking key nested in ``value``, skipping the free-form
    maps whose keys are user data rather than K8s field names."""
    if isinstance(value, dict):
        for key, nested in value.items():
            if not inside_freeform and isinstance(key, str) and _SNAKE_RE.search(key):
                yield key
            yield from _suspect_keys(
                nested, inside_freeform=key in _FREEFORM_MAPS
            )
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _suspect_keys(nested, inside_freeform=inside_freeform)


def _label(node: Dict[str, Any]) -> str:
    return str(node.get("task_id") or node.get("id") or "task")


def _field_errors(where: str, field: str, value: Any) -> List[str]:
    """Validate one pod field's JSON. ``where`` names the task, for the message."""
    errors: List[str] = []
    human = field.replace("_", " ")

    if field in LIST_FIELDS:
        if not isinstance(value, list):
            return [
                f"{where}: {human} must be a JSON list, got "
                f"{type(value).__name__}. Example: "
                f'[{{"name": "scratch", "emptyDir": {{}}}}]'
            ]
        for index, item in enumerate(value):
            if not isinstance(item, dict):
                errors.append(
                    f"{where}: {human}[{index}] must be a JSON object, got "
                    f"{type(item).__name__}."
                )
                continue
            for required in REQUIRED_KEYS.get(field, ()):
                if not item.get(required):
                    errors.append(
                        f"{where}: {human}[{index}] is missing the required "
                        f"'{required}' field."
                    )
    elif not isinstance(value, dict):
        return [
            f"{where}: {human} must be a JSON object, got "
            f"{type(value).__name__}. Example: "
            '{"requests": {"cpu": "500m"}, "limits": {"cpu": "2"}}'
        ]

    # The silent-drop guard: Kubernetes JSON is camelCase, and the deserializer
    # discards anything else without a word.
    for key in sorted(set(_suspect_keys(value))):
        errors.append(
            f"{where}: {human} uses '{key}', but Kubernetes JSON is camelCase — "
            f"did you mean '{_camel(key)}'? A key in the wrong style is ignored "
            "without any error."
        )
    return errors


def node_fields(node: Dict[str, Any]) -> Dict[str, Any]:
    """The pod fields this node actually sets (non-empty), keyed by param name."""
    params = node.get("params")
    if not isinstance(params, dict):
        return {}
    out: Dict[str, Any] = {}
    for field in K8S_PARAMS:
        value = params.get(field)
        if value in (None, "", [], {}):
            continue
        out[field] = value
    return out


def uses_pod_spec(ir: Dict[str, Any]) -> bool:
    """Whether any node needs the ``PodGenerator`` import — i.e. supplies raw
    K8s JSON for one of these fields. A pod task that sets none of them stays
    byte-identical to before (no stray import)."""
    nodes = ir.get("nodes")
    if not isinstance(nodes, list):
        return False
    return any(
        isinstance(node, dict) and node_fields(node) for node in nodes
    )


def validation_errors(ir: Dict[str, Any]) -> List[str]:
    """Errors across every node's pod-field JSON — pure and offline, so it runs
    in codegen's stage-1 alongside the other structural checks."""
    nodes = ir.get("nodes")
    if not isinstance(nodes, list):
        return []
    errors: List[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        fields = node_fields(node)
        if not fields:
            continue
        where = f"Task '{_label(node)}'"
        for field, value in fields.items():
            errors.extend(_field_errors(where, field, value))
    return errors


def manifest_for(node: Dict[str, Any]) -> Dict[str, Any]:
    """The pod manifest fragment codegen deserializes for this node.

    One dict carrying every pod field the node sets, so the generated DAG makes
    a *single* ``deserialize_model_dict`` call per task and reads the typed
    objects back off it. The single container is named ``base`` because that is
    the name ``KubernetesPodOperator`` gives its own container.
    """
    fields = node_fields(node)
    container: Dict[str, Any] = {"name": "base"}
    spec: Dict[str, Any] = {"containers": [container]}
    for field, value in fields.items():
        key = MANIFEST_KEY[field]
        if field in ("volume_mounts", "container_resources"):
            container[key] = value
        else:
            spec[key] = value
    return {"spec": spec}


def accessors(node: Dict[str, Any]) -> List[Tuple[str, str]]:
    """``(operator_kwarg, attribute path on the deserialized spec)`` for each
    field the node sets — what the template renders as
    ``volumes=_pod_<task>.volumes``."""
    paths = {
        "volumes": "volumes",
        "init_containers": "init_containers",
        "volume_mounts": "containers[0].volume_mounts",
        "container_resources": "containers[0].resources",
    }
    return [(field, paths[field]) for field in K8S_PARAMS if field in node_fields(node)]
