"""Kubernetes pod fields taking raw K8s JSON: validation + codegen (PRD §6.12)."""

import pytest

from jupyterlab_airflow import podspec
from jupyterlab_airflow.codegen import generate_dag

FULL_PARAMS = {
    "image": "python:3.12-slim",
    "volumes": [
        {"name": "scratch", "emptyDir": {}},
        {"name": "data", "persistentVolumeClaim": {"claimName": "my-pvc"}},
    ],
    "volume_mounts": [
        {"name": "scratch", "mountPath": "/scratch"},
        {"name": "data", "mountPath": "/data", "readOnly": True},
    ],
    "init_containers": [
        {"name": "fetch", "image": "busybox", "command": ["sh", "-c", "echo prep"]}
    ],
    "container_resources": {
        "requests": {"cpu": "500m", "memory": "512Mi"},
        "limits": {"cpu": "2", "memory": "2Gi"},
    },
}


def _node(params, task_id="train"):
    return {
        "id": "n1", "op": "kubernetes_pod", "task_id": task_id,
        "params": params, "position": {"x": 0, "y": 0},
    }


def _ir(params, syntax="taskflow"):
    return {
        "schema_version": "1.0",
        "provenance": {"generator": "airflow-studio", "studio_version": "0.1.0",
                       "afdag_id": "k8s"},
        "syntax_style": syntax,
        "dag": {"dag_id": "k8s_demo", "schedule": "@once",
                "start_date": "2026-01-01", "catchup": False, "retries": 0,
                "retry_delay_seconds": 300, "tags": [], "owner": "",
                "params": {}, "default_args": {}},
        "nodes": [_node(params)], "edges": [],
    }


# -- field detection -------------------------------------------------------


def test_node_fields_ignores_blank_and_absent():
    assert podspec.node_fields(_node({"image": "x"})) == {}
    assert podspec.node_fields(_node({"image": "x", "volumes": []})) == {}
    assert podspec.node_fields(_node({"image": "x", "container_resources": {}})) == {}
    assert set(podspec.node_fields(_node(FULL_PARAMS))) == set(podspec.K8S_PARAMS)


def test_uses_pod_spec_only_when_a_field_is_set():
    assert podspec.uses_pod_spec(_ir({"image": "x"})) is False
    assert podspec.uses_pod_spec(_ir(FULL_PARAMS)) is True


# -- manifest shape --------------------------------------------------------


def test_manifest_puts_container_fields_on_the_base_container():
    """`volume_mounts`/`resources` are container-level; `volumes`/`initContainers`
    are spec-level. The container is named `base` to match the one
    KubernetesPodOperator creates."""
    manifest = podspec.manifest_for(_node(FULL_PARAMS))
    spec = manifest["spec"]
    container = spec["containers"][0]
    assert container["name"] == "base"
    assert "volumeMounts" in container and "resources" in container
    assert "volumes" in spec and "initContainers" in spec
    # camelCase manifest keys, whatever the param names are.
    assert "volume_mounts" not in container and "init_containers" not in spec


def test_accessors_map_each_field_to_its_attribute_path():
    assert dict(podspec.accessors(_node(FULL_PARAMS))) == {
        "volumes": "volumes",
        "volume_mounts": "containers[0].volume_mounts",
        "init_containers": "init_containers",
        "container_resources": "containers[0].resources",
    }


# -- the camelCase guard ---------------------------------------------------


@pytest.mark.parametrize(
    "params,bad,suggestion",
    [
        ({"volumes": [{"name": "s", "empty_dir": {}}]}, "empty_dir", "emptyDir"),
        (
            {"volume_mounts": [{"name": "s", "mount_path": "/s", "mountPath": "/s"}]},
            "mount_path",
            "mountPath",
        ),
        (
            {"init_containers": [{"name": "i", "image": "b",
                                  "image_pull_policy": "Always"}]},
            "image_pull_policy",
            "imagePullPolicy",
        ),
        (
            {"volumes": [{"name": "d", "persistent_volume_claim": {"claimName": "p"}}]},
            "persistent_volume_claim",
            "persistentVolumeClaim",
        ),
    ],
)
def test_snake_case_keys_are_rejected_with_a_suggestion(params, bad, suggestion):
    """Airflow's deserializer reads camelCase only and drops anything else in
    SILENCE, so a snake_case key would vanish and surface much later as a
    puzzling pod-admission failure. Catch it at the field."""
    errors = podspec.validation_errors(_ir({"image": "x", **params}))
    assert errors, f"{bad} should have been flagged"
    assert bad in errors[0] and suggestion in errors[0]


def test_nested_snake_case_is_caught():
    params = {"image": "x", "init_containers": [
        {"name": "i", "image": "b",
         "volumeMounts": [{"name": "s", "mount_path": "/s"}]}]}
    assert "mountPath" in podspec.validation_errors(_ir(params))[0]


@pytest.mark.parametrize(
    "params",
    [
        # Resource names are user data, not K8s field names — underscores and
        # dots are legal and must not be flagged.
        {"container_resources": {"limits": {"nvidia.com/gpu": "1",
                                            "hugepages-2Mi": "100Mi"}}},
        {"container_resources": {"requests": {"my_custom_resource": "1"}}},
    ],
)
def test_free_form_map_keys_are_not_linted(params):
    assert podspec.validation_errors(_ir({"image": "x", **params})) == []


def test_valid_camel_case_json_passes():
    assert podspec.validation_errors(_ir(FULL_PARAMS)) == []


# -- shape + required fields -----------------------------------------------


@pytest.mark.parametrize(
    "params,expect",
    [
        ({"volumes": {"name": "s"}}, "must be a JSON list"),
        ({"volumes": ["not-an-object"]}, "must be a JSON object"),
        ({"container_resources": [1, 2]}, "must be a JSON object"),
        ({"volumes": [{"emptyDir": {}}]}, "required 'name'"),
        ({"volume_mounts": [{"name": "s"}]}, "required 'mountPath'"),
        ({"init_containers": [{"name": "i"}]}, "required 'image'"),
    ],
)
def test_shape_and_required_field_errors(params, expect):
    errors = podspec.validation_errors(_ir({"image": "x", **params}))
    assert errors and expect in errors[0]


def test_errors_name_the_task():
    errors = podspec.validation_errors(_ir({"image": "x",
                                            "volumes": [{"emptyDir": {}}]}))
    assert "train" in errors[0]


# -- codegen ---------------------------------------------------------------


@pytest.mark.parametrize("syntax", ["taskflow", "traditional"])
def test_generates_valid_python_in_both_families(syntax):
    out = generate_dag(_ir(FULL_PARAMS, syntax))
    assert out["valid"], out["errors"]
    code = out["code"]
    # One deserialize call per task, read back as typed objects.
    assert "_pod_train = PodGenerator.deserialize_model_dict(" in code
    assert "volumes=_pod_train.volumes" in code
    assert "volume_mounts=_pod_train.containers[0].volume_mounts" in code
    assert "init_containers=_pod_train.init_containers" in code
    assert "container_resources=_pod_train.containers[0].resources" in code
    # Never `resources=`: that is BaseOperator's scheduler-resources kwarg and
    # would silently leave the container's k8s resources unset.
    assert "\n        resources=" not in code


@pytest.mark.parametrize("syntax", ["taskflow", "traditional"])
def test_import_is_collected_only_when_used(syntax):
    with_fields = generate_dag(_ir(FULL_PARAMS, syntax))["code"]
    assert "from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator" \
        in with_fields
    # A pod task that sets none of them is byte-identical to before the feature.
    without = generate_dag(_ir({"image": "python:3.12-slim"}, syntax))
    assert without["valid"], without["errors"]
    assert "PodGenerator" not in without["code"]
    assert "_pod_" not in without["code"]


def test_partial_fields_emit_only_what_is_set():
    out = generate_dag(_ir({"image": "x", "volumes": [{"name": "s", "emptyDir": {}}]}))
    assert out["valid"], out["errors"]
    assert "volumes=_pod_train.volumes" in out["code"]
    assert "volume_mounts=" not in out["code"]
    assert "container_resources=" not in out["code"]


def test_invalid_json_blocks_codegen():
    out = generate_dag(_ir({"image": "x", "volumes": [{"name": "s",
                                                       "empty_dir": {}}]}))
    assert out["valid"] is False
    assert "emptyDir" in out["errors"][0]


def test_other_operators_are_unaffected():
    """`pod_setup`/`pod_kwargs` are passed to every template; a non-pod operator
    must render exactly as before."""
    ir = _ir(FULL_PARAMS)
    ir["nodes"] = [{"id": "n", "op": "bash", "task_id": "t",
                    "params": {"bash_command": "echo hi"},
                    "position": {"x": 0, "y": 0}}]
    out = generate_dag(ir)
    assert out["valid"], out["errors"]
    assert "PodGenerator" not in out["code"] and "_pod_" not in out["code"]
