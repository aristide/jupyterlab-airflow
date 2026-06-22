"""Tests for the operator registry loader (`GET operators` data source)."""

import pytest

from jupyterlab_airflow import registry


@pytest.fixture(autouse=True)
def _clear_cache():
    # Each test starts from a cold cache and unset user dir.
    registry._cache["signature"] = None
    registry._cache["operators"] = None
    yield


def test_bundled_registry_loads():
    ops = registry.load_registry()
    ids = {op["id"] for op in ops}
    assert {"empty", "bash", "python_task", "branch", "trigger_dagrun"} <= ids
    # The §6.2.1 P0 additions: ShortCircuit/LatestOnly + the first Sensors.
    assert {
        "shortcircuit",
        "latest_only",
        "file_sensor",
        "external_task_sensor",
        "datetime_sensor",
        "timedelta_sensor",
    } <= ids
    # Sorted by (category, label) for a deterministic palette order.
    keys = [(op.get("category", ""), op.get("label", op["id"])) for op in ops]
    assert keys == sorted(keys)


def test_p1_gated_ops_present_with_correct_providers():
    by_id = {op["id"]: op for op in registry.load_registry()}
    assert {"http", "sql", "sql_sensor"} <= set(by_id)
    expected = {
        "http": (
            "apache-airflow-providers-http",
            "airflow.providers.http.operators.http",
        ),
        "sql": (
            "apache-airflow-providers-common-sql",
            "airflow.providers.common.sql.operators.sql",
        ),
        "sql_sensor": (
            "apache-airflow-providers-common-sql",
            "airflow.providers.common.sql.sensors.sql",
        ),
    }
    for op_id, (provider, module) in expected.items():
        op = by_id[op_id]
        assert op["provider"] == provider, op_id
        assert module in op["import"], op_id
        # Gated ops use a NON-standard provider (so gating dims them when absent).
        assert op["provider"] != "apache-airflow-providers-standard", op_id
        # Never an Airflow-2 import path.
        assert "airflow.operators." not in op["import"], op_id
        assert "airflow.sensors." not in op["import"], op_id
    # SqlSensor lives in the Sensors category with the sensor common_params.
    assert by_id["sql_sensor"]["category"] == "Sensors"
    assert {"mode", "poke_interval", "timeout"} <= set(
        by_id["sql_sensor"]["common_params"]
    )


def test_p2_cloud_k8s_ops_present_with_correct_providers():
    by_id = {op["id"]: op for op in registry.load_registry()}
    assert {
        "kubernetes_pod",
        "s3_key_sensor",
        "gcs_object_sensor",
        "bigquery_insert_job",
    } <= set(by_id)
    expected = {
        "kubernetes_pod": (
            "apache-airflow-providers-cncf-kubernetes",
            "airflow.providers.cncf.kubernetes.operators.pod",
            "Kubernetes",
        ),
        "s3_key_sensor": (
            "apache-airflow-providers-amazon",
            "airflow.providers.amazon.aws.sensors.s3",
            "Sensors",
        ),
        "gcs_object_sensor": (
            "apache-airflow-providers-google",
            "airflow.providers.google.cloud.sensors.gcs",
            "Sensors",
        ),
        "bigquery_insert_job": (
            "apache-airflow-providers-google",
            "airflow.providers.google.cloud.operators.bigquery",
            "Cloud",
        ),
    }
    for op_id, (provider, module, category) in expected.items():
        op = by_id[op_id]
        assert op["provider"] == provider, op_id
        assert module in op["import"], op_id
        assert op["category"] == category, op_id
        # Gated (non-standard) providers, non-Airflow-2 import paths.
        assert op["provider"] != "apache-airflow-providers-standard", op_id
        assert "airflow.operators." not in op["import"], op_id
        assert "airflow.sensors." not in op["import"], op_id
    # KubernetesPodOperator carries the new (non-legacy) `operators.pod` module.
    assert "operators.kubernetes_pod" not in by_id["kubernetes_pod"]["import"]


def test_sensors_are_airflow3_standard_provider():
    by_id = {op["id"]: op for op in registry.load_registry()}
    expected = {
        "file_sensor": "airflow.providers.standard.sensors.filesystem",
        "external_task_sensor": "airflow.providers.standard.sensors.external_task",
        "datetime_sensor": "airflow.providers.standard.sensors.date_time",
        "timedelta_sensor": "airflow.providers.standard.sensors.time_delta",
        "latest_only": "airflow.providers.standard.operators.latest_only",
        "shortcircuit": "airflow.providers.standard.operators.python",
    }
    for op_id, module in expected.items():
        op = by_id[op_id]
        assert module in op["import"], op_id
        # Never an Airflow-2 import path (those fail to import in Airflow 3).
        assert "airflow.sensors." not in op["import"], op_id
        assert "airflow.operators." not in op["import"], op_id
        assert op["provider"] == "apache-airflow-providers-standard", op_id


def test_sensors_declare_the_sensor_common_params():
    # FileSensor establishes the Sensors category + the sensor common_params
    # (mode / poke_interval / timeout) on top of the universal per-task ones.
    by_id = {op["id"]: op for op in registry.load_registry()}
    for op_id in (
        "file_sensor",
        "external_task_sensor",
        "datetime_sensor",
        "timedelta_sensor",
    ):
        common = by_id[op_id].get("common_params", [])
        assert {"mode", "poke_interval", "timeout"} <= set(common), op_id
        assert by_id[op_id]["category"] == "Sensors", op_id


def test_v13_lakehouse_p0_ops_present_with_correct_providers():
    by_id = {op["id"]: op for op in registry.load_registry()}
    # (id, provider, import-module, category) — verified against the real provider
    # wheels (amazon 9.30, sftp 5.8, common-sql 2.0, apache-spark 6.1,
    # papermill 3.13, smtp 3.0, slack 9.10).
    expected = {
        "s3_create_object": (
            "apache-airflow-providers-amazon",
            "airflow.providers.amazon.aws.operators.s3",
            "Storage",
        ),
        "sftp_transfer": (
            "apache-airflow-providers-sftp",
            "airflow.providers.sftp.operators.sftp",
            "Ingestion",
        ),
        "sql_column_check": (
            "apache-airflow-providers-common-sql",
            "airflow.providers.common.sql.operators.sql",
            "Data Quality",
        ),
        "sql_table_check": (
            "apache-airflow-providers-common-sql",
            "airflow.providers.common.sql.operators.sql",
            "Data Quality",
        ),
        "spark_submit": (
            "apache-airflow-providers-apache-spark",
            "airflow.providers.apache.spark.operators.spark_submit",
            "Compute",
        ),
        "papermill": (
            "apache-airflow-providers-papermill",
            "airflow.providers.papermill.operators.papermill",
            "Compute",
        ),
        "email": (
            "apache-airflow-providers-smtp",
            "airflow.providers.smtp.operators.smtp",
            "Notifications",
        ),
        "slack_post": (
            "apache-airflow-providers-slack",
            "airflow.providers.slack.operators.slack",
            "Notifications",
        ),
    }
    assert set(expected) <= set(by_id)
    for op_id, (provider, module, category) in expected.items():
        op = by_id[op_id]
        assert op["provider"] == provider, op_id
        assert module in op["import"], op_id
        assert op["category"] == category, op_id
        # Operator-style (no TaskFlow-native decorator); gated (non-standard).
        assert op["taskflow"] == "operator", op_id
        assert op["provider"] != "apache-airflow-providers-standard", op_id
        # Never an Airflow-2 import path.
        assert "airflow.operators." not in op["import"], op_id
        assert "airflow.sensors." not in op["import"], op_id
        assert "airflow.decorators" not in op["import"], op_id
        # Both template families ship (Traditional is still supported, §6.3).
        assert op["template_taskflow"].strip(), op_id
        assert op["template_traditional"].strip(), op_id


def test_bash_is_airflow3_correct():
    bash = next(op for op in registry.load_registry() if op["id"] == "bash")
    assert "airflow.providers.standard.operators.bash" in bash["import"]
    # Never an Airflow-2 import path.
    assert "airflow.operators." not in bash["import"]
    assert bash["task_id_prefix"] == "bash"


def test_client_view_hides_codegen_fields():
    view = registry.client_view()
    bash = next(op for op in view if op["id"] == "bash")
    # camelCase key for the TS interface.
    assert bash["taskIdPrefix"] == "bash"
    # Codegen-only fields are not shipped to the browser.
    for hidden in ("import", "import_taskflow", "template_traditional", "template_taskflow"):
        assert hidden not in bash
    required = {p["name"] for p in bash["params"] if p["required"]}
    assert required == {"bash_command"}


def test_client_view_ships_common_params():
    view = {op["id"]: op for op in registry.client_view()}
    # Every op exposes the universal per-task common settings (PRD §6.1.3)...
    assert view["bash"]["commonParams"] == [
        "retries",
        "retry_delay",
        "depends_on_past",
    ]
    # ...and sensors add the sensor common params.
    assert {"mode", "poke_interval", "timeout"} <= set(
        view["file_sensor"]["commonParams"]
    )


def test_client_view_ships_doc_fields_for_info_tab():
    bash = next(op for op in registry.client_view() if op["id"] == "bash")
    # Operator-level learning fields, camelCased for the TS IOperatorDef.
    assert bash["description"]
    assert bash["docsUrl"].startswith("http")
    assert bash["example"]
    assert bash["provider"] == "apache-airflow-providers-standard"
    assert bash["airflowMinVersion"] == "3.0"
    # Per-param contextual help reaches the client.
    cmd = next(p for p in bash["params"] if p["name"] == "bash_command")
    assert cmd["help"]


def test_client_view_ships_sensor_for_palette_and_form():
    view = {op["id"]: op for op in registry.client_view()}
    fs = view["file_sensor"]
    # The palette groups by category; "Sensors" is a new group rendered as-is.
    assert fs["category"] == "Sensors"
    assert fs["taskIdPrefix"] == "file_sensor"
    # The NODE form needs the required/optional params (with help) for this op.
    required = {p["name"] for p in fs["params"] if p["required"]}
    assert required == {"filepath"}
    assert any(p["name"] == "fs_conn_id" and not p["required"] for p in fs["params"])
    # Codegen-only fields stay server-side even for the new ops.
    assert "import" not in fs and "template_taskflow" not in fs


def test_caches_until_files_change():
    first = registry.load_registry()
    assert registry.load_registry() is first  # same object: cache hit
    assert registry.load_registry(force=True) is not first  # forced reload


def test_user_dir_overrides_and_adds(tmp_path, monkeypatch):
    # Add a brand-new operator and override the bundled `bash` label.
    (tmp_path / "custom.yaml").write_text("id: custom\nlabel: Custom\ncategory: Misc\n")
    (tmp_path / "bash.yaml").write_text("id: bash\nlabel: Overridden bash\ncategory: Python/Bash\n")
    monkeypatch.setenv("AIRFLOW_OPERATORS_DIR", str(tmp_path))

    by_id = {op["id"]: op for op in registry.load_registry(force=True)}
    assert "custom" in by_id
    assert by_id["bash"]["label"] == "Overridden bash"


def test_top_level_must_be_mapping(tmp_path, monkeypatch):
    (tmp_path / "broken.yaml").write_text("- just\n- a\n- list\n")
    monkeypatch.setenv("AIRFLOW_OPERATORS_DIR", str(tmp_path))
    with pytest.raises(registry.RegistryError):
        registry.load_registry(force=True)


def test_missing_id_raises(tmp_path, monkeypatch):
    (tmp_path / "noid.yaml").write_text("label: No id here\ncategory: Misc\n")
    monkeypatch.setenv("AIRFLOW_OPERATORS_DIR", str(tmp_path))
    with pytest.raises(registry.RegistryError):
        registry.load_registry(force=True)
