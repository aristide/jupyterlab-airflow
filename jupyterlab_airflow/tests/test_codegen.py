"""Tests for IR -> Airflow 3.x TaskFlow code generation."""

import ast
import re

import pytest

from jupyterlab_airflow.codegen import generate_dag


def _ir(nodes, edges, **dag):
    base = {
        "dag_id": "my_dag",
        "schedule": "@daily",
        "start_date": "2026-01-01",
        "catchup": False,
        "retries": 1,
        "retry_delay_seconds": 300,
        "tags": ["studio"],
    }
    base.update(dag)
    return {"dag": base, "nodes": nodes, "edges": edges}


def test_taskflow_dag_is_valid_python_and_airflow3():
    ir = _ir(
        nodes=[
            {"id": "n1", "op": "bash", "task_id": "extract",
             "params": {"bash_command": "echo hi"}},
            {"id": "n2", "op": "python_task", "task_id": "transform",
             "params": {"code": "return 1"}},
        ],
        edges=[{"source": "n1", "target": "n2"}],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]

    # Parses and compiles (no execution).
    ast.parse(code)

    # Airflow 3.x imports only — never Airflow-2 paths.
    assert "from airflow.sdk import dag, task" in code
    assert "airflow.operators." not in code
    assert "from airflow.decorators" not in code
    assert "from airflow.models" not in code

    # Provenance header + structure.
    assert code.startswith("# airflow-studio: managed")
    assert "syntax=taskflow" in code
    assert "@task.bash(task_id='extract')" in code
    assert "extract_task >> transform_task" in code
    assert code.rstrip().endswith("my_dag()")


def test_header_carries_afdag_id_for_reassociation():
    ir = _ir(
        nodes=[{"id": "n1", "op": "bash", "task_id": "t",
                "params": {"bash_command": "echo hi"}}],
        edges=[],
    )
    ir["provenance"] = {"afdag_id": "abc-123-uuid"}
    code = generate_dag(ir)["code"]
    # afdag_id rides in the provenance header so a deployed DAG stays linked to
    # its `.afdag` across a dag_id rename (PRD §6.1.8(B) / §8.9).
    assert "afdag_id=abc-123-uuid" in code.splitlines()[0]


def test_operators_render_as_assignments():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "empty", "task_id": "start", "params": {}},
            {"id": "b", "op": "trigger_dagrun", "task_id": "fire",
             "params": {"trigger_dag_id": "other"}},
        ],
        edges=[{"source": "a", "target": "b"}],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    assert "start = EmptyOperator(" in code
    assert "fire = TriggerDagRunOperator(" in code
    assert "from airflow.providers.standard.operators.empty import EmptyOperator" in code
    # `dag, task` from airflow.sdk is imported exactly once (no redundant line).
    assert code.count("from airflow.sdk import task\n") == 0


def test_sensors_render_as_airflow3_operators():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "file_sensor", "task_id": "wait_file",
             "params": {"filepath": "/data/in_{{ ds }}.csv"}},
            {"id": "b", "op": "external_task_sensor", "task_id": "wait_etl",
             "params": {"external_dag_id": "ingest", "external_task_id": "load"}},
            {"id": "c", "op": "datetime_sensor", "task_id": "after_six",
             "params": {"target_time": "{{ ds }}T18:00:00+00:00"}},
            {"id": "d", "op": "timedelta_sensor", "task_id": "hold",
             "params": {"delta_seconds": 3600}},
        ],
        edges=[
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
            {"source": "c", "target": "d"},
        ],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)

    # Operator-style instances, Airflow-3 standard-provider imports only.
    assert "wait_file = FileSensor(" in code
    assert "wait_etl = ExternalTaskSensor(" in code
    assert "after_six = DateTimeSensor(" in code
    assert "hold = TimeDeltaSensor(" in code
    assert "from airflow.providers.standard.sensors.filesystem import FileSensor" in code
    assert (
        "from airflow.providers.standard.sensors.external_task import ExternalTaskSensor"
        in code
    )
    assert "airflow.sensors." not in code and "airflow.operators." not in code
    # TimeDeltaSensor reuses the pinned timedelta import; the seconds are int()-
    # coerced so a stringified value can't emit an importable-but-broken delta.
    assert "delta=timedelta(seconds=int(3600))" in code
    # An optional param left out is simply not emitted.
    assert "fs_conn_id" not in code


def test_timedelta_sensor_coerces_a_stringified_value():
    # If delta_seconds reaches the IR as a string, int(...) keeps the emitted
    # timedelta importable instead of `timedelta(seconds='3600')` (a runtime
    # TypeError that still passes ast.parse).
    ir = _ir(
        nodes=[{"id": "n", "op": "timedelta_sensor", "task_id": "hold",
                "params": {"delta_seconds": "3600"}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    assert "delta=timedelta(seconds=int('3600'))" in res["code"]


def test_optional_sensor_param_is_emitted_when_set():
    ir = _ir(
        nodes=[{"id": "a", "op": "file_sensor", "task_id": "wait_file",
                "params": {"filepath": "/data/x", "fs_conn_id": "my_fs"}}],
        edges=[],
    )
    code = generate_dag(ir)["code"]
    assert "fs_conn_id='my_fs'" in code


def test_shortcircuit_renders_taskflow_decorator():
    ir = _ir(
        nodes=[{"id": "n", "op": "shortcircuit", "task_id": "gate",
                "params": {"code": "return bool(rows)"}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    assert "@task.short_circuit(task_id='gate')" in code
    assert "return bool(rows)" in code


def test_latest_only_renders_as_operator():
    ir = _ir(
        nodes=[{"id": "n", "op": "latest_only", "task_id": "only_latest",
                "params": {}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    assert "only_latest = LatestOnlyOperator(" in code
    assert (
        "from airflow.providers.standard.operators.latest_only import LatestOnlyOperator"
        in code
    )


def test_gated_provider_ops_render_airflow3():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "http", "task_id": "call_api",
             "params": {"http_conn_id": "my_api", "endpoint": "v1/orders",
                        "method": "GET"}},
            {"id": "b", "op": "sql", "task_id": "load",
             "params": {"conn_id": "warehouse", "sql": "INSERT INTO t SELECT 1"}},
            {"id": "c", "op": "sql_sensor", "task_id": "wait_rows",
             "params": {"conn_id": "warehouse", "sql": "SELECT count(*) FROM t"}},
        ],
        edges=[{"source": "c", "target": "a"}, {"source": "a", "target": "b"}],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)

    assert "call_api = HttpOperator(" in code
    assert "endpoint='v1/orders'" in code and "method='GET'" in code
    assert "http_conn_id='my_api'" in code
    assert "load = SQLExecuteQueryOperator(" in code
    assert "wait_rows = SqlSensor(" in code
    assert "conn_id='warehouse'" in code
    # Airflow-3 provider imports, never Airflow-2 paths.
    assert "from airflow.providers.http.operators.http import HttpOperator" in code
    assert (
        "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator"
        in code
    )
    assert "from airflow.providers.common.sql.sensors.sql import SqlSensor" in code
    assert "airflow.operators." not in code and "airflow.sensors." not in code


def test_http_optional_params_only_emitted_when_set():
    # No http_conn_id/data/headers -> those kwargs are omitted (operator defaults).
    ir = _ir(
        nodes=[{"id": "n", "op": "http", "task_id": "ping",
                "params": {"endpoint": "health", "method": "GET"}}],
        edges=[],
    )
    code = generate_dag(ir)["code"]
    assert "endpoint='health'" in code
    assert "http_conn_id" not in code
    assert "data=" not in code and "headers=" not in code
    # Optional params come after the required ones, so an omitted http_conn_id
    # leaves no stray blank line inside the call.
    assert "ping',\n\n" not in code

    # With them set -> emitted.
    ir2 = _ir(
        nodes=[{"id": "n", "op": "http", "task_id": "post",
                "params": {"endpoint": "hook", "method": "POST",
                           "data": "payload", "headers": {"X-Key": "1"}}}],
        edges=[],
    )
    code2 = generate_dag(ir2)["code"]
    assert "data='payload'" in code2
    assert "headers={'X-Key': '1'}" in code2


def test_p2_cloud_k8s_ops_render_airflow3():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "kubernetes_pod", "task_id": "pod",
             "params": {"image": "python:3.12-slim", "arguments": ["print(1)"]}},
            {"id": "b", "op": "s3_key_sensor", "task_id": "wait_s3",
             "params": {"bucket_name": "lake", "bucket_key": "in/x.csv"}},
            {"id": "c", "op": "gcs_object_sensor", "task_id": "wait_gcs",
             "params": {"bucket": "gb", "object": "in/x.csv"}},
            {"id": "d", "op": "bigquery_insert_job", "task_id": "bq",
             "params": {"configuration": {"query": {"query": "SELECT 1"}}}},
        ],
        edges=[{"source": "b", "target": "a"}, {"source": "c", "target": "d"}],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)

    assert "pod = KubernetesPodOperator(" in code and "image='python:3.12-slim'" in code
    assert "wait_s3 = S3KeySensor(" in code and "bucket_key='in/x.csv'" in code
    assert "wait_gcs = GCSObjectExistenceSensor(" in code and "object='in/x.csv'" in code
    assert "bq = BigQueryInsertJobOperator(" in code and "configuration=" in code
    assert (
        "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator"
        in code
    )
    assert "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor" in code
    assert (
        "from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor"
        in code
    )
    assert (
        "from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator"
        in code
    )
    assert "airflow.operators." not in code and "airflow.sensors." not in code


def test_operator_block_drops_blank_for_omitted_middle_optional():
    # KubernetesPodOperator with image + env_vars set but the optionals BETWEEN
    # them (name/namespace/cmds/arguments) omitted must not leave a stray blank
    # line inside the constructor call (operator blocks are blank-stripped at
    # render time, since they hold no user code).
    ir = _ir(
        nodes=[{"id": "n", "op": "kubernetes_pod", "task_id": "pod",
                "params": {"image": "img", "env_vars": {"K": "1"}}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    block = re.search(r"pod = KubernetesPodOperator\(.*?\n {4}\)", code, re.S)
    assert block, code
    assert "\n\n" not in block.group(0)
    assert "image='img'" in code and "env_vars={'K': '1'}" in code


def test_code_node_body_blank_lines_are_preserved():
    # The operator-block blank-strip must NOT touch a code node's user-authored
    # body — blank lines the user wrote (incl. inside a multi-line literal) stay.
    body = "vals = [\n    1,\n\n    2,\n]\nreturn sum(vals)"
    ir = _ir(
        nodes=[{"id": "n", "op": "python_task", "task_id": "t",
                "params": {"code": body}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    # Both the between-statements blank and the in-literal blank survive.
    assert "1,\n\n" in res["code"]


def test_cycle_is_rejected():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "empty", "task_id": "x", "params": {}},
            {"id": "b", "op": "empty", "task_id": "y", "params": {}},
        ],
        edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
    )
    res = generate_dag(ir)
    assert not res["valid"]
    assert any("cycle" in e.lower() for e in res["errors"])


def test_invalid_and_duplicate_identifiers():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "empty", "task_id": "dup", "params": {}},
            {"id": "b", "op": "empty", "task_id": "dup", "params": {}},
        ],
        edges=[],
        dag_id="1bad",
    )
    res = generate_dag(ir)
    assert not res["valid"]
    joined = " ".join(res["errors"])
    assert "dag_id" in joined and "Duplicate" in joined


def test_unknown_operator_is_rejected():
    ir = _ir(
        nodes=[{"id": "a", "op": "does_not_exist", "task_id": "t", "params": {}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert not res["valid"]
    assert "Unknown operator" in res["errors"][0]


def test_string_values_are_escaped_not_injected():
    # A bash_command containing quotes/newlines must round-trip as a safe literal.
    payload = 'echo "hi";\nrm -rf /tmp/x  # not executed'
    ir = _ir(
        nodes=[{"id": "n", "op": "bash", "task_id": "t",
                "params": {"bash_command": payload}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    # The payload is emitted via repr() inside the function body; parsing the
    # generated module yields back the exact string with no code injection.
    module = ast.parse(res["code"])
    literals = [
        node.value
        for node in ast.walk(module)
        if isinstance(node, ast.Constant) and node.value == payload
    ]
    assert literals, "bash_command should appear as a single safe string literal"


def test_deterministic_output():
    ir = _ir(
        nodes=[{"id": "n", "op": "bash", "task_id": "t",
                "params": {"bash_command": "echo hi"}}],
        edges=[],
    )
    assert generate_dag(ir)["code"] == generate_dag(ir)["code"]
