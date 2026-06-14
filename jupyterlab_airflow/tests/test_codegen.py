"""Tests for IR -> Airflow 3.x TaskFlow code generation."""

import ast

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
