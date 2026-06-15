"""Tests for the validation pipeline (Appendix E stages 1-7)."""

from jupyterlab_airflow import validation


def _ir():
    return {
        "dag": {"dag_id": "vdag", "schedule": "@daily", "start_date": "2026-01-01"},
        "nodes": [
            {"id": "n", "op": "bash", "task_id": "t",
             "params": {"bash_command": "echo hi"}}
        ],
        "edges": [],
    }


def test_validate_passes_stages_1_to_6_and_skips_dagbag_without_airflow():
    res = validation.validate_dag(_ir())
    # Airflow isn't installed in the test env -> stage 7 is skipped, not failed.
    assert res["dagbag"]["status"] == "skipped"
    assert res["valid"] is True
    assert "from airflow.sdk import dag, task" in res["code"]


def test_validate_fails_on_bad_graph_without_running_dagbag():
    ir = _ir()
    ir["dag"]["dag_id"] = "1bad"  # invalid identifier -> stage 3 failure
    res = validation.validate_dag(ir)
    assert res["valid"] is False
    assert res["errors"]
    # A stage 1-6 failure short-circuits before the DagBag subprocess.
    assert res["dagbag"]["status"] == "skipped"


def test_dagbag_check_skips_cleanly_when_airflow_absent():
    # The subprocess must report skipped (never raise) when Airflow is missing.
    out = validation.dagbag_check("x = 1\n")
    assert out["status"] in ("skipped", "ok", "error")
    # In this env Airflow is absent -> skipped.
    assert out["status"] == "skipped"


def test_dagbag_env_is_secret_scrubbed(monkeypatch):
    monkeypatch.setenv("AIRFLOW_API_TOKEN", "supersecret")
    monkeypatch.setenv("AIRFLOW_PASSWORD", "pw")
    monkeypatch.setenv("MY_DB_PASSWORD", "pw2")
    env = validation._scrubbed_env()
    assert "AIRFLOW_API_TOKEN" not in env
    assert "AIRFLOW_PASSWORD" not in env
    assert "MY_DB_PASSWORD" not in env
