"""Smoke tests for the server extension handlers.

These exercise the Tornado routing and error mapping without a live Airflow:
the Airflow client is monkeypatched with a fake.
"""

import json

import pytest

from jupyterlab_airflow import client as client_module


class FakeClient:
    def health(self):
        return {"ok": True, "base_url": "http://airflow.test", "username": "admin"}

    def trigger_dag(self, dag_id, conf=None, logical_date=None):
        return {"dag_run_id": "manual__1", "dag_id": dag_id, "state": "queued"}

    def deploy_status(self, dag_id, filename):
        return {
            "state": "registered",
            "dag": {"dag_id": dag_id, "is_paused": True},
        }

    def list_import_errors(self, limit=100):
        return {"import_errors": [], "total_entries": 0}

    def list_dags(self, limit=100, offset=0, dag_id_pattern=None, **kwargs):
        return {"dags": [{"dag_id": "demo", "is_paused": False}], "total_entries": 1}

    def list_task_instances(self, dag_id, dag_run_id):
        return {
            "task_instances": [{"task_id": "t", "state": "success", "try_number": 1}],
            "total_entries": 1,
        }

    def get_task_logs(self, dag_id, dag_run_id, task_id, try_number=1):
        return {"content": "log line"}

    def clear_task_instances(self, dag_id, **kwargs):
        return {"task_instances": [{"task_id": "t"}], "total_entries": 1}

    def delete_dag(self, dag_id):
        return {}


@pytest.fixture(autouse=True)
def fake_client(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(client_module, "get_client", lambda: fake)
    # handlers import get_client by name into their own module namespace
    from jupyterlab_airflow import handlers as handlers_module

    monkeypatch.setattr(handlers_module, "get_client", lambda: fake)
    yield fake


async def test_dags_endpoint(jp_fetch):
    response = await jp_fetch("jupyterlab-airflow", "dags")
    assert response.code == 200
    payload = json.loads(response.body)
    assert payload["data"]["total_entries"] == 1


async def test_operators_endpoint(jp_fetch):
    response = await jp_fetch("jupyterlab-airflow", "operators")
    assert response.code == 200
    payload = json.loads(response.body)
    ops = payload["data"]
    ids = {op["id"] for op in ops}
    assert "bash" in ids
    bash = next(op for op in ops if op["id"] == "bash")
    assert bash["taskIdPrefix"] == "bash"
    # Codegen-only fields stay server-side.
    assert "import" not in bash and "template_taskflow" not in bash


async def test_generate_endpoint(jp_fetch):
    ir = {
        "dag": {"dag_id": "gen_dag", "schedule": "@daily", "start_date": "2026-01-01"},
        "nodes": [
            {"id": "n", "op": "bash", "task_id": "t",
             "params": {"bash_command": "echo hi"}}
        ],
        "edges": [],
    }
    response = await jp_fetch(
        "jupyterlab-airflow", "generate", method="POST", body=json.dumps(ir)
    )
    assert response.code == 200
    data = json.loads(response.body)["data"]
    assert data["valid"] is True
    assert "from airflow.sdk import dag, task" in data["code"]
    assert "@task.bash(task_id='t')" in data["code"]


def _bash_ir(dag_id="ep_dag"):
    return {
        "dag": {"dag_id": dag_id, "schedule": "@daily", "start_date": "2026-01-01"},
        "nodes": [
            {"id": "n", "op": "bash", "task_id": "t",
             "params": {"bash_command": "echo hi"}}
        ],
        "edges": [],
    }


async def test_validate_endpoint(jp_fetch):
    response = await jp_fetch(
        "jupyterlab-airflow", "validate", method="POST", body=json.dumps(_bash_ir())
    )
    assert response.code == 200
    data = json.loads(response.body)["data"]
    assert data["valid"] is True
    assert data["dagbag"]["status"] == "skipped"  # no Airflow in the test env


async def test_deploy_endpoint(jp_fetch, tmp_path, monkeypatch):
    monkeypatch.setenv("AIRFLOW_DAGS_DIR", str(tmp_path))
    response = await jp_fetch(
        "jupyterlab-airflow", "deploy", method="POST", body=json.dumps(_bash_ir())
    )
    assert response.code == 200
    data = json.loads(response.body)["data"]
    assert data["deployed"] is True
    assert data["filename"] == "ep_dag.py"
    assert (tmp_path / "ep_dag.py").exists()


async def test_deploy_status_endpoint(jp_fetch):
    response = await jp_fetch(
        "jupyterlab-airflow",
        "deploy",
        "status",
        params={"dag_id": "my_dag", "filename": "my_dag.py"},
    )
    assert response.code == 200
    data = json.loads(response.body)["data"]
    assert data["state"] == "registered"


async def test_import_errors_endpoint(jp_fetch):
    response = await jp_fetch("jupyterlab-airflow", "importerrors")
    assert response.code == 200
    data = json.loads(response.body)["data"]
    assert data["total_entries"] == 0


async def test_task_instances_endpoint(jp_fetch):
    response = await jp_fetch(
        "jupyterlab-airflow",
        "taskinstances",
        params={"dag_id": "demo", "run_id": "r1"},
    )
    assert response.code == 200
    data = json.loads(response.body)["data"]
    assert data["task_instances"][0]["task_id"] == "t"


async def test_task_logs_endpoint(jp_fetch):
    response = await jp_fetch(
        "jupyterlab-airflow",
        "taskinstances",
        "logs",
        params={"dag_id": "demo", "run_id": "r1", "task_id": "t"},
    )
    assert response.code == 200
    assert json.loads(response.body)["data"]["content"] == "log line"


async def test_task_clear_endpoint(jp_fetch):
    response = await jp_fetch(
        "jupyterlab-airflow",
        "taskinstances",
        "clear",
        method="POST",
        body=json.dumps({"dag_id": "demo", "run_id": "r1", "task_ids": ["t"]}),
    )
    assert response.code == 200
    assert json.loads(response.body)["data"]["total_entries"] == 1


async def test_dag_delete_endpoint(jp_fetch, tmp_path, monkeypatch):
    monkeypatch.setenv("AIRFLOW_DAGS_DIR", str(tmp_path))
    response = await jp_fetch(
        "jupyterlab-airflow",
        "dags",
        "delete",
        method="POST",
        body=json.dumps({"dag_id": "demo"}),
    )
    assert response.code == 200
    data = json.loads(response.body)["data"]
    assert data["dag_id"] == "demo"
    assert data["purged_history"] is True


async def test_trigger_endpoint(jp_fetch):
    response = await jp_fetch(
        "jupyterlab-airflow",
        "dags",
        "trigger",
        method="POST",
        body=json.dumps({"dag_id": "demo"}),
    )
    assert response.code == 200
    payload = json.loads(response.body)
    assert payload["data"]["state"] == "queued"
