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

    def list_dags(self, limit=100, offset=0):
        return {"dags": [{"dag_id": "demo", "is_paused": False}], "total_entries": 1}

    def trigger_dag(self, dag_id, conf=None, logical_date=None):
        return {"dag_run_id": "manual__1", "dag_id": dag_id, "state": "queued"}


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
