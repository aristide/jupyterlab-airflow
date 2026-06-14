"""Unit tests for the Airflow REST client using requests-mock."""

import pytest

from jupyterlab_airflow.client import API_PREFIX, AirflowClient, AirflowError
from jupyterlab_airflow.config import AirflowConfig

BASE = "http://airflow.test"


def make_client():
    cfg = AirflowConfig(base_url=BASE, username="admin", password="admin")
    return AirflowClient(cfg)


def test_fetches_token_then_lists_dags(requests_mock):
    token_m = requests_mock.post(
        f"{BASE}/auth/token", json={"access_token": "tok-123"}
    )
    dags_m = requests_mock.get(
        f"{BASE}{API_PREFIX}/dags",
        json={"dags": [{"dag_id": "demo"}], "total_entries": 1},
    )

    client = make_client()
    out = client.list_dags()

    assert out["total_entries"] == 1
    assert token_m.called
    assert dags_m.last_request.headers["Authorization"] == "Bearer tok-123"


def test_refreshes_token_on_401(requests_mock):
    requests_mock.post(
        f"{BASE}/auth/token",
        [{"json": {"access_token": "old"}}, {"json": {"access_token": "new"}}],
    )
    requests_mock.get(
        f"{BASE}{API_PREFIX}/dags",
        [{"status_code": 401, "json": {}}, {"json": {"dags": []}}],
    )

    client = make_client()
    out = client.list_dags()

    assert out == {"dags": []}


def test_trigger_posts_logical_date_and_conf(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    run_m = requests_mock.post(
        f"{BASE}{API_PREFIX}/dags/demo/dagRuns", json={"dag_run_id": "r1"}
    )

    client = make_client()
    client.trigger_dag("demo", conf={"k": "v"})

    body = run_m.last_request.json()
    assert body["conf"] == {"k": "v"}
    assert "logical_date" in body


def test_raises_airflow_error_on_bad_credentials(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", status_code=401, json={"detail": "nope"})

    client = make_client()
    with pytest.raises(AirflowError):
        client.list_dags()
