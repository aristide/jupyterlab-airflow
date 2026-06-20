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


def test_deploy_status_registered(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    requests_mock.get(
        f"{BASE}{API_PREFIX}/importErrors", json={"import_errors": []}
    )
    requests_mock.get(
        f"{BASE}{API_PREFIX}/dags/my_dag",
        json={"dag_id": "my_dag", "is_paused": True},
    )

    out = make_client().deploy_status("my_dag", "my_dag.py")
    assert out["state"] == "registered"
    assert out["dag"]["is_paused"] is True


def test_deploy_status_failed_matches_filename(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    requests_mock.get(
        f"{BASE}{API_PREFIX}/importErrors",
        json={
            "import_errors": [
                {"filename": "/opt/airflow/dags/my_dag.py", "stack_trace": "boom"}
            ]
        },
    )

    out = make_client().deploy_status("my_dag", "my_dag.py")
    assert out["state"] == "failed"
    assert out["import_error"]["stack_trace"] == "boom"


def test_deploy_status_processing_when_dag_absent(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    requests_mock.get(
        f"{BASE}{API_PREFIX}/importErrors",
        json={"import_errors": [{"filename": "/opt/airflow/dags/other.py"}]},
    )
    requests_mock.get(f"{BASE}{API_PREFIX}/dags/my_dag", status_code=404, json={})

    out = make_client().deploy_status("my_dag", "my_dag.py")
    assert out["state"] == "processing"


def test_list_dags_uses_exclude_stale_not_only_active(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    m = requests_mock.get(f"{BASE}{API_PREFIX}/dags", json={"dags": []})

    make_client().list_dags(dag_id_pattern="etl")

    qs = m.last_request.qs
    assert qs["exclude_stale"] == ["true"]
    assert "only_active" not in qs
    assert qs["dag_id_pattern"] == ["etl"]


def test_clear_task_instances_dry_run_payload(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    m = requests_mock.post(
        f"{BASE}{API_PREFIX}/dags/d/clearTaskInstances",
        json={"task_instances": [{"task_id": "t"}], "total_entries": 1},
    )

    out = make_client().clear_task_instances(
        "d", task_ids=["t"], dag_run_id="r1", dry_run=True
    )

    body = m.last_request.json()
    assert body["dry_run"] is True
    assert body["task_ids"] == ["t"]
    assert body["dag_run_id"] == "r1"
    assert out["total_entries"] == 1


def test_get_task_logs_normalises_list_to_text(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    requests_mock.get(
        f"{BASE}{API_PREFIX}/dags/d/dagRuns/r/taskInstances/t/logs/1",
        json={"content": [["2026-06-15", "started"], "done"]},
    )

    out = make_client().get_task_logs("d", "r", "t", 1)
    assert out["content"] == "2026-06-15 started\ndone"


def test_delete_dag_issues_delete(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    m = requests_mock.delete(f"{BASE}{API_PREFIX}/dags/d", json={})

    make_client().delete_dag("d")
    assert m.called


def test_list_providers_reads_target(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    m = requests_mock.get(
        f"{BASE}{API_PREFIX}/providers",
        json={
            "providers": [
                {"package_name": "apache-airflow-providers-http", "version": "5.0.0"}
            ],
            "total_entries": 1,
        },
    )
    out = make_client().list_providers()
    assert m.called
    assert out["providers"][0]["package_name"] == "apache-airflow-providers-http"


def test_version_reads_target(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    requests_mock.get(
        f"{BASE}{API_PREFIX}/version",
        json={"version": "3.0.2", "git_version": "abc"},
    )
    assert make_client().version()["version"] == "3.0.2"


def test_get_dag_details_fetches_params(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    m = requests_mock.get(
        f"{BASE}{API_PREFIX}/dags/d/details",
        json={
            "dag_id": "d",
            "params": {
                "region": {
                    "value": "eu-west-1",
                    "description": "Target region",
                    "schema": {"type": "string", "enum": ["eu-west-1", "us-east-1"]},
                }
            },
        },
    )

    out = make_client().get_dag_details("d")
    assert m.called
    assert out["params"]["region"]["value"] == "eu-west-1"


def test_get_dag_run_fetches_single_run(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    m = requests_mock.get(
        f"{BASE}{API_PREFIX}/dags/d/dagRuns/r1",
        json={"dag_run_id": "r1", "state": "running"},
    )

    out = make_client().get_dag_run("d", "r1")
    assert m.called
    assert out["state"] == "running"


def test_set_dag_run_state_patches_state(requests_mock):
    requests_mock.post(f"{BASE}/auth/token", json={"access_token": "t"})
    m = requests_mock.patch(
        f"{BASE}{API_PREFIX}/dags/d/dagRuns/r1",
        json={"dag_run_id": "r1", "state": "failed"},
    )

    # Default state is "failed" — the stop-a-run path (PRD §6.6).
    make_client().set_dag_run_state("d", "r1")
    assert m.last_request.json() == {"state": "failed"}
