import os

import pytest

pytest_plugins = ("pytest_jupyter.jupyter_server", )


@pytest.fixture
def jp_server_config(jp_server_config):
    return {"ServerApp": {"jpserver_extensions": {"jupyterlab_airflow": True}}}


@pytest.fixture(scope="session", autouse=True)
def _airflow_studio_reconciler_off(tmp_path_factory):
    """Keep the deploy reconciler (PRD §6.5.4) out of the way of every test.

    A live PeriodicCallback inside the pytest_jupyter server would poll a
    FakeClient on a background thread mid-assertion. Tests that WANT the
    reconciler opt in via the ``reconciler_on`` fixture. The journal directory is
    redirected regardless of the switch, so a test run can never leave live
    entries behind that a later real ``jupyter lab`` would execute against a real
    Airflow.
    """
    previous = {
        name: os.environ.get(name)
        for name in ("JUPYTERLAB_AIRFLOW_RECONCILER", "JUPYTERLAB_AIRFLOW_JOURNAL_DIR")
    }
    os.environ["JUPYTERLAB_AIRFLOW_RECONCILER"] = "off"
    os.environ["JUPYTERLAB_AIRFLOW_JOURNAL_DIR"] = str(
        tmp_path_factory.mktemp("deploy-journal")
    )
    yield
    for name, value in previous.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


@pytest.fixture(autouse=True)
def _clean_journal_between_tests():
    """Give every test an empty journal, including its ``retired/`` markers.

    The session fixture above redirects the journal *directory*, but that
    directory is shared by the whole run, and ``get_journal()`` caches one
    ``Journal`` per root. A retire marker written by one test therefore survives
    into the next, where ``drop_retired`` filters that dag_id out of ``GET
    /dags`` — which is exactly how ``test_bare_ir_deploy_still_works_and_is_not
    _journaled`` came to pass alone and fail in the suite.

    Cleared before *and* after, so a test is protected from its predecessors
    whether or not they cleaned up after themselves.
    """
    from jupyterlab_airflow import journal as journal_mod

    def _wipe():
        try:
            store = journal_mod.get_journal()
        except Exception:  # noqa: BLE001 - a broken journal must not fail a test
            return
        for sub in ("pending", "inflight", "done", "quarantine", "retired"):
            directory = os.path.join(store.root, sub)
            if not os.path.isdir(directory):
                continue
            for name in os.listdir(directory):
                try:
                    os.unlink(os.path.join(directory, name))
                except OSError:
                    pass

    _wipe()
    yield
    _wipe()


@pytest.fixture
def reconciler_on(monkeypatch, tmp_path):
    """Turn the journal on for one test, with its own directory and **no timer**.

    ``start(schedule=False)`` wires the executor and the crash recovery without
    ever creating a PeriodicCallback, so the test drives ``sweep_once`` itself and
    nothing fires into its assertions.
    """
    from jupyterlab_airflow import journal as journal_mod
    from jupyterlab_airflow import reconciler as reconciler_mod

    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_RECONCILER", "on")
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_JOURNAL_DIR", str(tmp_path / "journal"))
    store = journal_mod.get_journal()
    rec = reconciler_mod.DeployReconciler(
        store,
        reconciler_mod._log,
        interval_s=reconciler_mod.interval_s(),
        deadline_s=reconciler_mod.deploy_budget_s(),
        retention_s=reconciler_mod.retention_s(),
    )
    monkeypatch.setattr(reconciler_mod, "_RECONCILER", rec)
    rec.start(schedule=False)
    yield store
    rec.stop()
    monkeypatch.setattr(reconciler_mod, "_RECONCILER", None)
