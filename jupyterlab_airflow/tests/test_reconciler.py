"""Tests for the deploy reconciler (PRD §6.5.4).

The brain is a plain synchronous function with ``now``/``client``/``target``/
``journal`` injected, so every case below runs without a server, without a live
Airflow, and — crucially — without a single ``sleep``.
"""

import json
import logging
from datetime import timedelta

import pytest

from jupyterlab_airflow import client as client_module
from jupyterlab_airflow.audit import AUDIT_LOGGER_NAME
from jupyterlab_airflow.client import AirflowError
from jupyterlab_airflow.deploy import MANAGED_PREFIX, DeployError
from jupyterlab_airflow.journal import (
    JOURNAL_VERSION,
    Journal,
    iso,
    new_steps,
    parse_ts,
    utcnow,
)
from jupyterlab_airflow.reconciler import reopen_expired, sweep_once

from .fakes import FakeClient, FakeTarget, managed_file

CID = "3f9c1b8a4d21e0775c8a9b0f16dd4e21"
DAG_ID = "sales_daily"
OLD_DAG_ID = "sales_dly"
AFDAG_ID = "afd_7c21"
LOG = logging.getLogger("test.reconciler")


@pytest.fixture
def store(tmp_path):
    return Journal(str(tmp_path / "deploy-journal"))


@pytest.fixture
def now():
    return utcnow()


@pytest.fixture
def audit_records():
    records = []

    class _H(logging.Handler):
        def emit(self, record):
            records.append(json.loads(record.getMessage()))

    logger = logging.getLogger(AUDIT_LOGGER_NAME)
    handler = _H()
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.INFO)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)


@pytest.fixture(autouse=True)
def _airflow_env(monkeypatch):
    # The supersession check compares the entry's recorded base_url against the
    # live config, so pin it (and keep the default deploy target).
    monkeypatch.setenv("AIRFLOW_API_URL", "http://airflow:8080")
    monkeypatch.delenv("AIRFLOW_DEPLOY_TARGET", raising=False)


def make_entry(created, *, retire=None, phase="awaiting_registration",
               budget_s=900, run_on_deploy=True, **overrides):
    entry = {
        "version": JOURNAL_VERSION,
        "deploy_id": CID,
        "created_at": iso(created),
        "updated_at": iso(created),
        "deadline_at": iso(created + timedelta(seconds=budget_s)),
        "action_deadline_at": iso(created + timedelta(seconds=budget_s)),
        "next_attempt_at": iso(created),
        "polls": 0,
        "user": "aristide",
        "role_at_deploy": "editor",
        "dag_id": DAG_ID,
        "filename": f"{DAG_ID}.py",
        "afdag_id": AFDAG_ID,
        "airflow_base_url": "http://airflow:8080",
        "target_kind": "shared_volume",
        "run_id": f"studio__{CID}",
        "run_on_deploy": run_on_deploy,
        "retire": retire,
        "phase": phase,
        "steps": new_steps(),
        "outcome": None,
        "import_error": None,
        "message": "",
        "terminal_at": None,
    }
    entry.update(overrides)
    return entry


def deployed_target(extra=None):
    files = {f"{DAG_ID}.py": managed_file(DAG_ID, afdag_id=AFDAG_ID, correlation_id=CID)}
    files.update(extra or {})
    return FakeTarget(files)


def sweep(store, client, target, when):
    return sweep_once(
        now=when, journal=store, client=client, target=target, log=LOG, retention=86400
    )


def drain(store, client, target, when, limit=8):
    """Sweep repeatedly at the same instant until the entry is terminal. One
    state transition happens per entry per sweep, by design."""
    for _ in range(limit):
        report = sweep(store, client, target, when)
        if report.pending == 0:
            return report
    raise AssertionError("entry never reached a terminal state")


@pytest.fixture
def as_client(monkeypatch):
    """Point ``get_client()`` at the fake, so ``retire_old_dag``'s own lookups
    (set_paused / delete_dag / variable purge) go through it too."""

    def _install(client):
        monkeypatch.setattr(client_module, "get_client", lambda: client)
        return client

    return _install


# --------------------------------------------------------------------------- #
# 1. The reported defect, verbatim.
# --------------------------------------------------------------------------- #
def test_deploy_completes_with_no_browser_anywhere(store, now, as_client):
    """18:56:09 written · 18:59:42 page reloaded · 19:01:05 registered.

    The widget died 85 s before Airflow registered the DAG, so the old dag_id
    stayed deployed and active and the new one stayed paused forever. Nothing in
    this test has a browser.
    """
    client = as_client(
        FakeClient([{"state": "processing"}, {"state": "processing"},
                    {"state": "registered", "dag": {"dag_id": DAG_ID, "is_paused": True}}])
    )
    target = deployed_target(
        {f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="old")}
    )
    store.put(make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False}))

    sweep(store, client, target, now + timedelta(seconds=30))
    sweep(store, client, target, now + timedelta(seconds=120))
    drain(store, client, target, now + timedelta(seconds=296))

    entry = store.get(CID)
    assert entry["outcome"] == "completed"
    assert entry["steps"]["retire"]["state"] == "done"
    assert f"{OLD_DAG_ID}.py" in target.deleted  # the old file is gone
    assert client.paused[OLD_DAG_ID] is True  # the old DAG is paused
    assert client.paused[DAG_ID] is False  # the new DAG is live
    assert client.names("trigger_dag") == [("trigger_dag", DAG_ID, f"studio__{CID}")]


# --------------------------------------------------------------------------- #
# 2-3. Timing out and failing to import never retire.
# --------------------------------------------------------------------------- #
def test_registration_timeout_never_retires(store, now, as_client, audit_records):
    client = as_client(FakeClient([{"state": "processing"}]))
    target = deployed_target(
        {f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="old")}
    )
    store.put(make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False}))

    for offset in (10, 60, 200):
        sweep(store, client, target, now + timedelta(seconds=offset))
    sweep(store, client, target, now + timedelta(seconds=1000))

    entry = store.get(CID)
    assert entry["outcome"] == "expired"
    # Timing out is a diagnosis, never a licence to act.
    assert target.deleted == []
    assert client.count("set_paused") == 0
    assert client.count("delete_dag") == 0
    # The residue is operationally dangerous, so it IS audited.
    residue = [r for r in audit_records if r["action"] == "retire"]
    assert residue and residue[0]["outcome"] == "error"
    assert residue[0]["dag_id"] == OLD_DAG_ID
    assert residue[0]["via"] == "reconciler"


def test_import_failure_never_retires(store, now, as_client):
    client = as_client(
        FakeClient([{"state": "failed", "import_error": {"stack_trace": "boom"}}])
    )
    target = deployed_target(
        {f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="old")}
    )
    store.put(make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False}))

    sweep(store, client, target, now + timedelta(seconds=30))

    entry = store.get(CID)
    assert entry["outcome"] == "import_failed"
    # A broken new DAG must never cost you the working old one.
    assert entry["steps"]["retire"]["state"] == "skipped"
    assert target.deleted == []
    assert client.count("set_paused") == 0


# --------------------------------------------------------------------------- #
# 4-6. Supersession: only definite evidence licenses a decision.
# --------------------------------------------------------------------------- #
def _registered_at_retire(now):
    """Registered, at the first step that touches the old DAG.

    Since the commit-last reorder that is `quiescing` (pause it) rather than
    `retiring` (delete it) — the destructive half now runs only after unpause
    and trigger have actually completed.
    """
    entry = make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False}, phase="quiescing")
    entry["steps"]["registered"]["state"] = "done"
    return entry


def _ready_to_retire_entry(now, **kw):
    """Registered, unpaused and triggered — the only state that authorises the
    irreversible retire."""
    entry = make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False},
                       phase="retiring", **kw)
    for name in ("registered", "quiesce", "unpause", "trigger"):
        entry["steps"][name]["state"] = "done"
    return entry


@pytest.mark.parametrize(
    "files, label",
    [
        ({}, "file deleted"),
        ({f"{DAG_ID}.py": managed_file(DAG_ID, afdag_id=AFDAG_ID, correlation_id="newer")},
         "a newer deploy overwrote it"),
        ({f"{DAG_ID}.py": managed_file(DAG_ID, afdag_id=AFDAG_ID, correlation_id="older")},
         "a rollback restored the .bak"),
        ({f"{DAG_ID}.py": "# airflow-studio: managed  dag_id=sales_daily\nx = 1\n"},
         "no correlation_id"),
    ],
)
def test_supersession_stops_every_mutating_step(store, now, as_client, files, label):
    client = as_client(FakeClient())
    target = FakeTarget(files)
    store.put(_registered_at_retire(now))

    sweep(store, client, target, now + timedelta(seconds=10))

    assert store.get(CID)["outcome"] == "superseded", label
    assert client.calls == []
    assert target.deleted == []


def test_ambiguity_is_not_supersession(store, now, as_client):
    # A read failure is ambiguity: the entry waits and is retried, never acted on.
    client = as_client(FakeClient())
    target = deployed_target()
    target.read_error = DeployError("dags folder unreachable")
    store.put(_registered_at_retire(now))

    sweep(store, client, target, now + timedelta(seconds=10))

    entry = store.get(CID)
    assert entry["outcome"] is None
    assert entry["phase"] == "quiescing"
    assert client.calls == []


def test_environment_drift_is_supersession(store, now, as_client, monkeypatch):
    # Completing a lifecycle against a DIFFERENT Airflow is a wrong action.
    monkeypatch.setenv("AIRFLOW_API_URL", "http://other-airflow:8080")
    client = as_client(FakeClient())
    store.put(_registered_at_retire(now))

    sweep(store, client, deployed_target(), now + timedelta(seconds=10))

    assert store.get(CID)["outcome"] == "superseded"
    assert client.calls == []


# --------------------------------------------------------------------------- #
# 7-9. The trigger: the only non-idempotent step.
# --------------------------------------------------------------------------- #
def test_trigger_happens_exactly_once_with_a_deterministic_run_id(store, now, as_client):
    client = as_client(FakeClient([{"state": "registered"}]))
    target = deployed_target()
    store.put(make_entry(now))

    drain(store, client, target, now + timedelta(seconds=30))
    # Another sweep after the entry is terminal must not re-trigger.
    sweep(store, client, target, now + timedelta(seconds=120))

    assert client.names("trigger_dag") == [("trigger_dag", DAG_ID, f"studio__{CID}")]


def test_409_adopts_the_existing_run(store, now, as_client):
    # The window a `triggered` flag can never cover: kill -9 between the request
    # landing in Airflow and the journal update being persisted.
    client = as_client(FakeClient())
    client.trigger_raises = AirflowError("duplicate", status=409)
    entry = make_entry(now, phase="triggering")
    entry["steps"]["registered"]["state"] = "done"
    entry["steps"]["unpause"]["state"] = "done"
    store.put(entry)

    sweep(store, client, deployed_target(), now + timedelta(seconds=10))

    assert store.get(CID)["outcome"] == "completed"
    assert client.count("get_dag_run") == 1
    assert client.count("trigger_dag") == 1  # no retry loop


def test_404_on_trigger_is_terminal(store, now, as_client):
    client = as_client(FakeClient())
    client.trigger_raises = AirflowError("gone", status=404)
    entry = make_entry(now, phase="triggering")
    entry["steps"]["registered"]["state"] = "done"
    entry["steps"]["unpause"]["state"] = "done"
    store.put(entry)

    sweep(store, client, deployed_target(), now + timedelta(seconds=10))
    sweep(store, client, deployed_target(), now + timedelta(seconds=300))

    assert store.get(CID)["outcome"] == "failed"
    assert client.count("trigger_dag") == 1


# --------------------------------------------------------------------------- #
# 10-11. Retiring the WRONG DAG is the hazard, not retiring twice.
# --------------------------------------------------------------------------- #
def test_retire_refuses_a_file_owned_by_another_flow(store, now, as_client, audit_records):
    stranger = managed_file(OLD_DAG_ID, afdag_id="afd_someone_else", correlation_id="theirs")
    client = as_client(FakeClient())
    target = deployed_target({f"{OLD_DAG_ID}.py": stranger})
    store.put(_ready_to_retire_entry(now))

    sweep(store, client, target, now + timedelta(seconds=10))

    assert target.files[f"{OLD_DAG_ID}.py"] == stranger  # untouched
    assert client.count("set_paused") == 0  # pausing a stranger's DAG is as bad
    entry = store.get(CID)
    assert entry["steps"]["retire"]["state"] == "skipped"
    rejected = [r for r in audit_records if r["action"] == "retire"]
    assert rejected and rejected[0]["outcome"] == "rejected"


def test_never_retires_the_dag_this_deploy_wrote(store, now, as_client, caplog):
    # A rename away and back must not retire the DAG we just deployed.
    client = as_client(FakeClient())
    target = deployed_target()
    entry = make_entry(now, retire={"dag_id": DAG_ID, "purge": True}, phase="retiring")
    for name in ("registered", "quiesce", "unpause", "trigger"):
        entry["steps"][name]["state"] = "done"
    store.put(entry)

    with caplog.at_level(logging.ERROR, logger="jupyterlab_airflow.reconciler"):
        sweep(store, client, target, now + timedelta(seconds=10))

    assert store.get(CID)["steps"]["retire"]["state"] == "skipped"
    assert target.deleted == []
    assert client.count("delete_dag") == 0
    assert any("refusing to retire" in rec.message for rec in caplog.records)


# --------------------------------------------------------------------------- #
# 12-13. Deliberate user intent, and the action budget.
# --------------------------------------------------------------------------- #
def test_a_user_pause_vetoes_the_unpause_and_the_trigger(store, now, as_client):
    client = as_client(FakeClient([{"state": "registered"}]))
    target = deployed_target()
    store.put(make_entry(now))
    store.veto_unpause(DAG_ID)

    drain(store, client, target, now + timedelta(seconds=30))

    entry = store.get(CID)
    assert entry["outcome"] == "completed"
    assert client.count("set_paused") == 0
    assert client.count("trigger_dag") == 0
    assert "paused" in entry["message"]


def test_past_the_action_deadline_nothing_is_unpaused_or_triggered(store, now, as_client):
    client = as_client(FakeClient())
    entry = make_entry(now, phase="unpausing")
    entry["steps"]["registered"]["state"] = "done"
    entry["action_deadline_at"] = iso(now - timedelta(seconds=1))
    store.put(entry)

    sweep(store, client, deployed_target(), now)

    stored = store.get(CID)
    assert stored["outcome"] == "expired"
    assert stored["steps"]["unpause"]["state"] == "skipped"
    assert stored["steps"]["trigger"]["state"] == "skipped"
    assert client.calls == []


# --------------------------------------------------------------------------- #
# 14. Crash recovery.
# --------------------------------------------------------------------------- #
def test_restart_resumes_at_the_step_that_was_pending(store, now, as_client):
    client = as_client(FakeClient())
    entry = make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False}, phase="triggering")
    for name in ("registered", "quiesce", "unpause"):
        entry["steps"][name]["state"] = "done"
    store.put(entry)
    store.claim(CID)  # simulate the process dying mid-sweep

    assert store.resume() == 1
    drain(store, client, deployed_target(), now + timedelta(seconds=200))

    assert store.get(CID)["outcome"] == "completed"
    # The new DAG is never unpaused again: that step was already `done`, and the
    # entry resumed at `triggering`, which is past it.
    assert [c for c in client.calls if c[0] == "set_paused" and c[1] == DAG_ID] == []
    assert client.count("trigger_dag") == 1


# --------------------------------------------------------------------------- #
# 15. Supersede + retire inheritance (deploy-side).
# --------------------------------------------------------------------------- #
def _journal_a_deploy(deploy_id, dag_id, lifecycle, monkeypatch, tmp_path):
    from jupyterlab_airflow import deploy as deploy_mod

    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_JOURNAL_DIR", str(tmp_path / "journal"))
    ir = {"provenance": {"afdag_id": AFDAG_ID}}
    deploy_mod._journal_deploy(
        deploy_id, dag_id, f"{dag_id}.py", ir, lifecycle, "aristide", FakeTarget()
    )


def test_a_redeploy_supersedes_and_inherits_the_retire_intent(monkeypatch, tmp_path):
    from jupyterlab_airflow.journal import get_journal

    first = "1" * 32
    second = "2" * 32
    _journal_a_deploy(
        first, DAG_ID, {"retire": {"dag_id": OLD_DAG_ID, "purge": False}, "run_on_deploy": True},
        monkeypatch, tmp_path,
    )
    _journal_a_deploy(second, DAG_ID, {"retire": None, "run_on_deploy": True}, monkeypatch, tmp_path)

    store = get_journal()
    assert store.get(first)["outcome"] == "superseded"
    # The old dag_id is not stranded: the new entry carries the retire forward.
    assert store.get(second)["retire"] == {"dag_id": OLD_DAG_ID, "purge": False}


def test_a_second_rename_queues_both_retire_intents(monkeypatch, tmp_path):
    """Rename A → B, deploy; then rename B → C inside the ~300 s registration
    window and deploy again.

    The entry used to have exactly one retire slot, so the inherited intent was
    dropped with nothing but a log line — and it dropped the *more dangerous* one:
    B was at worst paused by the superseded deploy, while A was still deployed,
    still registered and (the retire being what would have paused it) still
    UNPAUSED, running on its schedule beside C. The deploy response reported a
    clean success. Both intents are now queued.
    """
    from jupyterlab_airflow.journal import get_journal, retire_intents

    first = "1" * 32
    second = "2" * 32
    _journal_a_deploy(
        first, DAG_ID, {"retire": {"dag_id": OLD_DAG_ID, "purge": False}, "run_on_deploy": True},
        monkeypatch, tmp_path,
    )
    _journal_a_deploy(
        second, "sales_v3",
        {"retire": {"dag_id": DAG_ID, "purge": False}, "run_on_deploy": True},
        monkeypatch, tmp_path,
    )

    entry = get_journal().get(second)
    assert [i["dag_id"] for i in retire_intents(entry)] == [DAG_ID, OLD_DAG_ID]


def test_both_queued_retires_are_actually_performed(store, now, as_client):
    """…and the queue is drained: neither dag_id is left live and unpaused."""
    client = as_client(FakeClient([{"state": "registered"}]))
    target = deployed_target(
        {
            f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="a"),
            "sales_mid.py": managed_file("sales_mid", afdag_id=AFDAG_ID, correlation_id="b"),
        }
    )
    store.put(
        make_entry(
            now,
            retire={"dag_id": "sales_mid", "purge": False},
            retire_also=[{"dag_id": OLD_DAG_ID, "purge": False}],
        )
    )

    drain(store, client, target, now + timedelta(seconds=30))

    entry = store.get(CID)
    assert entry["outcome"] == "completed"
    assert entry["steps"]["retire"]["state"] == "done"
    assert sorted(target.deleted) == sorted([f"{OLD_DAG_ID}.py", "sales_mid.py"])
    assert client.paused[OLD_DAG_ID] is True and client.paused["sales_mid"] is True
    assert client.paused[DAG_ID] is False  # only the new DAG is live


def test_a_mid_queue_failure_never_retires_the_same_dag_twice(store, now, as_client,
                                                              audit_records):
    """The queue is drained across sweeps, so a retry after a transient failure
    must not re-delete (or re-audit) what already succeeded."""
    client = as_client(FakeClient([{"state": "registered"}]))
    target = deployed_target(
        {
            f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="a"),
            "sales_mid.py": managed_file("sales_mid", afdag_id=AFDAG_ID, correlation_id="b"),
        }
    )
    entry = make_entry(
        now,
        retire={"dag_id": "sales_mid", "purge": False},
        retire_also=[{"dag_id": OLD_DAG_ID, "purge": False}],
        phase="retiring",
    )
    for name in ("registered", "quiesce", "unpause", "trigger"):
        entry["steps"][name]["state"] = "done"
    store.put(entry)

    # The SECOND retire hits a transient 500 — the first has already landed.
    failing = {"count": 0}
    real_set_paused = client.set_paused

    def _flaky(dag_id, is_paused):
        if dag_id == OLD_DAG_ID and failing["count"] == 0:
            failing["count"] += 1
            raise AirflowError("upstream hiccup", status=503)
        return real_set_paused(dag_id, is_paused)

    client.set_paused = _flaky
    sweep(store, client, target, now + timedelta(seconds=10))
    # The first intent landed; the second is queued for retry, and the step
    # records what already succeeded so it is not redone.
    assert store.get(CID)["steps"]["retire"]["retired"] == ["sales_mid"]

    drain(store, client, target, now + timedelta(seconds=200))

    assert store.get(CID)["outcome"] == "completed"
    assert target.deleted.count("sales_mid.py") == 1  # retired exactly once
    ok = [r for r in audit_records if r["action"] == "retire" and r["outcome"] == "ok"]
    assert sorted(r["dag_id"] for r in ok) == sorted([OLD_DAG_ID, "sales_mid"])


def test_an_expired_renames_retire_is_inherited_by_the_next_deploy(
    store, now, as_client, monkeypatch, tmp_path
):
    """Rename A → B; Airflow is slower than the budget, so the lifecycle expires
    with the retire *skipped* and A left registered and unpaused. The user clicks
    Deploy again — the natural recovery.

    `supersede` only scans OPEN entries, so the terminal entry's intent used to be
    invisible: the new deploy carried `retire=None` and A was never retired. The
    other route out was closed at the same moment — `reopen_expired` refuses once
    a newer deploy of the flow is in flight, and the editor only ever re-attaches
    to `latest_for_afdag`, which now returns the *new* entry — so A stayed live,
    unpaused and scheduled, in the list beside B, with no remedy left in the UI.
    """
    from jupyterlab_airflow import deploy as deploy_mod

    client = as_client(FakeClient([{"state": "processing"}]))
    target = deployed_target(
        {f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="old")}
    )
    store.put(make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False}))
    for offset in (10, 60, 200, 500, 901):
        sweep(store, client, target, now + timedelta(seconds=offset))
    expired = store.get(CID)
    assert expired["outcome"] == "expired"
    assert expired["steps"]["retire"]["state"] == "skipped"

    # Deploy again. `_journal_deploy` writes into the SAME journal as `store`.
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_JOURNAL_DIR", store.root)
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_DEPLOY_BUDGET_S", "7200")
    second = "2" * 32
    deploy_mod._journal_deploy(
        second, DAG_ID, f"{DAG_ID}.py", {"provenance": {"afdag_id": AFDAG_ID}},
        {"retire": None, "run_on_deploy": True}, "aristide", FakeTarget(),
    )
    # …and the file on disk is the one THIS deploy wrote (the supersession check
    # reads the correlation_id out of its provenance header).
    target.files[f"{DAG_ID}.py"] = managed_file(
        DAG_ID, afdag_id=AFDAG_ID, correlation_id=second
    )
    assert store.get(second)["retire"] == {"dag_id": OLD_DAG_ID, "purge": False}
    # Taken over exactly once: a re-armed "Keep waiting" cannot retire it again.
    assert store.get(CID)["steps"]["retire"]["skipped_reason"] == (
        "inherited by a newer deploy of this flow"
    )

    later = now + timedelta(seconds=1000)
    client.statuses = [{"state": "registered"}]
    drain(store, client, target, later)

    assert store.get(second)["outcome"] == "completed"
    assert f"{OLD_DAG_ID}.py" in target.deleted
    assert client.paused[OLD_DAG_ID] is True


# --------------------------------------------------------------------------- #
# 16-19. Robustness, authorization, clocks, and the trace loop.
# --------------------------------------------------------------------------- #
def test_a_corrupt_entry_never_blocks_a_healthy_sibling(store, now, as_client):
    import os

    client = as_client(FakeClient([{"state": "registered"}]))
    target = deployed_target()
    store.put(make_entry(now))
    with open(os.path.join(store.root, "pending", f"{'9' * 32}.json"), "w") as fh:
        fh.write("{not json")

    drain(store, client, target, now + timedelta(seconds=30))

    assert store.get(CID)["outcome"] == "completed"
    assert os.listdir(os.path.join(store.root, "quarantine"))


def test_a_role_downgrade_denies_every_pending_entry(store, now, as_client, monkeypatch,
                                                     audit_records):
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_ROLE", "viewer")
    client = as_client(FakeClient([{"state": "registered"}]))
    store.put(make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False}))

    report = sweep(store, client, deployed_target(), now + timedelta(seconds=10))

    assert report.denied == 1
    assert client.calls == []  # nothing was attempted
    assert store.get(CID)["outcome"] == "denied"
    denied = [r for r in audit_records if r["outcome"] == "denied"]
    assert {r["action"] for r in denied} == {"retire", "unpause", "trigger"}
    assert all(r["via"] == "reconciler" and r["detail"] == "role=viewer" for r in denied)


def test_a_forward_clock_jump_does_not_abandon_a_fresh_entry(store, now, as_client):
    client = as_client(FakeClient([{"state": "processing"}]))
    target = deployed_target()
    store.put(make_entry(now))

    sweep(store, client, target, now + timedelta(seconds=10))
    sweep(store, client, target, now + timedelta(seconds=60))
    # NTP yanks the clock past the deadline at polls == 2: expiry needs BOTH.
    sweep(store, client, target, now + timedelta(seconds=5000))

    entry = store.get(CID)
    assert entry["outcome"] is None
    assert entry["polls"] == 3


def test_every_record_of_a_lifecycle_shares_one_correlation_id(store, now, as_client,
                                                               audit_records):
    client = as_client(FakeClient([{"state": "registered"}]))
    target = deployed_target(
        {f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="old")}
    )
    store.put(make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False}))

    drain(store, client, target, now + timedelta(seconds=30))

    assert {r["action"] for r in audit_records} == {"retire", "unpause", "trigger"}
    assert {r["correlation_id"] for r in audit_records} == {CID}
    assert all(r["via"] == "reconciler" for r in audit_records)
    # The human who clicked Deploy — never "system".
    assert {r["user"] for r in audit_records} == {"aristide"}


def test_backoff_is_jittered_and_bounded(store, now, as_client):
    client = as_client(FakeClient([{"state": "processing"}]))
    target = deployed_target()
    store.put(make_entry(now))

    sweep(store, client, target, now)
    delay = (parse_ts(store.get(CID)["next_attempt_at"]) - now).total_seconds()
    assert 12 <= delay <= 18  # 15s ±20%


async def test_sweep_now_runs_on_the_dedicated_thread(store, monkeypatch):
    """The scheduling shell: every blocking thing happens off the IOLoop, on one
    named thread of its own — so a sweep stalled on an unreachable git remote can
    never starve the interactive requests that share the default executor."""
    import threading

    from jupyterlab_airflow import reconciler as reconciler_mod

    threads = set()

    class _Recording(FakeClient):
        def deploy_status(self, dag_id, filename):
            threads.add(threading.current_thread().name)
            return super().deploy_status(dag_id, filename)

    client = _Recording([{"state": "registered"}])
    target = deployed_target()
    monkeypatch.setattr(reconciler_mod, "get_client", lambda: client)
    monkeypatch.setattr(reconciler_mod, "get_deploy_target", lambda: target)
    store.put(make_entry(utcnow()))

    rec = reconciler_mod.DeployReconciler(
        store, LOG, interval_s=5, deadline_s=900, retention_s=86400
    )
    rec.start(schedule=False)  # no PeriodicCallback: the test drives the sweep
    try:
        report = await rec.sweep_now()
    finally:
        rec.stop()

    assert report.advanced == 1
    assert all(name.startswith("afstudio-reconciler") for name in threads)
    assert threads and threading.current_thread().name not in threads


# --------------------------------------------------------------------------- #
# 20-25. Concurrency, budgets and ownership — the adversarial-review defects.
# --------------------------------------------------------------------------- #
def test_a_superseded_entry_a_sweep_holds_never_unpauses_the_old_dag(store, now, as_client):
    """The critical defect, end to end.

    Flow A is deployed as `sales_daily`. A sweep claims the entry and blocks in
    an Airflow call; meanwhile the user renames the flow and redeploys, which
    supersedes it. The sweep then releases its stale copy. Before the claim
    token that release resurrected the entry, and it went on to unpause and
    trigger `sales_daily` — the dag_id the user had just renamed away from.
    """
    client = as_client(FakeClient([{"state": "registered"}]))
    target = deployed_target()
    store.put(make_entry(now))

    claimed = store.claim(CID)  # a sweep takes it and stalls in `deploy_status`
    store.supersede(dag_id="sales_v2", afdag_id=AFDAG_ID, except_deploy_id="b" * 32)
    claimed["phase"] = "unpausing"
    claimed["steps"]["registered"]["state"] = "done"
    store.release(claimed)  # the stalled call returns

    drain(store, client, target, now + timedelta(seconds=30))

    entry = store.get(CID)
    assert entry["outcome"] == "superseded"
    assert client.count("set_paused") == 0
    assert client.count("trigger_dag") == 0


def test_a_cancel_recorded_mid_claim_stops_the_remaining_steps(store, now, as_client):
    """The cancel handler's escape hatch, which used to report success and then
    be silently undone by the holding sweep's release."""
    client = as_client(FakeClient([{"state": "registered"}]))
    target = deployed_target(
        {f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="old")}
    )
    store.put(make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": True}))

    claimed = store.claim(CID)
    stopped = store.request_stop(
        CID, outcome="cancelled", message="Cancelled — the remaining steps were not run."
    )
    assert stopped == "requested"
    claimed["steps"]["registered"]["state"] = "done"
    claimed["phase"] = "retiring"
    store.release(claimed)

    drain(store, client, target, now + timedelta(seconds=30))

    entry = store.get(CID)
    assert entry["outcome"] == "cancelled"
    assert target.deleted == []  # nothing retired
    assert client.count("delete_dag") == 0  # nothing purged
    assert client.count("set_paused") == 0  # nothing unpaused
    assert client.count("trigger_dag") == 0  # nothing triggered


def test_an_unreadable_target_expires_the_retire_instead_of_looping(store, now, as_client):
    """`_do_retire` was the only phase with no deadline check, so an `unknown`
    supersession verdict (an expired S3 credential, an unreachable endpoint)
    backed off forever: never terminal, the observing tab never resolved, and the
    reconciler's PeriodicCallback never stopped."""
    client = as_client(FakeClient())
    target = deployed_target()
    target.read_error = DeployError("s3: credentials expired")
    store.put(_ready_to_retire_entry(now))

    # Well inside the budget: it just waits.
    report = sweep(store, client, target, now + timedelta(seconds=10))
    assert report.pending == 1
    assert store.get(CID)["outcome"] is None

    # Past the action budget: terminal, and the un-retired DAG is reported.
    sweep(store, client, target, now + timedelta(seconds=901))
    entry = store.get(CID)
    assert entry["outcome"] == "expired"
    assert entry["steps"]["retire"]["state"] == "skipped"
    assert OLD_DAG_ID in entry["message"]
    assert target.deleted == []


def test_a_flow_without_provenance_never_retires_a_strangers_dag(
    store, now, as_client, audit_records
):
    """`expect_afdag_id=entry["afdag_id"] or None` disabled the ownership guard
    for exactly the flows it protects least well. A pre-provenance flow's delayed
    retire could then delete and purge another flow's freshly deployed DAG."""
    stranger = managed_file(OLD_DAG_ID, afdag_id="afd_flow_b", correlation_id="theirs")
    client = as_client(FakeClient())
    target = deployed_target({f"{OLD_DAG_ID}.py": stranger})
    entry = _ready_to_retire_entry(now)
    entry["afdag_id"] = ""  # a pre-provenance `.afdag`
    entry["retire"]["purge"] = True
    store.put(entry)

    sweep(store, client, target, now + timedelta(seconds=10))

    assert target.files[f"{OLD_DAG_ID}.py"] == stranger  # NOT deleted
    assert client.count("delete_dag") == 0  # history NOT purged
    stored = store.get(CID)
    assert stored["steps"]["retire"]["state"] == "skipped"
    assert [r["outcome"] for r in audit_records if r["action"] == "retire"] == ["rejected"]


def test_a_pre_provenance_file_is_still_retired(store, now, as_client):
    """The mirror image: a file deployed before the header carried `afdag_id` is
    *unattributable*, not *foreign*. Refusing there stranded the rename and told
    the user another flow owned a file that was theirs all along."""
    legacy = f"{MANAGED_PREFIX}  studio=0.1.0  dag_id={OLD_DAG_ID}  syntax=taskflow\nx = 1\n"
    client = as_client(FakeClient())
    target = deployed_target({f"{OLD_DAG_ID}.py": legacy})
    store.put(_ready_to_retire_entry(now))

    drain(store, client, target, now + timedelta(seconds=10))

    entry = store.get(CID)
    assert entry["steps"]["retire"]["state"] == "done"
    assert f"{OLD_DAG_ID}.py" in target.deleted
    assert client.paused[OLD_DAG_ID] is True


def test_keep_waiting_rearms_an_expired_rename_server_side(store, now, as_client):
    """"Keep waiting" after an expired budget must re-arm the SERVER. The retire
    intent lives only in the journal, so a browser-side retry would unpause and
    trigger the new DAG while the old one is still live and unpaused."""
    client = as_client(FakeClient([{"state": "processing"}]))
    target = deployed_target(
        {f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="old")}
    )
    store.put(make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": False}))

    # Airflow never registers it inside the budget.
    for offset in (10, 60, 200, 500, 901):
        sweep(store, client, target, now + timedelta(seconds=offset))
    expired = store.get(CID)
    assert expired["outcome"] == "expired"
    assert expired["steps"]["retire"]["skipped_reason"] == "the new DAG never registered"

    later = now + timedelta(seconds=1000)
    resumed, reason = reopen_expired(store, CID, now=later, budget_s=900)
    assert (resumed, reason) == (True, "")
    rearmed = store.get(CID)
    assert rearmed["outcome"] is None
    assert rearmed["phase"] == "awaiting_registration"
    assert rearmed["steps"]["retire"]["state"] == "pending"

    # Airflow finally picks the file up, and the SERVER finishes the rename.
    client.statuses = [{"state": "registered"}]
    drain(store, client, target, later + timedelta(seconds=30))

    entry = store.get(CID)
    assert entry["outcome"] == "completed"
    assert f"{OLD_DAG_ID}.py" in target.deleted  # the old DAG is retired…
    assert client.paused[OLD_DAG_ID] is True
    assert client.paused[DAG_ID] is False  # …before the new one goes live


def test_only_an_expired_lifecycle_can_be_rearmed(store, now, as_client):
    client = as_client(FakeClient([{"state": "registered"}]))
    store.put(make_entry(now, run_on_deploy=False))
    drain(store, client, deployed_target(), now + timedelta(seconds=10))
    assert store.get(CID)["outcome"] == "completed"

    resumed, reason = reopen_expired(store, CID, now=now, budget_s=900)
    assert resumed is False and "not expired" in reason
    assert reopen_expired(store, "f" * 32, now=now, budget_s=900) == (False, "no such deploy")


# --------------------------------------------------------------------------- #
# Regression: the reported incident (2026-08-17).
#
# A user renamed HELLO -> HELLOO. The reconciler registered the new DAG, then
# PURGED the old one, then declared itself `superseded` with unpause and trigger
# still pending — "nothing further was done". The user was left with no working
# DAG at all and the old one's run history irrecoverably gone.
#
# Two invariants now make that sequence unreachable, and each is pinned here:
#   I1 commit-last        — the irreversible half runs only after the new DAG is
#                           actually live, so an abandoned migration leaves the
#                           old DAG paused-but-intact rather than deleted.
#   I2 no silent abandon  — an entry that HAS committed can never close with an
#                           outcome meaning "nothing further was needed".
# --------------------------------------------------------------------------- #
def test_the_reported_incident_leaves_the_old_dag_intact(store, now, as_client):
    """Supersession arriving mid-migration must not cost the user their DAG."""
    client = as_client(FakeClient([{"state": "registered"}]))
    # The new DAG's file has been replaced by a newer deploy — the exact
    # condition that made the real entry declare itself superseded.
    target = deployed_target(
        {
            f"{OLD_DAG_ID}.py": managed_file(OLD_DAG_ID, afdag_id=AFDAG_ID, correlation_id="a"),
            f"{DAG_ID}.py": managed_file(DAG_ID, afdag_id=AFDAG_ID, correlation_id="newer"),
        }
    )
    entry = make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": True}, phase="quiescing")
    entry["steps"]["registered"]["state"] = "done"
    store.put(entry)

    drain(store, client, target, now + timedelta(seconds=200))
    result = store.get(CID)

    # The migration stopped, as it should — but destroyed nothing on the way.
    assert result["outcome"] == "superseded"
    assert target.deleted == []                     # the old .py survives
    assert client.count("delete_dag") == 0          # its history survives
    assert result["steps"]["retire"]["state"] != "done"


def test_a_committed_entry_can_never_be_abandoned_silently(store, now, as_client,
                                                           audit_records):
    """I2: once something irreversible happened, `superseded` is not an honest
    ending — the entry must say it needs repair, and say so in the audit log."""
    client = as_client(FakeClient([{"state": "registered"}]))
    target = deployed_target({f"{DAG_ID}.py": managed_file(DAG_ID, afdag_id=AFDAG_ID, correlation_id=CID)})
    entry = make_entry(now, retire={"dag_id": OLD_DAG_ID, "purge": True}, phase="retiring")
    for name in ("registered", "quiesce", "unpause", "trigger"):
        entry["steps"][name]["state"] = "done"
    # Pretend the destructive half already landed, then have the entry stop.
    entry["steps"]["retire"]["result"] = {"removed_file": True, "purged_history": True}
    entry["stop_requested"] = {"outcome": "superseded", "message": "a newer deploy took over"}
    store.put(entry)

    drain(store, client, target, now + timedelta(seconds=60))
    result = store.get(CID)

    assert result["outcome"] == "needs_repair"
    assert OLD_DAG_ID in result["message"] and DAG_ID in result["message"]
    assert [r for r in audit_records
            if r["action"] == "retire" and r["outcome"] == "error"]
