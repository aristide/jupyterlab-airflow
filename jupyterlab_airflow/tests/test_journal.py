"""Tests for the durable deploy journal (PRD §6.5.4).

The journal is pure filesystem + validation, so everything here is exercised
directly against a temp directory — no server, no Airflow, no clock.
"""

import json
import os
import threading
from datetime import timedelta

import pytest

from jupyterlab_airflow.journal import (
    JOURNAL_VERSION,
    MAX_ENTRY_BYTES,
    Journal,
    get_journal,
    iso,
    new_steps,
    utcnow,
)


def _entry(deploy_id="a" * 32, dag_id="sales_daily", **overrides):
    now = utcnow()
    entry = {
        "version": JOURNAL_VERSION,
        "deploy_id": deploy_id,
        "created_at": iso(now),
        "updated_at": iso(now),
        "deadline_at": iso(now + timedelta(seconds=900)),
        "action_deadline_at": iso(now + timedelta(seconds=900)),
        "next_attempt_at": iso(now),
        "polls": 0,
        "user": "aristide",
        "role_at_deploy": "editor",
        "dag_id": dag_id,
        "filename": f"{dag_id}.py",
        "afdag_id": "afd_1",
        "airflow_base_url": "http://airflow:8080",
        "target_kind": "shared_volume",
        "run_id": f"studio__{deploy_id}",
        "run_on_deploy": True,
        "retire": None,
        "phase": "awaiting_registration",
        "steps": new_steps(),
        "outcome": None,
        "import_error": None,
        "message": "",
        "terminal_at": None,
    }
    entry.update(overrides)
    return entry


@pytest.fixture
def journal(tmp_path):
    return Journal(str(tmp_path / "deploy-journal"))


def test_put_get_release_finish_round_trip(journal):
    deploy_id = journal.put(_entry())
    assert deploy_id == "a" * 32
    assert journal.get(deploy_id)["dag_id"] == "sales_daily"

    claimed = journal.claim(deploy_id)
    assert claimed is not None
    claimed["phase"] = "unpausing"
    journal.release(claimed)
    assert journal.list_pending()[0]["phase"] == "unpausing"

    entry = journal.claim(deploy_id)
    journal.finish(entry, "completed")
    assert journal.list_pending() == []
    done = journal.get(deploy_id)
    assert done["outcome"] == "completed" and done["terminal_at"]


def test_atomic_write_leaves_no_temp_file(journal):
    journal.put(_entry())
    leftovers = [p for p in os.listdir(journal.root) if p.startswith(".afjournal-")]
    assert leftovers == []


def test_claim_is_exclusive(journal):
    journal.put(_entry())
    assert journal.claim("a" * 32) is not None
    assert journal.claim("a" * 32) is None  # already in inflight/


def test_concurrent_claims_have_exactly_one_winner(journal):
    journal.put(_entry())
    winners = []
    barrier = threading.Barrier(16)

    def _race():
        barrier.wait()
        if journal.claim("a" * 32) is not None:
            winners.append(1)

    threads = [threading.Thread(target=_race) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(winners) == 1


def test_resume_returns_inflight_entries_to_pending(journal):
    journal.put(_entry())
    journal.claim("a" * 32)
    assert journal.list_pending() == []
    assert journal.resume() == 1
    assert len(journal.list_pending()) == 1


def test_reclaim_stale_only_takes_back_old_claims(journal):
    journal.put(_entry())
    journal.claim("a" * 32)
    now = utcnow()
    assert journal.reclaim_stale(now, stale_s=600) == 0
    assert journal.reclaim_stale(now + timedelta(seconds=700), stale_s=600) == 1
    assert len(journal.list_pending()) == 1


def test_prune_honours_retention(journal):
    journal.put(_entry())
    journal.finish(journal.claim("a" * 32), "completed")
    now = utcnow()
    assert journal.prune(now, retention_s=86400) == 0
    assert journal.get("a" * 32) is not None
    assert journal.prune(now + timedelta(days=2), retention_s=86400) == 1
    assert journal.get("a" * 32) is None


def test_latest_for_afdag_prefers_the_newest_entry(journal):
    older = _entry(deploy_id="b" * 32, created_at=iso(utcnow() - timedelta(hours=1)))
    journal.put(older)
    journal.put(_entry(deploy_id="c" * 32))
    assert journal.latest_for_afdag("afd_1")["deploy_id"] == "c" * 32


def test_veto_unpause_skips_unpause_and_trigger(journal):
    journal.put(_entry())
    assert journal.veto_unpause("sales_daily") == 1
    entry = journal.get("a" * 32)
    assert entry["steps"]["unpause"]["state"] == "skipped"
    assert entry["steps"]["trigger"]["state"] == "skipped"
    # A different DAG is untouched.
    assert journal.veto_unpause("other_dag") == 0


def test_supersede_finishes_older_entries_for_the_same_flow(journal):
    journal.put(_entry(deploy_id="b" * 32, retire={"dag_id": "sales_dly", "purge": False}))
    journal.put(_entry(deploy_id="c" * 32))
    superseded = journal.supersede(
        dag_id="sales_daily", afdag_id="afd_1", except_deploy_id="c" * 32
    )
    assert [e["deploy_id"] for e in superseded] == ["b" * 32]
    assert journal.get("b" * 32)["outcome"] == "superseded"
    assert [e["deploy_id"] for e in journal.list_pending()] == ["c" * 32]


def test_put_refuses_a_viewer_entry(journal):
    # The invariant that makes the reconciler's authorization argument valid:
    # only an authorized deploy can mint work, so a viewer entry is forged.
    with pytest.raises(ValueError):
        journal.put(_entry(role_at_deploy="viewer"))


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda e: e.update(dag_id="../../etc/passwd"), id="path-traversal-dag-id"),
        pytest.param(lambda e: e.update(deploy_id="not-a-uuid"), id="bad-deploy-id"),
        pytest.param(lambda e: e.update(filename="other.py"), id="filename-mismatch"),
        pytest.param(lambda e: e.update(phase="rm -rf"), id="bad-phase"),
        pytest.param(lambda e: e.update(outcome="pwned"), id="bad-outcome"),
        pytest.param(lambda e: e.update(deadline_at="tomorrow"), id="bad-timestamp"),
        pytest.param(lambda e: e.update(retire={"dag_id": "a-b", "purge": True}), id="bad-retire-id"),
        # A queued intent names a dag_id that gets interpolated into an Airflow
        # path AND into a `{dag_id}.py` the retire DELETES — checked as strictly
        # as the primary one, and never accepted as a bare string.
        pytest.param(lambda e: e.update(retire_also=[{"dag_id": "../../etc/passwd"}]),
                     id="bad-queued-retire-id"),
        pytest.param(lambda e: e.update(retire_also=["sales_dly"]), id="queued-retire-not-object"),
        pytest.param(lambda e: e.update(retire_also={"dag_id": "sales_dly"}), id="queued-retire-not-list"),
        pytest.param(lambda e: e.update(steps={}), id="missing-steps"),
    ],
)
def test_put_refuses_a_malformed_entry(journal, mutate):
    entry = _entry()
    mutate(entry)
    with pytest.raises(ValueError):
        journal.put(entry)
    assert journal.list_pending() == []


def _write_raw(journal, name, payload):
    path = os.path.join(journal.root, "pending", name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(payload)
    return path


@pytest.mark.parametrize(
    "payload",
    [
        '{"version": 1, "deploy_id"',  # truncated JSON
        "[1, 2, 3]",  # a JSON list, not an entry
        json.dumps({"version": 1, "deploy_id": "z" * 32, "dag_id": "../etc"}),
    ],
)
def test_corrupt_entries_are_quarantined_not_executed(journal, payload):
    _write_raw(journal, f"{'d' * 32}.json", payload)
    assert journal.list_pending() == []
    quarantined = os.listdir(os.path.join(journal.root, "quarantine"))
    assert quarantined  # kept as forensic evidence, never deleted


def test_oversized_entry_is_quarantined(journal):
    _write_raw(journal, f"{'e' * 32}.json", " " * (MAX_ENTRY_BYTES + 1))
    assert journal.list_pending() == []
    assert os.listdir(os.path.join(journal.root, "quarantine"))


def test_a_newer_version_is_left_alone(journal):
    # A downgrade must not destroy the work of a newer server it cannot read.
    entry = _entry(deploy_id="f" * 32)
    entry["version"] = 99
    _write_raw(journal, f"{'f' * 32}.json", json.dumps(entry))
    assert journal.list_pending() == []
    assert os.listdir(os.path.join(journal.root, "quarantine")) == []
    assert os.path.isfile(os.path.join(journal.root, "pending", f"{'f' * 32}.json"))


def test_get_journal_follows_the_env_override(tmp_path, monkeypatch):
    first = tmp_path / "one"
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_JOURNAL_DIR", str(first))
    assert get_journal().root == str(first)
    second = tmp_path / "two"
    monkeypatch.setenv("JUPYTERLAB_AIRFLOW_JOURNAL_DIR", str(second))
    assert get_journal().root == str(second)


# --------------------------------------------------------------------------- #
# Claim ownership: a holder must not resurrect an entry taken away from it.
# --------------------------------------------------------------------------- #
def test_supersede_does_not_resurrect_a_claimed_entry(journal):
    """The reported critical defect, verbatim.

    A sweep claims E1 and blocks inside an Airflow call. Meanwhile the user
    renames the flow and re-deploys, which supersedes E1. Before the claim token,
    the sweep's `release()` wrote its stale copy back over the `done/` record and
    E1 went on to unpause and trigger the dag_id the user had just renamed away
    from — the duplicate-pipeline state the feature exists to prevent.
    """
    deploy_id = journal.put(_entry())
    claimed = journal.claim(deploy_id)
    assert claimed is not None

    superseded = journal.supersede(
        dag_id="sales_v2", afdag_id="afd_1", except_deploy_id="b" * 32
    )
    assert [e["deploy_id"] for e in superseded] == [deploy_id]

    # The holder returns from its Airflow call and releases its stale copy. The
    # release is allowed (its token still matches — nobody wrote over it), but
    # the supersession it could not see is merged forward rather than lost.
    assert journal.release(claimed) is True

    entry = journal.get(deploy_id)
    assert entry["stop_requested"]["outcome"] == "superseded"
    # It is NOT back in pending/ with a clean slate: `reconciler.advance` honours
    # the recorded stop before any transition, so nothing else is ever done to
    # the dag_id the user renamed away from.
    assert [e["deploy_id"] for e in journal.list_pending()] == [deploy_id]


def test_supersede_finishes_an_unclaimed_entry_outright(journal):
    deploy_id = journal.put(_entry())
    assert journal.supersede(
        dag_id="sales_v2", afdag_id="afd_1", except_deploy_id="b" * 32
    )
    assert journal.get(deploy_id)["outcome"] == "superseded"
    assert journal.list_pending() == []


def test_request_stop_on_a_claimed_entry_survives_the_holders_release(journal):
    """The cancel handler's escape hatch. It reported `cancelled: true` while a
    sweep held the entry, and that sweep's release silently undid it."""
    deploy_id = journal.put(_entry())
    claimed = journal.claim(deploy_id)

    assert journal.request_stop(
        deploy_id, outcome="cancelled", message="Cancelled — stopped."
    ) == "requested"

    claimed["phase"] = "unpausing"  # the holder made progress meanwhile
    assert journal.release(claimed) is True  # its own token still matches

    entry = journal.get(deploy_id)
    assert entry["phase"] == "unpausing"  # progress is kept…
    assert entry["stop_requested"] == {
        "outcome": "cancelled",
        "message": "Cancelled — stopped.",
    }  # …and so is the cancellation, for `advance` to honour first.


def test_request_stop_on_an_idle_entry_finishes_it(journal):
    deploy_id = journal.put(_entry())
    assert journal.request_stop(
        deploy_id, outcome="cancelled", message="Cancelled."
    ) == "finished"
    assert journal.get(deploy_id)["outcome"] == "cancelled"
    assert journal.request_stop(deploy_id, outcome="cancelled", message="x") is None


def test_a_stale_holder_cannot_overwrite_a_terminal_entry(journal):
    """Two claims of the same entry (the second after a stale reclaim): only the
    current token may write."""
    deploy_id = journal.put(_entry())
    first = journal.claim(deploy_id)
    journal.release(first)
    second = journal.claim(deploy_id)
    assert first["claim_token"] != second["claim_token"]

    journal.finish(second, "completed")
    assert journal.finish(first, "failed") is False
    assert journal.get(deploy_id)["outcome"] == "completed"


def test_veto_unpause_on_a_claimed_entry_is_recorded_as_a_request(journal):
    deploy_id = journal.put(_entry())
    claimed = journal.claim(deploy_id)

    assert journal.veto_unpause("sales_daily") == 1

    # The holder writes its own (pre-veto) step block back…
    assert journal.release(claimed) is True
    entry = journal.get(deploy_id)
    assert entry["steps"]["unpause"]["state"] == "pending"
    # …but the veto survives as a request that `advance` applies.
    assert entry["veto_unpause_requested"] is True


def test_a_forged_stop_request_is_quarantined(journal):
    entry = _entry(deploy_id="c" * 32)
    entry["stop_requested"] = {"outcome": "rm -rf", "message": "x"}
    _write_raw(journal, f"{'c' * 32}.json", json.dumps(entry))
    assert journal.list_pending() == []
    assert os.listdir(os.path.join(journal.root, "quarantine"))


def test_a_resumed_entry_carries_no_stale_claim(journal):
    """`resume`/`reclaim_stale` move the file wholesale, so an `inflight/` copy's
    claim token would otherwise ride along into `pending/` — and every later
    unclaimed write (a cancel, a supersede, a role-downgrade denial) would be
    refused as if a claim nobody holds were still live."""
    deploy_id = journal.put(_entry())
    journal.claim(deploy_id)
    assert journal.resume() == 1

    assert "claim_token" not in journal.list_pending()[0]
    assert journal.request_stop(
        deploy_id, outcome="cancelled", message="Cancelled."
    ) == "finished"
    assert journal.get(deploy_id)["outcome"] == "cancelled"


# -- retired dag_ids + stranded retire intents (PRD §6.1.8(B)) --------------- #


def test_retired_markers_are_recorded_released_and_ttl_bounded(journal):
    assert journal.retired_ids() == set()
    assert journal.mark_retired("sales_dly") is True
    assert journal.retired_ids() == {"sales_dly"}

    assert journal.clear_retired("sales_dly") is True
    assert journal.retired_ids() == set()

    # Expired markers are pruned where they are read, so nothing can be hidden
    # indefinitely even if every explicit release failed.
    journal.mark_retired("sales_dly", now=utcnow() - timedelta(days=2))
    assert journal.retired_ids() == set()
    assert os.listdir(os.path.join(journal.root, "retired")) == []


def test_a_marker_can_never_name_something_that_is_not_a_dag_id(journal):
    # The marker filename is derived from the id, and the id is compared against
    # what Airflow returns — an unchecked value would be a path-traversal write.
    assert journal.mark_retired("../../etc/passwd") is False
    assert journal.retired_ids() == set()


def _expired_rename(journal, deploy_id, reason="the new DAG never registered"):
    entry = _entry(deploy_id=deploy_id, retire={"dag_id": "sales_dly", "purge": False})
    entry["steps"]["retire"]["state"] = "skipped"
    entry["steps"]["retire"]["skipped_reason"] = reason
    journal.put(entry)
    journal.finish(journal.claim(deploy_id), "expired")
    return entry


def test_an_expired_renames_retire_intent_is_findable_and_consumed_once(journal):
    """`supersede` only scans OPEN entries, so a timed-out rename's intent lived
    in a terminal entry nothing could pick up — the old dag_id stayed live and
    unpaused with no route back. It is inheritable from here instead."""
    _expired_rename(journal, "b" * 32)

    stranded = journal.orphaned_retires(afdag_id="afd_1", dag_id="sales_daily")
    assert [e["deploy_id"] for e in stranded] == ["b" * 32]

    assert journal.consume_orphaned_retire("b" * 32) is True
    # Exactly once: a second deploy must not retire it again, and "Keep waiting"
    # must not re-arm a step a newer deploy now owns.
    assert journal.orphaned_retires(afdag_id="afd_1", dag_id="sales_daily") == []
    assert journal.consume_orphaned_retire("b" * 32) is False


@pytest.mark.parametrize(
    "reason",
    ["the new DAG failed to import", "file now owned by another flow",
     "paused from Studio while the deploy was still in flight"],
)
def test_only_a_timed_out_retire_is_inheritable(journal, reason):
    """Every other skip was a *decision*. Re-running it later is not "finishing
    the deploy", it is overriding the answer."""
    _expired_rename(journal, "c" * 32, reason=reason)
    assert journal.orphaned_retires(afdag_id="afd_1", dag_id="sales_daily") == []


def test_a_stale_stranded_intent_is_not_inherited(journal):
    _expired_rename(journal, "d" * 32)
    later = utcnow() + timedelta(days=3)
    assert journal.orphaned_retires(
        afdag_id="afd_1", dag_id="sales_daily", now=later
    ) == []


def test_a_consumed_request_is_not_merged_back_forever(journal):
    """The holder marks a request consumed rather than deleting it, so releasing
    does not re-import it from the on-disk copy on every single sweep."""
    deploy_id = journal.put(_entry())
    claimed = journal.claim(deploy_id)
    journal.veto_unpause("sales_daily")
    journal.release(claimed)  # merges the request forward

    consumer = journal.claim(deploy_id)
    assert consumer["veto_unpause_requested"] is True
    consumer["veto_unpause_requested"] = False  # what `advance` does after applying
    journal.release(consumer)

    assert journal.get(deploy_id)["veto_unpause_requested"] is False
