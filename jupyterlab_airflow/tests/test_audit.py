"""Tests for the audit trail (PRD §9)."""

import json
import logging

from jupyterlab_airflow.audit import ACTIONS, AUDIT_LOGGER_NAME, audit_event


def _capture():
    records = []

    class _H(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    logger = logging.getLogger(AUDIT_LOGGER_NAME)
    handler = _H()
    logger.addHandler(handler)
    prev = logger.level
    logger.setLevel(logging.INFO)
    return records, logger, handler, prev


def test_audit_event_record_shape_and_emission():
    records, logger, handler, prev = _capture()
    try:
        rec = audit_event("deploy", user="alice", correlation_id="c1", dag_id="etl")
        assert rec["action"] == "deploy"
        assert rec["user"] == "alice"
        assert rec["dag_id"] == "etl"
        assert rec["correlation_id"] == "c1"
        assert rec["outcome"] == "ok"
        assert "ts" in rec
        # Emitted as a single JSON line equal to the returned record.
        assert json.loads(records[-1]) == rec
        # "deploy" is a recognised action.
        assert "deploy" in ACTIONS
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev)


def test_audit_event_error_detail_trimmed_and_injection_safe():
    records, logger, handler, prev = _capture()
    try:
        audit_event(
            "delete",
            user="bob\nFAKE  action=deploy  user=admin",  # attempted log injection
            correlation_id="c2",
            dag_id="d\nx",
            outcome="error",
            detail="x" * 1000,
        )
        line = records[-1]
        # One JSON line — embedded newlines are escaped, so a crafted user/dag_id
        # cannot forge a second audit record.
        assert "\n" not in line
        emitted = json.loads(line)
        assert emitted["outcome"] == "error"
        assert len(emitted["detail"]) == 500  # trimmed
        assert emitted["user"] == "bob\nFAKE  action=deploy  user=admin"  # preserved
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev)


def test_audit_event_defaults_anonymous_user():
    records, logger, handler, prev = _capture()
    try:
        rec = audit_event("trigger", user="", correlation_id="c3", dag_id="d")
        assert rec["user"] == "anonymous"
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev)


import asyncio  # noqa: E402

from jupyterlab_airflow import handlers as handlers_module  # noqa: E402
from jupyterlab_airflow.handlers import _AirflowHandler  # noqa: E402


class _FakeHandler(_AirflowHandler):
    """A minimal stand-in that exercises respond() without Tornado machinery.
    ``log`` is a class attribute so it shadows the parent's read-only property."""

    log = logging.getLogger("test.fake")

    def __init__(self):
        self.current_user = "tester"
        self.status = 200
        self.finished = None

    async def run(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def set_status(self, code):
        self.status = code

    def finish(self, body):
        self.finished = body


def test_audit_emission_failure_does_not_break_a_successful_request(monkeypatch):
    # A custom/SIEM audit logging handler that raises must NOT turn a succeeded
    # action into a 500, nor double-record (review finding): audit is best-effort.
    calls = []

    def _boom(*args, **kwargs):
        calls.append(kwargs.get("outcome"))
        raise RuntimeError("SIEM ship failed")

    monkeypatch.setattr(handlers_module, "audit_event", _boom)
    h = _FakeHandler()
    asyncio.get_event_loop().run_until_complete(
        h.respond(lambda: {"dag_id": "demo", "state": "queued"}, audit_action="trigger")
    )
    # The successful action still returns 200 with its data — not a 500.
    assert h.status == 200
    assert json.loads(h.finished)["data"]["state"] == "queued"
    # audit_event was attempted exactly once (no re-fire from the error path).
    assert calls == ["ok"]
