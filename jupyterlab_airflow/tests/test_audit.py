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


# --------------------------------------------------------------------------- #
# The trail must actually be written (PRD §9).
#
# It used to be inert: records were emitted correctly, but the logger sits at
# NOTSET under a root logger Jupyter leaves at WARNING with no handlers, so
# every INFO record was discarded. The existing tests could not catch it —
# their fixture attaches its own handler, which is exactly what production
# lacked. These assert the default path end to end instead.
# --------------------------------------------------------------------------- #
import json as _json
import logging as _logging
import os as _os

import pytest as _pytest

from jupyterlab_airflow import audit as _audit


@_pytest.fixture
def clean_audit_logger():
    """Restore the real logger — these tests mutate it on purpose."""
    logger = _logging.getLogger(_audit.AUDIT_LOGGER_NAME)
    handlers, level = list(logger.handlers), logger.level
    for handler in handlers:
        logger.removeHandler(handler)
    logger.setLevel(_logging.NOTSET)
    yield logger
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    for handler in handlers:
        logger.addHandler(handler)
    logger.setLevel(level)


class _App:
    def __init__(self, data_dir):
        self.data_dir = str(data_dir)
        self.log = _logging.getLogger("test.serverapp")


def test_an_unconfigured_logger_discards_every_record(clean_audit_logger, tmp_path,
                                                      monkeypatch):
    """The exact production trap, pinned so it cannot come back."""
    monkeypatch.delenv(_audit.ENV_AUDIT_LOG, raising=False)
    assert clean_audit_logger.getEffectiveLevel() > _logging.INFO
    # Without configure(), the record passes through audit_event and lands
    # nowhere at all.
    assert clean_audit_logger.handlers == []


def test_configure_writes_records_to_a_file(clean_audit_logger, tmp_path, monkeypatch):
    monkeypatch.delenv(_audit.ENV_AUDIT_LOG, raising=False)
    path = _audit.configure(_App(tmp_path))

    assert path and path.startswith(str(tmp_path))
    _audit.audit_event("deploy", user="alice", correlation_id="abc", dag_id="etl")

    lines = [line for line in open(path, encoding="utf-8").read().splitlines() if line]
    assert len(lines) == 1
    # One complete JSON object per line — the property a SIEM depends on.
    record = _json.loads(lines[0])
    assert record["action"] == "deploy" and record["user"] == "alice"
    assert record["dag_id"] == "etl" and record["via"] == "request"


def test_an_explicit_path_is_honoured(clean_audit_logger, tmp_path, monkeypatch):
    target = tmp_path / "nested" / "trail.log"
    monkeypatch.setenv(_audit.ENV_AUDIT_LOG, str(target))

    assert _audit.configure(_App(tmp_path)) == str(target)
    _audit.audit_event("retire", user="bob", correlation_id="c", dag_id="old")
    assert _json.loads(open(target, encoding="utf-8").read().strip())["user"] == "bob"


def test_off_writes_no_file(clean_audit_logger, tmp_path, monkeypatch):
    monkeypatch.setenv(_audit.ENV_AUDIT_LOG, "off")
    assert _audit.configure(_App(tmp_path)) is None
    assert clean_audit_logger.handlers == []


def test_an_operators_own_handler_is_never_duplicated(clean_audit_logger, tmp_path,
                                                      monkeypatch):
    """A deployment routing the trail to its own sink keeps sole ownership."""
    monkeypatch.delenv(_audit.ENV_AUDIT_LOG, raising=False)
    mine = _logging.StreamHandler()
    clean_audit_logger.addHandler(mine)

    assert _audit.configure(_App(tmp_path)) is None
    assert clean_audit_logger.handlers == [mine]
    # ...but the level is still lifted, or their handler would receive nothing.
    assert clean_audit_logger.getEffectiveLevel() <= _logging.INFO


def test_an_explicit_level_is_respected(clean_audit_logger, tmp_path, monkeypatch):
    monkeypatch.delenv(_audit.ENV_AUDIT_LOG, raising=False)
    clean_audit_logger.setLevel(_logging.CRITICAL)  # a deliberate silencing
    _audit.configure(_App(tmp_path))
    assert clean_audit_logger.level == _logging.CRITICAL


def test_configure_is_idempotent(clean_audit_logger, tmp_path, monkeypatch):
    monkeypatch.delenv(_audit.ENV_AUDIT_LOG, raising=False)
    first = _audit.configure(_App(tmp_path))
    second = _audit.configure(_App(tmp_path))
    assert first == second
    assert len(clean_audit_logger.handlers) == 1


def test_an_unwritable_path_warns_but_does_not_raise(clean_audit_logger, tmp_path,
                                                     monkeypatch):
    blocker = tmp_path / "blocked"
    blocker.write_text("not a directory")
    monkeypatch.setenv(_audit.ENV_AUDIT_LOG, str(blocker / "audit.log"))
    # Server start must survive an unwritable audit path.
    assert _audit.configure(_App(tmp_path)) is None


def test_the_file_is_not_world_readable(clean_audit_logger, tmp_path, monkeypatch):
    """It names who did what — it should not be readable by every local user."""
    monkeypatch.delenv(_audit.ENV_AUDIT_LOG, raising=False)
    path = _audit.configure(_App(tmp_path))
    _audit.audit_event("delete", user="carol", correlation_id="d", dag_id="gone")
    if _os.name != "nt":  # POSIX permission bits only
        assert _os.stat(path).st_mode & 0o077 == 0
