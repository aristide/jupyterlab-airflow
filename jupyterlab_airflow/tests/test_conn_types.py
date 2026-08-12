"""SMTP/TLS connection-extra hints, presets and validation (PRD §6.13)."""

import json

import pytest

from jupyterlab_airflow import conn_types
from jupyterlab_airflow import connections as c


def _ir(conn_type, extra):
    return {
        "dag": {"dag_id": "f"},
        "nodes": [],
        "edges": [],
        "connections": [
            {
                "conn_id": "mail",
                "scope": "local",
                "conn_type": conn_type,
                "extra": json.dumps(extra) if extra is not None else "",
            }
        ],
    }


# -- registry --------------------------------------------------------------


def test_smtp_is_a_known_type_and_others_are_not():
    hints = conn_types.hints_for("smtp")
    assert hints and hints["conn_type"] == "smtp"
    assert conn_types.hints_for("postgres") is None
    assert conn_types.hints_for(None) is None
    assert conn_types.hints_for("") is None


def test_conn_type_lookup_is_case_and_space_insensitive():
    assert conn_types.hints_for("  SMTP ") is conn_types.hints_for("smtp")


def test_known_keys_cover_the_hooks_extra_fields():
    """Mirrors SmtpHook.get_connection_form_widgets() on the 3.0.2 provider."""
    assert set(conn_types.known_keys("smtp")) == {
        "from_email", "timeout", "retry_limit", "disable_tls", "disable_ssl",
        "ssl_context", "subject_template", "html_content_template",
    }
    assert conn_types.known_keys("postgres") == []


def test_presets_cover_the_three_real_setups():
    by_id = {p["id"]: p for p in conn_types.SMTP_PRESETS}
    # STARTTLS needs the counterintuitive disable_ssl, because `use_ssl` is
    # `not disable_ssl` and wins over starttls in the hook.
    assert by_id["starttls"]["extra"] == {"disable_ssl": True}
    assert by_id["starttls"]["port"] == 587
    # Implicit SSL is the hook's own default, so the preset clears nothing.
    assert by_id["ssl"]["extra"] == {} and by_id["ssl"]["port"] == 465
    # No encryption at all needs both off.
    assert by_id["plain"]["extra"] == {"disable_ssl": True, "disable_tls": True}


# -- hard errors -----------------------------------------------------------


@pytest.mark.parametrize("bad", ["strict", "DEFAULT", "verify", "true"])
def test_invalid_ssl_context_is_an_error(bad):
    """The hook raises RuntimeError for anything but default/none — and it does
    so when the mail is sent, long after the DAG parsed."""
    errors = c.declaration_errors(_ir("smtp", {"ssl_context": bad}))
    assert errors and "ssl_context" in errors[0] and "'default' or 'none'" in errors[0]


@pytest.mark.parametrize("good", ["default", "none"])
def test_valid_ssl_context_passes(good):
    assert c.declaration_errors(_ir("smtp", {"ssl_context": good})) == []


def test_absent_ssl_context_is_fine():
    assert c.declaration_errors(_ir("smtp", {"disable_ssl": True})) == []
    assert c.declaration_errors(_ir("smtp", {})) == []


def test_unknown_conn_type_is_never_validated():
    """A type the registry doesn't curate must not be second-guessed."""
    assert conn_types.extra_errors("postgres", {"ssl_context": "nonsense"}) == []
    assert c.declaration_errors(_ir("postgres", {"anything": "goes"})) == []


# -- advisory hints --------------------------------------------------------


def test_default_extra_warns_about_the_implicit_ssl_trap():
    notes = conn_types.extra_hints("smtp", {})
    assert len(notes) == 1
    assert "disable_ssl" in notes[0] and "587" in notes[0]


def test_no_trap_warning_once_disable_ssl_is_set():
    assert conn_types.extra_hints("smtp", {"disable_ssl": True}) == []


def test_unknown_key_is_a_hint_not_an_error():
    extra = {"disable_ssl": True, "disble_tls": True}
    notes = conn_types.extra_hints("smtp", extra)
    assert any("disble_tls" in note for note in notes)
    # ...and it never blocks a deploy.
    assert c.declaration_errors(_ir("smtp", extra)) == []


def test_hints_are_empty_for_an_unknown_type():
    assert conn_types.extra_hints("postgres", {"whatever": 1}) == []


# -- surfaced through the tab payload --------------------------------------


class _FakeClient:
    def list_connections(self, limit=1000, offset=0):
        return {"connections": [], "total_entries": 0}


def test_annotated_serves_type_hints_and_per_connection_notes():
    out = c.annotated(_ir("smtp", {}), _FakeClient())
    assert "smtp" in out["type_hints"]
    assert out["type_hints"]["smtp"]["presets"]
    assert out["connections"][0]["hints"], "the implicit-SSL note should surface"


def test_annotated_omits_hints_for_untracked_types():
    out = c.annotated(_ir("postgres", {"sslmode": "require"}), _FakeClient())
    assert out["type_hints"] == {}
    assert out["connections"][0]["hints"] == []


def test_unparseable_extra_does_not_break_the_payload():
    ir = _ir("smtp", None)
    ir["connections"][0]["extra"] = "{not json"
    out = c.annotated(ir, _FakeClient())
    assert out["connections"][0]["hints"] == [] or isinstance(
        out["connections"][0]["hints"], list
    )
