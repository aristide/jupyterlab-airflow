"""Well-known connection types and the ``extra`` keys they understand (PRD §6.13).

An Airflow connection's ``extra`` is an untyped JSON blob whose meaning is
defined entirely by the provider hook that reads it. In the Airflow UI each
provider contributes form widgets for its own keys; over the REST API — which is
all Studio has — that metadata is not exposed, so a user editing the CONNECTIONS
tab is left guessing key names that fail silently when wrong.

This is a small, deliberately **curated** registry of that metadata for the
types Studio's own operators actually steer people towards. It starts with
**SMTP**, because the email operator has *no* TLS parameters of its own — TLS is
configured entirely on the connection — and because the SMTP hook's defaults are
genuinely counterintuitive (see below).

Each entry is data only, served to the editor, so adding a type is a data change.
The registry never claims to be exhaustive: an unknown key is a *hint*, never an
error, because a provider may legitimately read keys we don't know about.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

# Verified against apache-airflow-providers-smtp on Airflow 3.0.2 by reading
# SmtpHook's properties directly:
#
#     smtp_starttls = not extra.get("disable_tls", False)
#     use_ssl       = not extra.get("disable_ssl", False)
#
# Both encryption modes are therefore **ON by default**, and `use_ssl` wins:
# `_build_client` picks `smtplib.SMTP_SSL` whenever `use_ssl` is true, and only
# calls `starttls()` on the plain-SMTP branch. That is the trap this registry
# exists to defuse — the common STARTTLS-on-587 setup needs the
# counterintuitive `{"disable_ssl": true}`, and without it the connection
# attempts implicit SSL against a STARTTLS port and simply hangs.
SMTP_EXTRA_FIELDS: List[Dict[str, Any]] = [
    {
        "key": "disable_ssl",
        "type": "boolean",
        "default": False,
        "label": "Disable implicit SSL",
        "help": (
            "Leave off (the default) for implicit SSL/TLS, the usual choice on "
            "port 465. Turn ON for a STARTTLS server on port 587 — the "
            "connection then starts in plain text and upgrades."
        ),
    },
    {
        "key": "disable_tls",
        "type": "boolean",
        "default": False,
        "label": "Disable STARTTLS",
        "help": (
            "Only applies when implicit SSL is disabled. Leave off so the "
            "connection upgrades to TLS with STARTTLS; turn ON only for a "
            "server with no encryption at all (local/dev)."
        ),
    },
    {
        "key": "ssl_context",
        "type": "string",
        "enum": ["default", "none"],
        "label": "SSL context",
        "help": (
            "Certificate verification for implicit SSL: 'default' verifies (the "
            "behaviour when unset), 'none' skips verification — for a "
            "self-signed server, and never against the public internet. Any "
            "other value makes the send fail at run time."
        ),
    },
    {
        "key": "timeout",
        "type": "integer",
        "default": 30,
        "label": "Connect timeout (s)",
        "help": "Seconds to wait for the SMTP connection. Defaults to 30.",
    },
    {
        "key": "retry_limit",
        "type": "integer",
        "default": 5,
        "label": "Connection retries",
        "help": "How many times to retry connecting. Defaults to 5.",
    },
    {
        "key": "from_email",
        "type": "string",
        "label": "From address",
        "help": (
            "Default sender for mail sent through this connection, used when a "
            "task does not set one."
        ),
    },
    {
        "key": "subject_template",
        "type": "string",
        "label": "Subject template path",
        "help": "Path to a file holding a default subject template.",
    },
    {
        "key": "html_content_template",
        "type": "string",
        "label": "HTML body template path",
        "help": "Path to a file holding a default HTML body template.",
    },
]

# One-click starting points for the three real-world SMTP setups. `extra` is the
# complete blob to write, so applying a preset is unambiguous.
SMTP_PRESETS: List[Dict[str, Any]] = [
    {
        "id": "starttls",
        "label": "STARTTLS — port 587",
        "help": (
            "The most common setup (Gmail, Office 365, most corporate relays). "
            "Starts plain and upgrades to TLS."
        ),
        "port": 587,
        "extra": {"disable_ssl": True},
    },
    {
        "id": "ssl",
        "label": "Implicit SSL/TLS — port 465",
        "help": "Encrypted from the first byte. The hook's default behaviour.",
        "port": 465,
        "extra": {},
    },
    {
        "id": "plain",
        "label": "No encryption — port 25 (dev only)",
        "help": (
            "For a local test relay such as MailHog. Credentials and message "
            "content travel in clear text — never use this off your machine."
        ),
        "port": 25,
        "extra": {"disable_ssl": True, "disable_tls": True},
    },
]

CONN_TYPES: Dict[str, Dict[str, Any]] = {
    "smtp": {
        "conn_type": "smtp",
        "label": "SMTP (email)",
        "intro": (
            "TLS for email is configured here, not on the task — the email "
            "operator has no TLS settings of its own. Both encryption modes are "
            "on by default, and implicit SSL takes precedence, so a STARTTLS "
            "server needs “Disable implicit SSL” turned on."
        ),
        "fields": SMTP_EXTRA_FIELDS,
        "presets": SMTP_PRESETS,
    }
}


def hints_for(conn_type: Optional[str]) -> Optional[Dict[str, Any]]:
    """The curated metadata for a connection type, or ``None`` when unknown."""
    if not conn_type:
        return None
    return CONN_TYPES.get(str(conn_type).strip().lower())


def known_keys(conn_type: Optional[str]) -> List[str]:
    hints = hints_for(conn_type)
    return [field["key"] for field in hints["fields"]] if hints else []


def extra_errors(conn_type: Optional[str], extra: Any) -> List[str]:
    """Hard errors in a known type's ``extra`` — values the provider *rejects*.

    Only genuine breakage belongs here. Today that is ``ssl_context``: the SMTP
    hook raises ``RuntimeError`` for anything but ``default``/``none``, and it
    does so when the mail is sent, not when the DAG is parsed — so the failure
    lands in a task log long after the typo.
    """
    hints = hints_for(conn_type)
    if not hints or not isinstance(extra, dict):
        return []
    errors: List[str] = []
    for field in hints["fields"]:
        if "enum" not in field:
            continue
        value = extra.get(field["key"])
        if value in (None, ""):
            continue
        if str(value) not in field["enum"]:
            allowed = " or ".join(repr(v) for v in field["enum"])
            errors.append(
                f"{field['key']} must be {allowed}, not {value!r} — the "
                "connection fails when it is used, not when the DAG is parsed."
            )
    return errors


def extra_hints(conn_type: Optional[str], extra: Any) -> List[str]:
    """Advisory notes: keys that are not in this type's known set, and the
    counterintuitive-default warning. Never errors — a provider may read keys
    this curated registry does not list."""
    hints = hints_for(conn_type)
    if not hints or not isinstance(extra, dict):
        return []
    notes: List[str] = []
    known = set(known_keys(conn_type))
    unknown = sorted(key for key in extra if key not in known)
    if unknown:
        notes.append(
            f"{hints['label']} connections don't use "
            f"{', '.join(repr(k) for k in unknown)}. Check the spelling — an "
            "unrecognised key is ignored."
        )
    if conn_type == "smtp" and not extra.get("disable_ssl"):
        notes.append(
            "Implicit SSL is on (the default), so this connects with SMTP_SSL. "
            "If your server is STARTTLS on port 587, set disable_ssl to true — "
            "otherwise the connection hangs."
        )
    return notes
