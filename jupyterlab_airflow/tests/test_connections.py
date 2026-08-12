"""Connections: scopes, ownership, registry-driven usage, gates, lifecycle (§6.11)."""

import pytest

from jupyterlab_airflow import connections as c
from jupyterlab_airflow.client import AirflowError


class _FakeClient:
    def __init__(self, conns=None, unreachable=False):
        self.conns = {
            entry["connection_id"]: dict(entry) for entry in (conns or [])
        }
        self.unreachable = unreachable
        self.created = []
        self.updated = []
        self.deleted = []

    def list_connections(self, limit=1000, offset=0):
        if self.unreachable:
            raise AirflowError("down", status=0)
        return {
            "connections": list(self.conns.values()),
            "total_entries": len(self.conns),
        }

    def get_connection(self, conn_id):
        if conn_id not in self.conns:
            raise AirflowError("not found", status=404)
        return self.conns[conn_id]

    def create_connection(self, conn_id, conn_type, **fields):
        if conn_id in self.conns:
            raise AirflowError("exists", status=409)
        entry = {"connection_id": conn_id, "conn_type": conn_type}
        entry.update({k: v for k, v in fields.items() if v not in (None, "")})
        self.conns[conn_id] = entry
        self.created.append(conn_id)
        return entry

    def update_connection(self, conn_id, conn_type, **fields):
        entry = self.conns.setdefault(conn_id, {"connection_id": conn_id})
        entry["conn_type"] = conn_type
        entry.update({k: v for k, v in fields.items() if v not in (None, "")})
        self.updated.append(conn_id)
        return entry

    def delete_connection(self, conn_id):
        if conn_id not in self.conns:
            raise AirflowError("not found", status=404)
        del self.conns[conn_id]
        self.deleted.append(conn_id)
        return {}


def _ir(conns=None, nodes=None, dag_id="my_flow"):
    ir = {"dag": {"dag_id": dag_id}, "nodes": nodes or [], "edges": []}
    if conns is not None:
        ir["connections"] = conns
    return ir


def _remote(conn_id, owner=None, **extra):
    entry = {"connection_id": conn_id, "conn_type": "postgres", **extra}
    if owner:
        entry["description"] = c.compose_description(owner)
    return entry


# -- param detection -------------------------------------------------------


@pytest.mark.parametrize(
    "name,expected",
    [
        ("conn_id", True),
        ("aws_conn_id", True),
        ("slack_webhook_conn_id", True),
        ("google_cloud_conn_id", True),
        ("endpoint", False),
        ("conn_type", False),
        ("connections", False),
        ("", False),
    ],
)
def test_is_conn_param(name, expected):
    assert c.is_conn_param(name) is expected


def test_registry_supplies_conn_params_and_defaults():
    """Usage detection is registry-driven, so a new operator YAML is covered
    with no change here."""
    index = c._conn_param_defaults()
    assert index["http"] == {"http_conn_id": "http_default"}
    assert index["s3_create_object"] == {"aws_conn_id": "aws_default"}
    assert "bash" not in index  # no connection params


# -- usage scanning --------------------------------------------------------


def test_explicit_param_value_is_used():
    ir = _ir(nodes=[{"task_id": "t", "op": "http", "params": {"http_conn_id": "mine"}}])
    assert c.used_ids(ir) == {"mine"}
    assert c.usages(ir)[0]["implicit"] is False


def test_blank_param_falls_back_to_the_registry_default():
    """Leaving the field blank does not mean 'no connection' — the operator
    still uses its default, and the task fails if that is absent."""
    ir = _ir(nodes=[{"task_id": "t", "op": "http", "params": {}}])
    use = c.usages(ir)[0]
    assert use["conn_id"] == "http_default"
    assert use["implicit"] is True


def test_explicit_value_overrides_the_default_and_is_not_implicit():
    ir = _ir(
        nodes=[{"task_id": "t", "op": "s3_create_object",
                "params": {"aws_conn_id": "custom_aws"}}]
    )
    assert c.used_ids(ir) == {"custom_aws"}
    assert c.usages(ir)[0]["implicit"] is False


def test_code_body_and_jinja_references_are_found():
    ir = _ir(
        nodes=[
            {"task_id": "code", "op": "python_task", "params": {},
             "code": "from airflow.hooks.base import BaseHook\n"
                     "c = BaseHook.get_connection('from_code')"},
            {"task_id": "tmpl", "op": "bash",
             "params": {"bash_command": "echo {{ conn.from_jinja.host }}"}},
        ]
    )
    assert c.used_ids(ir) == {"from_code", "from_jinja"}


def test_conn_get_form_is_not_mistaken_for_a_conn_named_get():
    ir = _ir(nodes=[{"task_id": "t", "op": "bash",
                     "params": {"bash_command": "{{ conn.get('real-id') }}"}}])
    assert c.used_ids(ir) == {"real-id"}


def test_references_label_the_param_and_flag_a_default():
    ir = _ir(
        nodes=[
            {"task_id": "a", "op": "http", "params": {"http_conn_id": "x"}},
            {"task_id": "b", "op": "http", "params": {}},
        ]
    )
    refs = c.references(ir)
    assert refs["x"] == ["task 'a' (http_conn_id)"]
    assert refs["http_default"] == ["task 'b' (http_conn_id) — default"]


def test_blocking_usage_and_unused():
    ir = _ir(
        [{"conn_id": "used", "scope": "remote"}, {"conn_id": "spare", "scope": "remote"}],
        [{"task_id": "t", "op": "http", "params": {"http_conn_id": "used"}}],
    )
    assert c.blocking_usage(ir, "used") == ["task 't' (http_conn_id)"]
    assert c.blocking_usage(ir, "spare") == []
    assert "spare" in c.unused_declarations(ir)


# -- back-compatibility ----------------------------------------------------


def test_flow_without_a_connections_key_is_inert():
    """The decisive back-compat guarantee: a `.afdag` predating §6.11 has
    conn_ids and no declarations, and must keep deploying unchanged."""
    legacy = _ir(nodes=[{"task_id": "t", "op": "http",
                         "params": {"http_conn_id": "legacy"}}])
    assert "connections" not in legacy
    assert c.declared(legacy) == []
    assert c.declaration_errors(legacy) == []
    assert c.block_errors(legacy, _FakeClient()) == []


def test_undeclared_is_reported_but_never_blocks():
    ir = _ir([], [{"task_id": "t", "op": "http", "params": {"http_conn_id": "ghost"}}])
    pending = c.undeclared(ir)
    assert [entry["conn_id"] for entry in pending] == ["ghost"]
    assert pending[0]["implicit"] is False
    # ...and it is NOT an error anywhere that could refuse a deploy.
    assert c.declaration_errors(ir) == []
    assert c.block_errors(ir, _FakeClient()) == []


def test_declared_connection_is_not_reported_as_undeclared():
    ir = _ir(
        [{"conn_id": "known", "scope": "remote"}],
        [{"task_id": "t", "op": "http", "params": {"http_conn_id": "known"}}],
    )
    assert c.undeclared(ir) == []


def test_deploy_warnings_only_for_ids_absent_from_airflow():
    ir = _ir([], [
        {"task_id": "a", "op": "http", "params": {"http_conn_id": "there"}},
        {"task_id": "b", "op": "http", "params": {"http_conn_id": "missing"}},
    ])
    client = _FakeClient([_remote("there")])
    warnings = c.deploy_warnings(ir, client)
    assert len(warnings) == 1
    assert "missing" in warnings[0] and "there" not in warnings[0]


# -- declaration validation ------------------------------------------------


def test_local_connection_requires_a_type():
    ir = _ir([{"conn_id": "c1", "scope": "local"}])
    assert "connection type" in c.declaration_errors(ir)[0]


def test_remote_connection_needs_no_type():
    assert c.declaration_errors(_ir([{"conn_id": "c1", "scope": "remote"}])) == []


def test_bad_port_and_extra_are_rejected():
    ir = _ir([
        {"conn_id": "p", "scope": "local", "conn_type": "postgres", "port": "abc"},
        {"conn_id": "e", "scope": "local", "conn_type": "http", "extra": "nope"},
    ])
    errors = " ".join(c.declaration_errors(ir))
    assert "non-numeric port" in errors and "not valid JSON" in errors


def test_extra_must_be_a_json_object_not_an_array():
    """Airflow answers a plain-text 500 for a non-object `extra`, so this has to
    be caught before the request."""
    ir = _ir([{"conn_id": "e", "scope": "local", "conn_type": "http",
               "extra": "[1, 2, 3]"}])
    assert "not an object" in c.declaration_errors(ir)[0]


@pytest.mark.parametrize("bad", ["with space", "with/slash", "with:colon", "with#hash"])
def test_conn_id_outside_airflows_pattern_is_rejected(bad):
    ir = _ir([{"conn_id": bad, "scope": "remote"}])
    assert "not valid" in c.declaration_errors(ir)[0]


@pytest.mark.parametrize("ok", ["with.dot", "with-dash", "with_under", "UPPER123"])
def test_conn_id_characters_airflow_allows(ok):
    assert c.declaration_errors(_ir([{"conn_id": ok, "scope": "remote"}])) == []


def test_over_long_conn_id_is_rejected():
    ir = _ir([{"conn_id": "c" * 201, "scope": "remote"}])
    assert "at most 200" in c.declaration_errors(ir)[0]


# -- deploy gates ----------------------------------------------------------


def test_missing_remote_connection_blocks():
    ir = _ir([{"conn_id": "shared", "scope": "remote"}])
    errors = c.block_errors(ir, _FakeClient())
    assert len(errors) == 1 and "no longer exists" in errors[0]


def test_existing_remote_connection_passes():
    ir = _ir([{"conn_id": "shared", "scope": "remote"}])
    assert c.block_errors(ir, _FakeClient([_remote("shared")])) == []


def test_local_id_owned_by_another_flow_blocks():
    ir = _ir([{"conn_id": "db", "scope": "local", "conn_type": "postgres"}])
    client = _FakeClient([_remote("db", owner="other_flow")])
    errors = c.block_errors(ir, client)
    assert len(errors) == 1 and "other_flow" in errors[0]


def test_local_id_created_outside_studio_blocks():
    ir = _ir([{"conn_id": "db", "scope": "local", "conn_type": "postgres"}])
    client = _FakeClient([_remote("db", description="by hand")])
    assert "outside Studio" in c.block_errors(ir, client)[0]


def test_reclaiming_our_own_connection_passes():
    ir = _ir([{"conn_id": "db", "scope": "local", "conn_type": "postgres"}])
    client = _FakeClient([_remote("db", owner="my_flow")])
    assert c.block_errors(ir, client) == []


def test_gates_are_empty_when_airflow_is_unreachable():
    ir = _ir([{"conn_id": "shared", "scope": "remote"}])
    assert c.block_errors(ir, _FakeClient(unreachable=True)) == []
    assert c.deploy_warnings(ir, _FakeClient(unreachable=True)) == []


# -- sync / purge ----------------------------------------------------------


def test_sync_creates_local_connections_with_the_ownership_marker():
    ir = _ir([{"conn_id": "db", "scope": "local", "conn_type": "postgres",
               "host": "h", "port": "5432", "description": "warehouse"}])
    client = _FakeClient()
    synced, warnings = c.sync(ir, client)
    assert synced == ["db"] and warnings == []
    stored = client.conns["db"]
    assert stored["conn_type"] == "postgres" and stored["host"] == "h"
    assert c.owner_of(stored["description"]) == "my_flow"
    assert c.strip_marker(stored["description"]) == "warehouse"


def test_sync_updates_a_connection_the_flow_owns():
    client = _FakeClient([_remote("db", owner="my_flow", host="old")])
    ir = _ir([{"conn_id": "db", "scope": "local", "conn_type": "postgres",
               "host": "new"}])
    synced, warnings = c.sync(ir, client)
    assert synced == ["db"] and warnings == [] and client.created == []
    assert client.conns["db"]["host"] == "new"


def test_sync_never_touches_remote_connections():
    ir = _ir([{"conn_id": "shared", "scope": "remote"}])
    client = _FakeClient()
    assert c.sync(ir, client) == ([], [])
    assert client.conns == {}


def test_purge_removes_only_what_this_flow_owns():
    client = _FakeClient([
        _remote("mine", owner="my_flow"),
        _remote("theirs", owner="other_flow"),
        _remote("manual", description="set by an operator"),
    ])
    assert c.purge("my_flow", client) == ["mine"]
    assert set(client.conns) == {"theirs", "manual"}


# -- single-connection CRUD guards -----------------------------------------


def test_set_one_creates_then_updates():
    client = _FakeClient()
    assert c.set_one("my_flow", "db", "postgres", {"host": "h"}, client)["created"]
    assert not c.set_one("my_flow", "db", "postgres", {"host": "h2"}, client)["created"]
    assert client.conns["db"]["host"] == "h2"


def test_set_one_refuses_a_connection_owned_by_another_flow():
    client = _FakeClient([_remote("db", owner="other_flow", host="theirs")])
    with pytest.raises(c.ConnectionOwnershipError, match="other_flow"):
        c.set_one("my_flow", "db", "postgres", {"host": "mine"}, client)
    assert client.conns["db"]["host"] == "theirs"


def test_set_one_requires_a_type():
    with pytest.raises(c.ConnectionOwnershipError, match="type"):
        c.set_one("my_flow", "db", "", {}, _FakeClient())


def test_delete_one_refuses_what_the_flow_does_not_own():
    client = _FakeClient([_remote("db", description="by hand")])
    with pytest.raises(c.ConnectionOwnershipError):
        c.delete_one("my_flow", "db", client)
    assert "db" in client.conns


def test_delete_one_of_a_missing_connection_is_not_an_error():
    assert c.delete_one("my_flow", "gone", _FakeClient())["deleted"] is False


# -- annotated payload -----------------------------------------------------


def test_annotated_reports_usage_existence_and_ownership():
    ir = _ir(
        [{"conn_id": "db", "scope": "local", "conn_type": "postgres"},
         {"conn_id": "shared", "scope": "remote"}],
        [{"task_id": "t", "op": "http", "params": {"http_conn_id": "db"}}],
    )
    client = _FakeClient([_remote("db", owner="my_flow"), _remote("other")])
    out = c.annotated(ir, client)
    by_id = {e["conn_id"]: e for e in out["connections"]}
    assert by_id["db"]["used_by"] == ["task 't' (http_conn_id)"]
    assert by_id["db"]["exists"] and by_id["db"]["owned"]
    assert not by_id["shared"]["exists"]
    assert [e["conn_id"] for e in out["available"] if not e["declared"]] == ["other"]
    assert out["airflow_reachable"] is True


def test_annotated_flags_a_redacted_password():
    ir = _ir([{"conn_id": "db", "scope": "local", "conn_type": "postgres"}])
    client = _FakeClient([_remote("db", owner="my_flow", password="***")])
    assert c.annotated(ir, client)["connections"][0]["redacted"] is True


def test_annotated_marks_airflow_unreachable():
    assert c.annotated(_ir(), _FakeClient(unreachable=True))["airflow_reachable"] is False
