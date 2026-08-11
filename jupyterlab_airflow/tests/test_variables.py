"""Variables: scopes, ownership, reference scanning, gates, lifecycle (PRD §6.10)."""

import pytest

from jupyterlab_airflow import variables as v
from jupyterlab_airflow.client import AirflowError


class _FakeClient:
    """Stands in for the Airflow variables endpoints."""

    def __init__(self, variables=None, unreachable=False):
        self.variables = {
            entry["key"]: dict(entry) for entry in (variables or [])
        }
        self.unreachable = unreachable
        self.deleted = []
        self.created = []
        self.updated = []

    def list_variables(self, limit=1000, offset=0, key_pattern=None):
        if self.unreachable:
            raise AirflowError("down", status=0)
        return {
            "variables": list(self.variables.values()),
            "total_entries": len(self.variables),
        }

    def get_variable(self, key):
        if key not in self.variables:
            raise AirflowError("not found", status=404)
        return self.variables[key]

    def create_variable(self, key, value, description=None):
        if key in self.variables:
            raise AirflowError("exists", status=409)
        self.variables[key] = {
            "key": key,
            "value": value,
            "description": description,
        }
        self.created.append(key)
        return self.variables[key]

    def update_variable(self, key, value, description=None):
        entry = self.variables.setdefault(key, {"key": key})
        entry["value"] = value
        if description is not None:
            entry["description"] = description
        self.updated.append(key)
        return entry

    def delete_variable(self, key):
        if key not in self.variables:
            raise AirflowError("not found", status=404)
        del self.variables[key]
        self.deleted.append(key)
        return {}


def _ir(variables=None, nodes=None, dag_id="my_flow", dag=None):
    return {
        "dag": {"dag_id": dag_id, **(dag or {})},
        "variables": variables or [],
        "nodes": nodes or [],
        "edges": [],
    }


# -- declarations ----------------------------------------------------------


def test_declared_normalizes_and_dedupes():
    ir = _ir(
        [
            {"key": " api_base ", "scope": "local", "value": "x"},
            {"key": "api_base", "scope": "remote"},  # duplicate key -> dropped
            {"key": "", "scope": "local"},  # no key -> dropped
            "not a dict",
            {"key": "tok", "scope": "bogus"},  # unknown scope -> local
        ]
    )
    out = v.declared(ir)
    assert [e["key"] for e in out] == ["api_base", "tok"]
    assert out[0]["scope"] == "local"
    assert out[1]["scope"] == "local"
    assert out[0]["var_type"] == "string"


def test_absent_variables_collection_is_inert():
    """Older `.afdag` files have no `variables` at all — nothing may break."""
    bare = {"dag": {"dag_id": "d"}, "nodes": [], "edges": []}
    assert v.declared(bare) == []
    assert v.undefined_reference_errors(bare) == []
    assert v.declaration_errors(bare) == []
    assert v.uses_variable_api(bare) is False


# -- ownership marker ------------------------------------------------------


def test_ownership_marker_roundtrip():
    described = v.compose_description("my_flow", "the base url")
    assert v.owner_of(described) == "my_flow"
    assert v.strip_marker(described) == "the base url"
    assert v.is_owned_by(described, "my_flow")
    assert not v.is_owned_by(described, "other_flow")


def test_unmarked_description_has_no_owner():
    assert v.owner_of("a hand-written description") is None
    assert v.owner_of(None) is None
    assert not v.is_owned_by("a hand-written description", "my_flow")


def test_compose_description_without_text_is_just_the_marker():
    assert v.compose_description("f", "") == "[airflow-studio dag_id=f]"


# -- reference scanning ----------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("{{ var.value.api_base }}", {"api_base"}),
        ("{{ var.json.cfg }}", {"cfg"}),
        ("{{ var.value.get('dotted.key') }}", {"dotted.key"}),
        ('{{ var.json.get("with-dash") }}', {"with-dash"}),
        ("Variable.get('code_key')", {"code_key"}),
        ('Variable.get("k", deserialize_json=True)', {"k"}),
        ("echo {{ var.value.a }} {{ var.value.b }}", {"a", "b"}),
        ("no references here", set()),
    ],
)
def test_reference_syntaxes(text, expected):
    ir = _ir(nodes=[{"task_id": "t", "op": "bash", "params": {"cmd": text}}])
    assert set(v.references(ir)) == expected


def test_references_scan_code_bodies_and_dag_params():
    ir = _ir(
        nodes=[
            {"task_id": "t", "op": "python_task", "params": {},
             "code": "x = Variable.get('from_code')"},
        ],
        dag={"params": {"p": "{{ var.value.from_dag }}"}},
    )
    refs = v.references(ir)
    assert refs["from_code"] == ["task 't'"]
    assert refs["from_dag"] == ["DAG params"]


def test_references_find_keys_nested_in_params():
    ir = _ir(
        nodes=[{"task_id": "t", "op": "x",
                "params": {"env": {"A": "{{ var.value.nested }}"},
                           "args": ["{{ var.value.in_list }}"]}}]
    )
    assert set(v.references(ir)) == {"nested", "in_list"}


def test_get_is_not_mistaken_for_a_key():
    """`var.value.get('k')` also matches the dotted form with key 'get'."""
    ir = _ir(nodes=[{"task_id": "t", "op": "b",
                     "params": {"c": "{{ var.value.get('real') }}"}}])
    assert set(v.references(ir)) == {"real"}


def test_uses_variable_api_only_for_code_bodies():
    jinja = _ir(nodes=[{"task_id": "t", "op": "b",
                        "params": {"c": "{{ var.value.k }}"}}])
    assert v.uses_variable_api(jinja) is False
    code = _ir(nodes=[{"task_id": "t", "op": "p", "params": {},
                       "code": "Variable.get('k')"}])
    assert v.uses_variable_api(code) is True


# -- structural validation -------------------------------------------------


def test_undefined_reference_is_an_error_naming_the_task():
    ir = _ir(nodes=[{"task_id": "loader", "op": "b",
                     "params": {"c": "{{ var.value.missing }}"}}])
    errors = v.undefined_reference_errors(ir)
    assert len(errors) == 1
    assert "missing" in errors[0] and "loader" in errors[0]


def test_declared_reference_is_not_an_error():
    ir = _ir(
        [{"key": "known", "scope": "remote"}],
        [{"task_id": "t", "op": "b", "params": {"c": "{{ var.value.known }}"}}],
    )
    assert v.undefined_reference_errors(ir) == []


def test_json_type_requires_valid_json_for_local_values():
    bad = _ir([{"key": "cfg", "scope": "local", "value": "nope", "var_type": "json"}])
    assert "valid JSON" in v.declaration_errors(bad)[0]
    good = _ir([{"key": "cfg", "scope": "local", "value": '{"a":1}', "var_type": "json"}])
    assert v.declaration_errors(good) == []


def test_whitespace_key_blocked_for_local_but_allowed_for_remote():
    """Airflow itself accepts spaces, so only keys we create are constrained —
    a remote key must be referenceable exactly as it exists there."""
    local = _ir([{"key": "has space", "scope": "local", "value": "1"}])
    assert "whitespace" in v.declaration_errors(local)[0]
    remote = _ir([{"key": "has space", "scope": "remote"}])
    assert v.declaration_errors(remote) == []


def test_over_long_key_is_rejected():
    ir = _ir([{"key": "k" * 251, "scope": "local", "value": "1"}])
    assert "at most 250" in v.declaration_errors(ir)[0]


def test_unused_and_blocking_usage():
    ir = _ir(
        [{"key": "used", "scope": "remote"}, {"key": "spare", "scope": "remote"}],
        [{"task_id": "t", "op": "b", "params": {"c": "{{ var.value.used }}"}}],
    )
    assert v.unused_declarations(ir) == ["spare"]
    assert v.blocking_usage(ir, "used") == ["task 't'"]
    assert v.blocking_usage(ir, "spare") == []


# -- deploy gate -----------------------------------------------------------


def test_block_errors_flag_a_vanished_remote_variable():
    ir = _ir([{"key": "shared", "scope": "remote"}])
    errors = v.block_errors(ir, _FakeClient())
    assert len(errors) == 1 and "no longer exists" in errors[0]


def test_block_errors_allow_an_existing_remote_variable():
    ir = _ir([{"key": "shared", "scope": "remote"}])
    client = _FakeClient([{"key": "shared", "value": "1", "description": "ops"}])
    assert v.block_errors(ir, client) == []


def test_block_errors_refuse_a_local_key_owned_by_another_flow():
    ir = _ir([{"key": "token", "scope": "local", "value": "mine"}])
    client = _FakeClient(
        [{"key": "token", "value": "theirs",
          "description": v.compose_description("other_flow")}]
    )
    errors = v.block_errors(ir, client)
    assert len(errors) == 1 and "other_flow" in errors[0]


def test_block_errors_refuse_a_local_key_created_outside_studio():
    ir = _ir([{"key": "token", "scope": "local", "value": "mine"}])
    client = _FakeClient([{"key": "token", "value": "x", "description": "by hand"}])
    errors = v.block_errors(ir, client)
    assert len(errors) == 1 and "outside Studio" in errors[0]


def test_block_errors_allow_reclaiming_our_own_key():
    ir = _ir([{"key": "token", "scope": "local", "value": "v2"}])
    client = _FakeClient(
        [{"key": "token", "value": "v1",
          "description": v.compose_description("my_flow")}]
    )
    assert v.block_errors(ir, client) == []


def test_block_errors_are_empty_when_airflow_is_unreachable():
    """A network blip must never block a deploy — /importErrors is the verdict."""
    ir = _ir([{"key": "shared", "scope": "remote"}])
    assert v.block_errors(ir, _FakeClient(unreachable=True)) == []


# -- sync / purge ----------------------------------------------------------


def test_sync_creates_local_variables_with_the_ownership_marker():
    ir = _ir([{"key": "api_base", "scope": "local", "value": "https://x",
               "description": "base url"}])
    client = _FakeClient()
    synced, warnings = v.sync(ir, client)
    assert synced == ["api_base"] and warnings == []
    stored = client.variables["api_base"]
    assert stored["value"] == "https://x"
    assert v.owner_of(stored["description"]) == "my_flow"
    assert v.strip_marker(stored["description"]) == "base url"


def test_sync_updates_a_variable_the_flow_already_owns():
    client = _FakeClient(
        [{"key": "api_base", "value": "old",
          "description": v.compose_description("my_flow")}]
    )
    ir = _ir([{"key": "api_base", "scope": "local", "value": "new"}])
    synced, warnings = v.sync(ir, client)
    assert synced == ["api_base"] and warnings == []
    assert client.variables["api_base"]["value"] == "new"
    assert client.created == []


def test_sync_never_touches_remote_variables():
    ir = _ir([{"key": "shared", "scope": "remote"}])
    client = _FakeClient()
    synced, warnings = v.sync(ir, client)
    assert synced == [] and warnings == [] and client.variables == {}


def test_purge_removes_only_variables_this_flow_owns():
    client = _FakeClient(
        [
            {"key": "mine", "value": "1",
             "description": v.compose_description("my_flow")},
            {"key": "theirs", "value": "2",
             "description": v.compose_description("other_flow")},
            {"key": "manual", "value": "3", "description": "set by an operator"},
        ]
    )
    removed = v.purge("my_flow", client)
    assert removed == ["mine"]
    assert set(client.variables) == {"theirs", "manual"}


def test_purge_is_a_noop_when_nothing_is_owned():
    client = _FakeClient([{"key": "manual", "value": "1", "description": None}])
    assert v.purge("my_flow", client) == []
    assert set(client.variables) == {"manual"}


# -- single-variable CRUD guards -------------------------------------------


def test_set_one_creates_then_updates():
    client = _FakeClient()
    assert v.set_one("my_flow", "k", "v1", "note", client)["created"] is True
    assert v.set_one("my_flow", "k", "v2", "note", client)["created"] is False
    assert client.variables["k"]["value"] == "v2"


def test_set_one_refuses_a_variable_owned_by_another_flow():
    client = _FakeClient(
        [{"key": "k", "value": "x",
          "description": v.compose_description("other_flow")}]
    )
    with pytest.raises(v.VariableOwnershipError, match="other_flow"):
        v.set_one("my_flow", "k", "mine", "", client)
    assert client.variables["k"]["value"] == "x"


def test_delete_one_refuses_a_variable_the_flow_does_not_own():
    client = _FakeClient([{"key": "k", "value": "x", "description": "by hand"}])
    with pytest.raises(v.VariableOwnershipError):
        v.delete_one("my_flow", "k", client)
    assert "k" in client.variables


def test_delete_one_of_a_missing_key_is_not_an_error():
    assert v.delete_one("my_flow", "gone", _FakeClient())["deleted"] is False


# -- annotated payload -----------------------------------------------------


def test_annotated_reports_usage_existence_and_ownership():
    ir = _ir(
        [
            {"key": "mine", "scope": "local", "value": "1"},
            {"key": "shared", "scope": "remote"},
        ],
        [{"task_id": "t", "op": "b", "params": {"c": "{{ var.value.mine }}"}}],
    )
    client = _FakeClient(
        [
            {"key": "mine", "value": "1",
             "description": v.compose_description("my_flow")},
            {"key": "other", "value": "9", "description": "elsewhere"},
        ]
    )
    out = v.annotated(ir, client)
    by_key = {e["key"]: e for e in out["variables"]}
    assert by_key["mine"]["used_by"] == ["task 't'"]
    assert by_key["mine"]["exists"] and by_key["mine"]["owned"]
    assert not by_key["shared"]["exists"]
    assert out["unused"] == ["shared"]
    assert out["airflow_reachable"] is True
    # The picker offers only variables the flow has not declared.
    assert [e["key"] for e in out["available"] if not e["declared"]] == ["other"]


def test_annotated_hides_a_redacted_value():
    """Airflow masks sensitive-looking keys; the masked literal must never be
    surfaced as a real value (writing it back would destroy the secret)."""
    ir = _ir([{"key": "my_password", "scope": "local", "value": "s3cret"}])
    client = _FakeClient(
        [{"key": "my_password", "value": "***",
          "description": v.compose_description("my_flow")}]
    )
    entry = v.annotated(ir, client)["variables"][0]
    assert entry["redacted"] is True
    assert entry["airflow_value"] is None


def test_annotated_flags_undefined_references():
    ir = _ir(nodes=[{"task_id": "t", "op": "b",
                     "params": {"c": "{{ var.value.ghost }}"}}])
    assert v.annotated(ir, _FakeClient())["undefined"] == ["ghost"]


def test_annotated_marks_airflow_unreachable():
    out = v.annotated(_ir(), _FakeClient(unreachable=True))
    assert out["airflow_reachable"] is False
