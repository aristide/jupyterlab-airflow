"""Tests for the operator registry loader (`GET operators` data source)."""

import pytest

from jupyterlab_airflow import registry


@pytest.fixture(autouse=True)
def _clear_cache():
    # Each test starts from a cold cache and unset user dir.
    registry._cache["signature"] = None
    registry._cache["operators"] = None
    yield


def test_bundled_registry_loads():
    ops = registry.load_registry()
    ids = {op["id"] for op in ops}
    assert {"empty", "bash", "python_task", "branch", "trigger_dagrun"} <= ids
    # Sorted by (category, label) for a deterministic palette order.
    keys = [(op.get("category", ""), op.get("label", op["id"])) for op in ops]
    assert keys == sorted(keys)


def test_bash_is_airflow3_correct():
    bash = next(op for op in registry.load_registry() if op["id"] == "bash")
    assert "airflow.providers.standard.operators.bash" in bash["import"]
    # Never an Airflow-2 import path.
    assert "airflow.operators." not in bash["import"]
    assert bash["task_id_prefix"] == "bash"


def test_client_view_hides_codegen_fields():
    view = registry.client_view()
    bash = next(op for op in view if op["id"] == "bash")
    # camelCase key for the TS interface.
    assert bash["taskIdPrefix"] == "bash"
    # Codegen-only fields are not shipped to the browser.
    for hidden in ("import", "import_taskflow", "template_traditional", "template_taskflow"):
        assert hidden not in bash
    required = {p["name"] for p in bash["params"] if p["required"]}
    assert required == {"bash_command"}


def test_client_view_ships_doc_fields_for_info_tab():
    bash = next(op for op in registry.client_view() if op["id"] == "bash")
    # Operator-level learning fields, camelCased for the TS IOperatorDef.
    assert bash["description"]
    assert bash["docsUrl"].startswith("http")
    assert bash["example"]
    assert bash["provider"] == "apache-airflow-providers-standard"
    assert bash["airflowMinVersion"] == "3.0"
    # Per-param contextual help reaches the client.
    cmd = next(p for p in bash["params"] if p["name"] == "bash_command")
    assert cmd["help"]


def test_caches_until_files_change():
    first = registry.load_registry()
    assert registry.load_registry() is first  # same object: cache hit
    assert registry.load_registry(force=True) is not first  # forced reload


def test_user_dir_overrides_and_adds(tmp_path, monkeypatch):
    # Add a brand-new operator and override the bundled `bash` label.
    (tmp_path / "custom.yaml").write_text("id: custom\nlabel: Custom\ncategory: Misc\n")
    (tmp_path / "bash.yaml").write_text("id: bash\nlabel: Overridden bash\ncategory: Python/Bash\n")
    monkeypatch.setenv("AIRFLOW_OPERATORS_DIR", str(tmp_path))

    by_id = {op["id"]: op for op in registry.load_registry(force=True)}
    assert "custom" in by_id
    assert by_id["bash"]["label"] == "Overridden bash"


def test_top_level_must_be_mapping(tmp_path, monkeypatch):
    (tmp_path / "broken.yaml").write_text("- just\n- a\n- list\n")
    monkeypatch.setenv("AIRFLOW_OPERATORS_DIR", str(tmp_path))
    with pytest.raises(registry.RegistryError):
        registry.load_registry(force=True)


def test_missing_id_raises(tmp_path, monkeypatch):
    (tmp_path / "noid.yaml").write_text("label: No id here\ncategory: Misc\n")
    monkeypatch.setenv("AIRFLOW_OPERATORS_DIR", str(tmp_path))
    with pytest.raises(registry.RegistryError):
        registry.load_registry(force=True)
