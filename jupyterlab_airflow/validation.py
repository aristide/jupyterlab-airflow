"""Full codegen validation pipeline (PRD Appendix E).

Stages 1–6 (schema/semantics/identifiers/render/parse/compile) are handled by
:func:`codegen.generate_dag` and execute **no** user code. Stage 7 — a ``DagBag``
import — is the trust boundary: it runs the generated module's top-level code, so
it happens in an **isolated subprocess** with a wall-clock timeout and a
secret-scrubbed environment.

In a Jupyter server where Airflow isn't importable (the common case — the Jupyter
env ≠ the Airflow env, PRD R2), stage 7 reports ``skipped`` rather than failing;
the authoritative verdict then comes post-deploy from ``/api/v2/importErrors``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from typing import Any, Dict

from .codegen import format_code, generate_dag

# Runs inside the subprocess. Prints a single JSON line describing the DagBag
# result. Never raises out — Airflow-absent is reported as ``skipped``.
_DAGBAG_SCRIPT = r"""
import json, sys
folder = sys.argv[1]
try:
    from airflow.dag_processing.dagbag import DagBag
except Exception as exc:  # Airflow not installed in this env
    print(json.dumps({"status": "skipped", "detail": "Airflow not importable: %s" % exc}))
    sys.exit(0)
try:
    bag = DagBag(dag_folder=folder, include_examples=False, safe_mode=False)
except Exception as exc:
    print(json.dumps({"status": "error", "detail": {"<dagbag>": str(exc)}}))
    sys.exit(0)
errors = {str(k): str(v) for k, v in (bag.import_errors or {}).items()}
print(json.dumps({
    "status": "error" if errors else "ok",
    "detail": errors,
    "dags": list(bag.dags.keys()),
}))
"""

# Env keys whose values must never reach the (potentially user-authored) DagBag
# subprocess.
_SECRET_ENV_PREFIXES = ("AIRFLOW_API_", "AIRFLOW_USERNAME", "AIRFLOW_PASSWORD")
_SECRET_ENV_SUBSTRINGS = ("SECRET", "TOKEN", "PASSWORD", "CONN")


def _scrubbed_env() -> Dict[str, str]:
    env = {}
    for key, value in os.environ.items():
        upper = key.upper()
        if upper.startswith(_SECRET_ENV_PREFIXES):
            continue
        if any(part in upper for part in _SECRET_ENV_SUBSTRINGS):
            continue
        env[key] = value
    return env


def dagbag_check(code: str, timeout: float = 30.0) -> Dict[str, Any]:
    """Stage 7: import ``code`` as a DagBag in an isolated subprocess.

    Returns ``{status: ok|skipped|error, detail, dags?}``. ``skipped`` when
    Airflow isn't importable; ``error`` on import errors or timeout.
    """
    with tempfile.TemporaryDirectory(prefix="afdag-validate-") as folder:
        with open(os.path.join(folder, "dag_under_test.py"), "w", encoding="utf-8") as fh:
            fh.write(code)
        try:
            proc = subprocess.run(
                [sys.executable, "-c", _DAGBAG_SCRIPT, folder],
                capture_output=True,
                text=True,
                timeout=timeout,
                env=_scrubbed_env(),
                cwd=folder,
            )
        except subprocess.TimeoutExpired:
            return {"status": "error", "detail": f"DagBag import timed out after {timeout:g}s"}

        line = (proc.stdout or "").strip().splitlines()
        if not line:
            return {"status": "error", "detail": proc.stderr.strip() or "no output from validator"}
        try:
            return json.loads(line[-1])
        except json.JSONDecodeError:
            return {"status": "error", "detail": proc.stderr.strip() or line[-1]}


def validate_dag(ir: Dict[str, Any], run_dagbag: bool = True) -> Dict[str, Any]:
    """Run the full Appendix E pipeline without writing anything.

    Returns ``{valid, code, errors, dagbag}``. ``valid`` requires stages 1–6 to
    pass and stage 7 to be ``ok`` or ``skipped`` (a ``skipped`` DagBag does not
    fail validation — Airflow will have the final say post-deploy).
    """
    result = generate_dag(ir)
    code = format_code(result["code"]) if result["valid"] else result["code"]
    dagbag: Dict[str, Any] = {"status": "skipped", "detail": "not run"}

    if result["valid"] and run_dagbag:
        dagbag = dagbag_check(code)

    valid = result["valid"] and dagbag["status"] in ("ok", "skipped")
    return {
        "valid": valid,
        "code": code,
        "errors": list(result["errors"]),
        "dagbag": dagbag,
    }
