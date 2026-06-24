"""Configuration for the Airflow connection.

All settings come from environment variables so that credentials never live
in the frontend or in tracked files. In the devcontainer these are provided
by ``.devcontainer/docker-compose.yaml``.

    AIRFLOW_API_URL       Base URL of the Airflow webserver/API server.
                          The REST API is expected at ``<url>/api/v2`` and the
                          token endpoint at ``<url>/auth/token``.
                          Default: http://localhost:8080
    AIRFLOW_USERNAME      Username used to obtain a JWT token. Default: admin
    AIRFLOW_PASSWORD      Password used to obtain a JWT token. Default: admin
    AIRFLOW_API_TOKEN     A pre-minted JWT. If set, username/password are not
                          used and no token is requested from /auth/token.
    AIRFLOW_VERIFY_SSL    "false" to disable TLS verification. Default: true
    AIRFLOW_DAGS_DIR      Deploy target: the dags folder on the shared volume
                          (read by ``deploy.py``). Default: /opt/airflow/dags.
                          In the devcontainer this is the mounted host
                          ``airflow-dags/`` folder.
    AIRFLOW_OPERATORS_DIR Optional extra directory of operator YAML files that
                          override/extend the bundled registry (``registry.py``).

    Deploy target selection (PRD §6.5.1 / §8.7), read by ``deploy.py``:
    AIRFLOW_DEPLOY_TARGET "shared_volume" (default), "git", or "s3" — which
                          DeployTarget to write through.
    AIRFLOW_GIT_DAGS_REPO For the git target: path to the local git working tree
                          that the Airflow GitDagBundle tracks (required for git).
    AIRFLOW_GIT_DAGS_SUBDIR  DAG subdir within the repo. Default: dags.
    AIRFLOW_GIT_DAGS_BRANCH  Branch to push to. Default: main.
    AIRFLOW_GIT_DAGS_REMOTE  Remote to push to (e.g. origin). Unset → commit-only
                          (for a repo Airflow reads directly).
    AIRFLOW_S3_DAGS_BUCKET For the s3 target: the bucket the Airflow S3 DAG
                          bundle reads (required for s3; needs the boto3 package).
    AIRFLOW_S3_DAGS_PREFIX   Key prefix for DAG objects. Default: dags.
    AIRFLOW_S3_ENDPOINT_URL  S3 endpoint for an S3-compatible store (e.g. MinIO);
                          unset → AWS S3.

Security / multi-user trust model (PRD §9):
    The server uses **one Airflow service account** per JupyterLab server process
    (the env creds above) — there is no per-request Airflow identity inside a
    server. On **JupyterHub** each user gets their own server process, so inject
    **per-user** Airflow creds at spawn (``c.Spawner.environment`` /
    ``auth_state``) for real per-user authorization; env creds are the
    single-user / dev fallback. Until per-user creds are injected, **any Jupyter
    user of a given server acts as that one Airflow account**, and the shared dags
    folder / bundle is a shared trust boundary — writing a DAG runs code as the
    Airflow worker (treat deploy as privileged).

    Every **mutating** action (deploy / trigger / pause / stop-run / clear /
    delete / rollback / retire) is **audited** (PRD §9): ``audit.py`` emits a
    structured ``{ts, user, action, dag_id, correlation_id, outcome}`` JSON line
    on the ``jupyterlab_airflow.audit`` logger, stamped with the authenticated
    Jupyter user. Route that logger to a file/SIEM via normal logging config.
"""

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class AirflowConfig:
    base_url: str
    username: str
    password: str
    token: str = ""
    verify_ssl: bool = True

    @classmethod
    def from_env(cls) -> "AirflowConfig":
        base_url = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080")
        return cls(
            base_url=base_url.rstrip("/"),
            username=os.environ.get("AIRFLOW_USERNAME", "admin"),
            password=os.environ.get("AIRFLOW_PASSWORD", "admin"),
            token=os.environ.get("AIRFLOW_API_TOKEN", ""),
            verify_ssl=_env_bool("AIRFLOW_VERIFY_SSL", True),
        )
