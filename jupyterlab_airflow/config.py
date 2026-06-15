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
