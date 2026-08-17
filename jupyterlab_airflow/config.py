"""Configuration for the Airflow connection.

All settings come from environment variables so that credentials never live
in the frontend or in tracked files. In the docker-compose dev environment
these are provided by the repo-root ``docker-compose.yml``.

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
                          In the docker-compose dev environment this is the
                          mounted host ``docker/airflow-dags/`` folder.
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

    Durable deploy lifecycle (PRD §6.5.4), read by ``reconciler.py``/``journal.py``.
    The server (not the browser) waits for registration, retires the renamed-away
    dag_id, unpauses and triggers — so a deploy completes even if the tab closes:
    JUPYTERLAB_AIRFLOW_RECONCILER  "on" (default) or "off". Off = no background
                          work at all and no journal entry; the editor drives the
                          remaining steps exactly as it did before. Also the
                          operator's escape hatch and the test kill switch.
    JUPYTERLAB_AIRFLOW_RECONCILE_INTERVAL_S  Sweep period, 5..300. Default 15.
                          The timer only exists while a deploy is in flight.
    JUPYTERLAB_AIRFLOW_DEPLOY_BUDGET_S  How long a deploy's lifecycle may take,
                          60..7200. Default 900 (Airflow's new-file scan interval
                          is ~300s; this covers three cycles plus parse). Past it
                          nothing is unpaused or triggered — a late unpause is not
                          "completing a deploy", it is fighting the user.
    JUPYTERLAB_AIRFLOW_JOURNAL_DIR  Where in-flight deploys are recorded. Default
                          <data_dir>/airflow-studio/deploy-journal. Point it at
                          local disk when the server's data_dir is on NFS.
    JUPYTERLAB_AIRFLOW_JOURNAL_RETENTION_S  How long finished entries stay
                          observable (so a reopened editor can say "deployed while
                          you were away"), 300..604800. Default 86400.
    Out-of-range/unparseable values are clamped/defaulted with a warning.

    Authorization (PRD §9):
    JUPYTERLAB_AIRFLOW_ROLE  "editor" (default) or "viewer". A viewer may read
                          everything but cannot run any privileged action —
                          deploy/undeploy/retire/rollback, trigger/pause/stop/
                          clear/delete, or write Variables/Connections. Unset
                          keeps the previous behaviour (editor), so this is
                          opt-in; an unrecognised value falls back to "viewer"
                          so a typo in a permission setting never grants rights.
                          On JupyterHub set it per user at spawn via
                          ``c.Spawner.environment`` — the same injection point
                          recommended below for per-user Airflow credentials.

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
    structured ``{ts, user, action, dag_id, correlation_id, outcome, via}`` JSON
    line on the ``jupyterlab_airflow.audit`` logger, stamped with the
    authenticated Jupyter user. Route that logger to a file/SIEM via normal
    logging config. ``via`` distinguishes a human request from a step the deploy
    reconciler completed in the background; the ``user`` is the human either way,
    and both share the deploy's ``correlation_id``.

    The **authorization gate is derived from that same audit marker**: a handler
    is privileged exactly when it is audited. Both live in one place
    (``_AirflowHandler.respond``), so the two sets cannot drift apart — a new
    mutating handler cannot be gated-but-unaudited or audited-but-ungated.
    A refused action is itself audited, with ``outcome="denied"``.
"""

import logging
import os
from dataclasses import dataclass

_log = logging.getLogger(__name__)

#: Studio authorization roles (PRD §9). ``editor`` may run privileged actions;
#: ``viewer`` may only read.
ROLE_EDITOR = "editor"
ROLE_VIEWER = "viewer"
ROLE_ENV_VAR = "JUPYTERLAB_AIRFLOW_ROLE"
_ROLES = (ROLE_EDITOR, ROLE_VIEWER)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def studio_role() -> str:
    """The current server process's Studio role, from ``JUPYTERLAB_AIRFLOW_ROLE``.

    Read per call rather than cached at import: it costs nothing, keeps tests
    honest (no module reload dance), and means a role change takes effect on
    server restart rather than needing a rebuild.

    **Unset defaults to ``editor``** so every existing single-user and dev
    install keeps working exactly as before — this control is opt-in.

    An **unrecognised** value falls back to ``viewer``, not ``editor``. That is
    deliberate: a typo in a permission setting must not silently grant the
    privilege it was meant to withhold. It is logged as a warning so the
    misconfiguration is visible rather than mysterious.

    On JupyterHub each user gets their own server process, so set this per user
    at spawn (``c.Spawner.environment``) — the same injection point §9 already
    recommends for per-user Airflow credentials.
    """
    raw = os.environ.get(ROLE_ENV_VAR)
    if raw is None:
        return ROLE_EDITOR
    role = raw.strip().lower()
    if role in _ROLES:
        return role
    _log.warning(
        "%s=%r is not one of %s — falling back to %r (a permission setting "
        "fails closed, so a typo never grants edit rights).",
        ROLE_ENV_VAR,
        raw,
        list(_ROLES),
        ROLE_VIEWER,
    )
    return ROLE_VIEWER


def can_edit() -> bool:
    """Whether this server may run privileged (mutating) Studio actions."""
    return studio_role() == ROLE_EDITOR


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
