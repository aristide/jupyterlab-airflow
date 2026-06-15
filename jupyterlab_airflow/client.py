"""A thin client for the Apache Airflow 3.x REST API (``/api/v2``).

Airflow 3 protects the REST API with JWT bearer tokens. A token is obtained by
POSTing credentials to ``/auth/token`` and is then sent as
``Authorization: Bearer <token>`` on every call. This client fetches the token
lazily, caches it, and transparently refreshes once on a ``401``.

The client is intentionally synchronous (built on ``requests``); the Tornado
handlers run it in a thread-pool executor so the server event loop is never
blocked.
"""

import threading

import requests

from .config import AirflowConfig

API_PREFIX = "/api/v2"
TOKEN_PATH = "/auth/token"


class AirflowError(Exception):
    """Raised when Airflow returns an error or is unreachable."""

    def __init__(self, message: str, status: int = 0, detail=None):
        super().__init__(message)
        self.status = status
        self.detail = detail


class AirflowClient:
    def __init__(self, config: AirflowConfig):
        self._config = config
        self._token = config.token or ""
        self._lock = threading.Lock()
        self._session = requests.Session()
        self._session.verify = config.verify_ssl

    # -- auth ---------------------------------------------------------------

    def _fetch_token(self) -> str:
        url = self._config.base_url + TOKEN_PATH
        try:
            resp = self._session.post(
                url,
                json={
                    "username": self._config.username,
                    "password": self._config.password,
                },
                timeout=30,
            )
        except requests.RequestException as err:
            raise AirflowError(f"Cannot reach Airflow at {url}: {err}") from err

        if resp.status_code >= 400:
            raise AirflowError(
                "Failed to authenticate against Airflow "
                f"({resp.status_code}). Check AIRFLOW_USERNAME / AIRFLOW_PASSWORD.",
                status=resp.status_code,
                detail=_safe_json(resp),
            )

        token = (resp.json() or {}).get("access_token")
        if not token:
            raise AirflowError("Airflow /auth/token response had no access_token")
        return token

    def _ensure_token(self) -> str:
        with self._lock:
            if not self._token:
                self._token = self._fetch_token()
            return self._token

    def _clear_token(self) -> None:
        with self._lock:
            # Never clear an externally supplied static token.
            if not self._config.token:
                self._token = ""

    # -- low-level request --------------------------------------------------

    def _request(self, method: str, path: str, *, params=None, json=None, _retry=True):
        token = self._ensure_token()
        url = self._config.base_url + API_PREFIX + path
        try:
            resp = self._session.request(
                method,
                url,
                params=params,
                json=json,
                headers={"Authorization": f"Bearer {token}"},
                timeout=60,
            )
        except requests.RequestException as err:
            raise AirflowError(f"Cannot reach Airflow at {url}: {err}") from err

        if resp.status_code == 401 and _retry and not self._config.token:
            # Token likely expired: drop it and try once more.
            self._clear_token()
            return self._request(method, path, params=params, json=json, _retry=False)

        if resp.status_code >= 400:
            raise AirflowError(
                f"Airflow API {method} {path} failed ({resp.status_code})",
                status=resp.status_code,
                detail=_safe_json(resp),
            )

        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()

    # -- high-level API -----------------------------------------------------

    def health(self) -> dict:
        """Return connection metadata; raises AirflowError if unreachable."""
        self._ensure_token()
        return {
            "ok": True,
            "base_url": self._config.base_url,
            "username": self._config.username,
        }

    def list_dags(self, limit: int = 100, offset: int = 0, only_active: bool = True) -> dict:
        return self._request(
            "GET",
            "/dags",
            params={
                "limit": limit,
                "offset": offset,
                "only_active": str(only_active).lower(),
            },
        )

    def get_dag(self, dag_id: str) -> dict:
        return self._request("GET", f"/dags/{dag_id}")

    def set_paused(self, dag_id: str, is_paused: bool) -> dict:
        return self._request(
            "PATCH",
            f"/dags/{dag_id}",
            params={"update_mask": "is_paused"},
            json={"is_paused": is_paused},
        )

    def trigger_dag(self, dag_id: str, conf=None, logical_date=None) -> dict:
        body = {"logical_date": logical_date, "conf": conf or {}}
        return self._request("POST", f"/dags/{dag_id}/dagRuns", json=body)

    def list_dag_runs(self, dag_id: str, limit: int = 10) -> dict:
        return self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns",
            params={"limit": limit, "order_by": "-logical_date"},
        )

    def list_import_errors(self, limit: int = 100) -> dict:
        """All current DAG-file import errors (the deploy recovery surface)."""
        return self._request("GET", "/importErrors", params={"limit": limit})

    def deploy_status(self, dag_id: str, filename: str) -> dict:
        """One observation of a deploy's tri-state (PRD §6.5.4).

        Returns ``{state, import_error?, dag?}`` where ``state`` is:
          - ``failed``     — an import error references the deployed file;
          - ``registered`` — the DAG appears with no import error;
          - ``processing`` — not visible yet (Airflow hasn't re-parsed).
        The frontend polls this with bounded backoff and a timeout.
        """
        errors = self.list_import_errors().get("import_errors", []) or []
        match = next(
            (
                err
                for err in errors
                if _basename(err.get("filename")) == filename
            ),
            None,
        )
        if match is not None:
            return {"state": "failed", "import_error": match}

        try:
            dag = self.get_dag(dag_id)
        except AirflowError as err:
            if err.status == 404:
                return {"state": "processing"}
            raise
        return {
            "state": "registered",
            "dag": {
                "dag_id": dag.get("dag_id", dag_id),
                "is_paused": dag.get("is_paused", True),
            },
        }


def _safe_json(resp):
    try:
        return resp.json()
    except ValueError:
        return resp.text


def _basename(path) -> str:
    """Last path segment of an import-error filename (handles / and \\)."""
    if not path:
        return ""
    return str(path).replace("\\", "/").rsplit("/", 1)[-1]


_CLIENT = None
_CLIENT_LOCK = threading.Lock()


def get_client() -> AirflowClient:
    """Return a process-wide AirflowClient built from the environment."""
    global _CLIENT
    with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = AirflowClient(AirflowConfig.from_env())
        return _CLIENT


def reset_client() -> None:
    """Drop the cached client (used by tests)."""
    global _CLIENT
    with _CLIENT_LOCK:
        _CLIENT = None
