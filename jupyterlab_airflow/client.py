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

    def list_dags(
        self,
        limit: int = 100,
        offset: int = 0,
        exclude_stale: bool = True,
        paused=None,
        dag_id_pattern=None,
        tags=None,
    ) -> dict:
        # Airflow 3 /api/v2: `only_active` was removed -> `exclude_stale`;
        # list filters are form-exploded (requests repeats list params).
        params = {
            "limit": limit,
            "offset": offset,
            "exclude_stale": str(exclude_stale).lower(),
        }
        if paused is not None:
            params["paused"] = str(paused).lower()
        if dag_id_pattern:
            params["dag_id_pattern"] = dag_id_pattern
        if tags:
            params["tags"] = tags
        return self._request("GET", "/dags", params=params)

    def get_dag(self, dag_id: str) -> dict:
        return self._request("GET", f"/dags/{dag_id}")

    def get_dag_details(self, dag_id: str) -> dict:
        """Full DAG detail incl. the serialized ``params`` dict — drives the
        manager's trigger-with-conf form (PRD §6.6/§15.10). Airflow serializes
        each param as ``{value, description, schema}`` (a JSON-Schema fragment)."""
        return self._request("GET", f"/dags/{dag_id}/details")

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

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict:
        """One DagRun's current state (drives the run-on-deploy / stop banners)."""
        return self._request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}")

    def set_dag_run_state(
        self, dag_id: str, dag_run_id: str, state: str = "failed"
    ) -> dict:
        """Set a DagRun's state (PRD §6.6/§8.8). Airflow 3 has **no** run cancel
        endpoint — stopping an in-flight run is ``PATCH …/dagRuns/{id}`` to a
        terminal state (``failed``); the scheduler then terminates its running
        task instances. Allowed states: ``queued|success|failed``."""
        return self._request(
            "PATCH",
            f"/dags/{dag_id}/dagRuns/{dag_run_id}",
            json={"state": state},
        )

    def list_import_errors(self, limit: int = 100) -> dict:
        """All current DAG-file import errors (the deploy recovery surface)."""
        return self._request("GET", "/importErrors", params={"limit": limit})

    def list_providers(self, limit: int = 1000) -> dict:
        """Providers installed in the **target** Airflow — the gating key for the
        operator palette (PRD §6.2.1). Returns ``{providers: [{package_name,
        version, description}], total_entries}``."""
        return self._request("GET", "/providers", params={"limit": limit})

    def version(self) -> dict:
        """The target Airflow version (``{version, git_version}``) — drives the
        ``version-too-old`` palette annotation against each op's
        ``airflow_min_version`` (PRD §6.2.1)."""
        return self._request("GET", "/version")

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

    def list_task_instances(self, dag_id: str, dag_run_id: str) -> dict:
        return self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
        )

    def get_task_logs(
        self, dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1
    ) -> dict:
        """Task-instance logs for one try (PRD §6.6).

        Airflow 3's logs endpoint returns ``content`` as a list of **structured
        events** (``{event, timestamp?, level?, logger?, …}`` — verified against
        ``apache-airflow-core 3.0.2`` ``StructuredLogMessage``) or a list of
        strings on a parse error. We pass the structured ``events`` through (so
        the viewer can colour by the *server-provided* level instead of guessing
        from the line text) **and** a flattened ``content`` string (for
        Copy/Download and the plain-text fallback). A plain-text / older response
        yields ``content`` only — back-compatible with the prior ``{content: str}``.

        For a **finished** task the response is the whole log in one chunk; for a
        **running/deferred** task it is the first chunk plus a ``continuation_token``
        (``full_content`` does not force an exhaustive read). We **drain the token**
        (bounded by ``_MAX_LOG_CHUNKS``) so the viewer shows the complete log so far,
        and set ``truncated: True`` if the cap is hit while more remained — rather
        than silently presenting a partial log as complete.
        """
        path = (
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        )
        params: dict = {"full_content": "true"}
        events: list = []
        lines: list = []
        token = None
        truncated = False
        for _ in range(_MAX_LOG_CHUNKS):
            raw = self._request("GET", path, params=params)
            content = raw.get("content") if isinstance(raw, dict) else raw
            chunk_lines, chunk_events = _parse_log_content(content)
            lines.extend(chunk_lines)
            events.extend(chunk_events)
            token = raw.get("continuation_token") if isinstance(raw, dict) else None
            # Stop at end-of-log (no token), or when a chunk adds nothing new (a
            # running task that has caught up to "now").
            if not token or not chunk_lines:
                break
            params = {"token": token}
        else:
            # Exited via the cap with a token still pending → more log remains.
            truncated = bool(token)
        result: dict = {"content": "\n".join(lines)}
        if events:
            result["events"] = events
        if truncated:
            result["truncated"] = True
        return result

    def clear_task_instances(
        self,
        dag_id: str,
        *,
        task_ids=None,
        dag_run_id=None,
        dry_run: bool = True,
        reset_dag_runs: bool = True,
    ) -> dict:
        """Clear (retry) task instances. ``dry_run`` previews the affected set."""
        body: dict = {"dry_run": dry_run, "reset_dag_runs": reset_dag_runs}
        if task_ids:
            body["task_ids"] = task_ids
        if dag_run_id:
            body["dag_run_id"] = dag_run_id
        return self._request("POST", f"/dags/{dag_id}/clearTaskInstances", json=body)

    def delete_dag(self, dag_id: str) -> dict:
        return self._request("DELETE", f"/dags/{dag_id}")


def _safe_json(resp):
    try:
        return resp.json()
    except ValueError:
        return resp.text


# How many log chunks to drain via the continuation token before giving up and
# flagging the log truncated (a running task streams a token forever).
_MAX_LOG_CHUNKS = 20


def _format_structured_line(event: dict) -> str:
    """Render one structured log event as a readable text line for the flattened
    ``content`` (Copy/Download + plain-text fallback): ``<timestamp> <LEVEL>
    <event>``, skipping absent fields."""
    parts = []
    if event.get("timestamp"):
        parts.append(event["timestamp"])
    if event.get("level"):
        parts.append(str(event["level"]).upper())
    message = event.get("event", "")
    if message:
        parts.append(message)
    return " ".join(parts)


def _parse_log_content(content) -> tuple:
    """Parse one logs-endpoint ``content`` chunk into ``(lines, events)``.

    ``content`` is a list of structured-event dicts (→ events + readable lines),
    a list of strings/tuples (parse-error fallback → lines only), or a string
    (plain text → one line). Any other/unexpected shape contributes nothing (so a
    malformed envelope yields an empty log, not a dumped Python repr)."""
    events: list = []
    lines: list = []
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict):
                event = {"event": str(item.get("event", ""))}
                for key in ("timestamp", "level", "logger"):
                    value = item.get(key)
                    if value not in (None, ""):
                        event[key] = str(value)
                events.append(event)
                lines.append(_format_structured_line(event))
            elif isinstance(item, (list, tuple)):
                lines.append(" ".join(str(piece) for piece in item))
            else:
                lines.append(str(item))
    elif isinstance(content, str) and content:
        lines.append(content)
    return lines, events


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
