import asyncio
import json
import traceback
from uuid import uuid4

import tornado
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join

from .audit import audit_event
from .client import AirflowError, get_client
from .codegen import generate_dag
from .deploy import (
    deploy_dag,
    find_orphans,
    find_source_path,
    purge_dag,
    rename_preflight,
    retire_old_dag,
    rollback_dag,
)
from .providers import annotated_notifiers, annotated_operators
from .validation import validate_dag

NAMESPACE = "jupyterlab-airflow"


class _AirflowHandler(APIHandler):
    """Base handler that runs the synchronous Airflow client off the event loop
    and maps :class:`AirflowError` onto a JSON error payload."""

    async def run(self, fn, *args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    def _current_username(self) -> str:
        """The authenticated Jupyter user for audit attribution (PRD §9). Under
        JupyterHub this is the Hub user (one server per user); in single-user/dev
        it's the local identity. Resolved defensively across jupyter_server
        identity shapes, defaulting to ``"anonymous"``."""
        user = self.current_user
        for attr in ("username", "name"):
            value = getattr(user, attr, None)
            if value:
                return str(value)
        if isinstance(user, (str, bytes)) and user:
            return user.decode() if isinstance(user, bytes) else user
        return "anonymous"

    async def respond(
        self,
        fn,
        *args,
        audit_action: str = None,
        audit_dag_id: str = None,
        **kwargs,
    ):
        """Run ``fn`` off the event loop and finish the JSON response. When
        ``audit_action`` is set, emit an audit record (PRD §9) on the way out —
        ``ok`` on success, ``rejected`` when the action ran but mutated nothing
        (a deploy refused by validation/provider gate), ``error`` on an exception
        — stamped with the current user and a per-request ``correlation_id``.
        Read-only handlers omit it (no audit)."""
        correlation_id = uuid4().hex if audit_action else ""

        def _audit(outcome, dag_id, detail=None):
            # Best-effort + isolated: a failure in audit emission (e.g. a custom
            # SIEM logging handler raising) must NOT alter the request outcome or
            # re-fire as a contradictory record — never let it propagate.
            if not audit_action:
                return
            try:
                audit_event(
                    audit_action,
                    user=self._current_username(),
                    correlation_id=correlation_id,
                    dag_id=dag_id,
                    outcome=outcome,
                    detail=detail,
                )
            except Exception:  # noqa: BLE001
                self.log.exception("audit emission failed (action=%s)", audit_action)

        try:
            data = await self.run(fn, *args, **kwargs)
        except AirflowError as err:
            _audit("error", audit_dag_id, detail=str(err))
            self.set_status(502)
            self.finish(json.dumps({"error": str(err), "detail": err.detail}))
            return
        except Exception as err:  # noqa: BLE001 - surface unexpected errors to UI
            _audit("error", audit_dag_id, detail=str(err))
            self.log.error(err)
            traceback.print_exc()
            self.set_status(500)
            self.finish(json.dumps({"error": str(err)}))
            return
        # Success: the outcome reflects the RESULT, not just exception-vs-not — a
        # deploy refused by validation / a missing provider returns
        # {"deployed": False, "errors": [...]} (nothing written), which must not be
        # audited as a successful deploy. Audit is emitted after fn but kept out of
        # finish()'s path (and best-effort above) so it can't break a good request.
        dag_id = audit_dag_id
        if dag_id is None and isinstance(data, dict):
            dag_id = data.get("dag_id")
        outcome, detail = "ok", None
        if isinstance(data, dict) and data.get("deployed") is False:
            outcome = "rejected"
            detail = "; ".join(str(e) for e in (data.get("errors") or [])) or None
        _audit(outcome, dag_id, detail)
        self.finish(json.dumps({"data": data}))


class HealthHandler(_AirflowHandler):
    @tornado.web.authenticated
    async def get(self):
        await self.respond(get_client().health)


class OperatorsHandler(_AirflowHandler):
    """Serve the operator registry to the editor palette + node forms, annotated
    with provider-availability against the target Airflow (PRD §6.2.1).

    Reads the bundled (and optional user) operator YAML registry, then reads the
    target's installed providers (cached, short-TTL) to tag each entry
    ``available | missing-provider | version-too-old | unknown``. ``?refresh=1``
    forces a fresh provider read. The Airflow round-trip + file I/O run off the
    event loop via :meth:`respond`/``run_in_executor``.
    """

    @tornado.web.authenticated
    async def get(self):
        refresh = self.get_argument("refresh", "").lower() in ("1", "true")
        await self.respond(annotated_operators, force=refresh)


class NotifiersHandler(_AirflowHandler):
    """Serve the notifier registry (PRD §6.8) to the editor's Notifications tab,
    annotated with provider-availability against the target Airflow (§6.2.1).
    ``?refresh=1`` forces a fresh provider read."""

    @tornado.web.authenticated
    async def get(self):
        refresh = self.get_argument("refresh", "").lower() in ("1", "true")
        await self.respond(annotated_notifiers, force=refresh)


class GenerateHandler(_AirflowHandler):
    """Render an `.afdag` IR (POST body) to Airflow 3.x Python for the CODE tab.

    Pure codegen — never touches Airflow and never executes user code. Returns
    ``{code, valid, errors}``; validation failures come back in ``errors`` (200),
    not as HTTP errors.
    """

    @tornado.web.authenticated
    async def post(self):
        ir = self.get_json_body() or {}
        await self.respond(generate_dag, ir)


class ValidateHandler(_AirflowHandler):
    """Run the full Appendix E validation pipeline (incl. the isolated DagBag
    subprocess) without writing anything. Returns ``{valid, code, errors, dagbag}``."""

    @tornado.web.authenticated
    async def post(self):
        ir = self.get_json_body() or {}
        await self.respond(validate_dag, ir)


class DeployHandler(_AirflowHandler):
    """Validate then atomically write the generated DAG to the dags folder.

    Privileged (PRD §9): writing into the dags folder == running code as the
    Airflow worker. Validation failures come back in ``errors`` (200), the file
    is not written. The post-deploy import poll lives in the manager.
    """

    @tornado.web.authenticated
    async def post(self):
        ir = self.get_json_body() or {}
        dag_id = (ir.get("dag") or {}).get("dag_id")
        await self.respond(deploy_dag, ir, audit_action="deploy", audit_dag_id=dag_id)


class DeployStatusHandler(_AirflowHandler):
    """One observation of a deploy's tri-state (registered/failed/processing).
    The frontend polls this with bounded backoff after a successful write."""

    @tornado.web.authenticated
    async def get(self):
        dag_id = self.get_argument("dag_id")
        filename = self.get_argument("filename")
        await self.respond(get_client().deploy_status, dag_id, filename)


class ImportErrorsHandler(_AirflowHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            limit = int(self.get_argument("limit", "100"))
        except ValueError:
            limit = 100
        await self.respond(get_client().list_import_errors, limit=limit)


class DagsHandler(_AirflowHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            limit = int(self.get_argument("limit", "100"))
            offset = int(self.get_argument("offset", "0"))
        except ValueError:
            self.set_status(400)
            self.finish(json.dumps({"error": "limit and offset must be integers"}))
            return
        pattern = self.get_argument("dag_id_pattern", "") or None
        await self.respond(
            get_client().list_dags,
            limit=limit,
            offset=offset,
            dag_id_pattern=pattern,
        )


class DagDetailsHandler(_AirflowHandler):
    """Full DAG detail incl. the serialized ``params`` dict — drives the manager's
    trigger-with-conf form (PRD §6.6/§15.10)."""

    @tornado.web.authenticated
    async def get(self):
        dag_id = self.get_argument("dag_id")
        await self.respond(get_client().get_dag_details, dag_id)


class DagPauseHandler(_AirflowHandler):
    @tornado.web.authenticated
    async def post(self):
        body = self.get_json_body() or {}
        dag_id = body.get("dag_id")
        is_paused = bool(body.get("is_paused"))
        if not dag_id:
            self.set_status(400)
            self.finish(json.dumps({"error": "dag_id required"}))
            return
        await self.respond(
            get_client().set_paused,
            dag_id,
            is_paused,
            audit_action="pause" if is_paused else "unpause",
            audit_dag_id=dag_id,
        )


class DagTriggerHandler(_AirflowHandler):
    @tornado.web.authenticated
    async def post(self):
        body = self.get_json_body() or {}
        dag_id = body.get("dag_id")
        if not dag_id:
            self.set_status(400)
            self.finish(json.dumps({"error": "dag_id required"}))
            return
        await self.respond(
            get_client().trigger_dag,
            dag_id,
            conf=body.get("conf") or {},
            logical_date=body.get("logical_date"),
            audit_action="trigger",
            audit_dag_id=dag_id,
        )


class DagRunsHandler(_AirflowHandler):
    @tornado.web.authenticated
    async def get(self):
        dag_id = self.get_argument("dag_id")
        try:
            limit = int(self.get_argument("limit", "10"))
        except ValueError:
            limit = 10
        await self.respond(get_client().list_dag_runs, dag_id, limit=limit)


class DagRunGetHandler(_AirflowHandler):
    """One DagRun's current state — polled by the editor's run-on-deploy banner."""

    @tornado.web.authenticated
    async def get(self):
        dag_id = self.get_argument("dag_id")
        run_id = self.get_argument("run_id")
        await self.respond(get_client().get_dag_run, dag_id, run_id)


class DagRunStateHandler(_AirflowHandler):
    """Set a DagRun's state. Used to **stop** an in-flight run (PRD §6.6): Airflow
    3 has no cancel endpoint, so stopping = PATCH the run to ``failed`` and the
    scheduler terminates its running tasks."""

    @tornado.web.authenticated
    async def post(self):
        body = self.get_json_body() or {}
        dag_id = body.get("dag_id")
        run_id = body.get("run_id")
        if not dag_id or not run_id:
            self.set_status(400)
            self.finish(json.dumps({"error": "dag_id and run_id required"}))
            return
        await self.respond(
            get_client().set_dag_run_state,
            dag_id,
            run_id,
            body.get("state") or "failed",
            audit_action="stop_run",
            audit_dag_id=dag_id,
        )


class OrphansHandler(_AirflowHandler):
    """Deployed Studio DAGs whose source `.afdag` was deleted (PRD §6.5.6). The
    reconciliation sweep diffs deployed-`.py` provenance against the `.afdag`
    files under the Jupyter Contents root; the manager surfaces the result so the
    user can undeploy them."""

    @tornado.web.authenticated
    async def get(self):
        contents_root = getattr(self.contents_manager, "root_dir", None)
        await self.respond(find_orphans, contents_root)


class DagSourceHandler(_AirflowHandler):
    """Resolve a deployed DAG back to its source `.afdag` Contents path so the
    manager can offer "Open in Studio to fix" on an import error (PRD §7).
    Returns ``{path}`` (or ``{path: null}`` when the source can't be located)."""

    @tornado.web.authenticated
    async def get(self):
        filename = self.get_argument("filename", "") or None
        dag_id = self.get_argument("dag_id", "") or None
        contents_root = getattr(self.contents_manager, "root_dir", None)
        await self.respond(
            find_source_path,
            filename=filename,
            dag_id=dag_id,
            contents_root=contents_root,
        )


class TaskInstancesHandler(_AirflowHandler):
    @tornado.web.authenticated
    async def get(self):
        dag_id = self.get_argument("dag_id")
        run_id = self.get_argument("run_id")
        await self.respond(get_client().list_task_instances, dag_id, run_id)


class TaskLogsHandler(_AirflowHandler):
    @tornado.web.authenticated
    async def get(self):
        dag_id = self.get_argument("dag_id")
        run_id = self.get_argument("run_id")
        task_id = self.get_argument("task_id")
        try:
            try_number = int(self.get_argument("try_number", "1"))
        except ValueError:
            try_number = 1
        await self.respond(
            get_client().get_task_logs, dag_id, run_id, task_id, try_number
        )


class TaskClearHandler(_AirflowHandler):
    """Clear (retry) task instances. ``dry_run`` previews the affected set first."""

    @tornado.web.authenticated
    async def post(self):
        body = self.get_json_body() or {}
        dag_id = body.get("dag_id")
        if not dag_id:
            self.set_status(400)
            self.finish(json.dumps({"error": "dag_id required"}))
            return
        dry_run = bool(body.get("dry_run", True))
        await self.respond(
            get_client().clear_task_instances,
            dag_id,
            task_ids=body.get("task_ids"),
            dag_run_id=body.get("run_id"),
            dry_run=dry_run,
            # A dry-run is a read-only preview; only audit a real clear.
            audit_action=None if dry_run else "clear",
            audit_dag_id=dag_id,
        )


class DagDeleteHandler(_AirflowHandler):
    """Delete a DAG: remove the deployed `.py` first, then purge its history.
    Also serves the editor's **Undeploy** (PRD §7) — same teardown."""

    @tornado.web.authenticated
    async def post(self):
        body = self.get_json_body() or {}
        dag_id = body.get("dag_id")
        if not dag_id:
            self.set_status(400)
            self.finish(json.dumps({"error": "dag_id required"}))
            return
        await self.respond(purge_dag, dag_id, audit_action="delete", audit_dag_id=dag_id)


class DagRollbackHandler(_AirflowHandler):
    """Roll a deployed DAG back to its previous version (PRD §6.5.5 / §7): restore
    the `.bak` saved on the last overwrite-deploy. File-only; the dag-processor
    re-imports the restored version."""

    @tornado.web.authenticated
    async def post(self):
        body = self.get_json_body() or {}
        dag_id = body.get("dag_id")
        if not dag_id:
            self.set_status(400)
            self.finish(json.dumps({"error": "dag_id required"}))
            return
        await self.respond(
            rollback_dag, dag_id, audit_action="rollback", audit_dag_id=dag_id
        )


class RenamePreflightHandler(_AirflowHandler):
    """Report a dag_id's deploy state so the editor can pick the rename path —
    draft / deployed-idle / blocked-on-active-run (PRD §6.1.8(B))."""

    @tornado.web.authenticated
    async def get(self):
        dag_id = self.get_argument("dag_id")
        await self.respond(rename_preflight, dag_id)


class DagRetireHandler(_AirflowHandler):
    """Reconcile the OLD dag_id after a rename migration: remove its file and
    either pause it (keep history) or purge it (PRD §6.1.8(B))."""

    @tornado.web.authenticated
    async def post(self):
        body = self.get_json_body() or {}
        dag_id = body.get("dag_id")
        if not dag_id:
            self.set_status(400)
            self.finish(json.dumps({"error": "dag_id required"}))
            return
        await self.respond(
            retire_old_dag,
            dag_id,
            purge=bool(body.get("purge")),
            audit_action="retire",
            audit_dag_id=dag_id,
        )


def _url(base_url, act):
    return url_path_join(base_url, NAMESPACE, act)


def setup_handlers(web_app):
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]
    handlers = [
        (_url(base_url, "health"), HealthHandler),
        (_url(base_url, "operators"), OperatorsHandler),
        (_url(base_url, "notifiers"), NotifiersHandler),
        (_url(base_url, "generate"), GenerateHandler),
        (_url(base_url, "validate"), ValidateHandler),
        (_url(base_url, "deploy"), DeployHandler),
        (_url(base_url, "deploy/status"), DeployStatusHandler),
        (_url(base_url, "importerrors"), ImportErrorsHandler),
        (_url(base_url, "dags"), DagsHandler),
        (_url(base_url, "dags/details"), DagDetailsHandler),
        (_url(base_url, "dags/pause"), DagPauseHandler),
        (_url(base_url, "dags/trigger"), DagTriggerHandler),
        (_url(base_url, "dags/delete"), DagDeleteHandler),
        (_url(base_url, "dags/rollback"), DagRollbackHandler),
        (_url(base_url, "dags/orphans"), OrphansHandler),
        (_url(base_url, "dags/source"), DagSourceHandler),
        (_url(base_url, "dags/rename/preflight"), RenamePreflightHandler),
        (_url(base_url, "dags/retire"), DagRetireHandler),
        (_url(base_url, "dagruns"), DagRunsHandler),
        (_url(base_url, "dagruns/get"), DagRunGetHandler),
        (_url(base_url, "dagruns/state"), DagRunStateHandler),
        (_url(base_url, "taskinstances"), TaskInstancesHandler),
        (_url(base_url, "taskinstances/logs"), TaskLogsHandler),
        (_url(base_url, "taskinstances/clear"), TaskClearHandler),
    ]
    web_app.add_handlers(host_pattern, handlers)
