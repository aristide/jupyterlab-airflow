import asyncio
import json
import traceback

import tornado
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join

from .client import AirflowError, get_client
from .codegen import generate_dag
from .registry import client_view

NAMESPACE = "jupyterlab-airflow"


class _AirflowHandler(APIHandler):
    """Base handler that runs the synchronous Airflow client off the event loop
    and maps :class:`AirflowError` onto a JSON error payload."""

    async def run(self, fn, *args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    async def respond(self, fn, *args, **kwargs):
        try:
            data = await self.run(fn, *args, **kwargs)
            self.finish(json.dumps({"data": data}))
        except AirflowError as err:
            self.set_status(502)
            self.finish(json.dumps({"error": str(err), "detail": err.detail}))
        except Exception as err:  # noqa: BLE001 - surface unexpected errors to UI
            self.log.error(err)
            traceback.print_exc()
            self.set_status(500)
            self.finish(json.dumps({"error": str(err)}))


class HealthHandler(_AirflowHandler):
    @tornado.web.authenticated
    async def get(self):
        await self.respond(get_client().health)


class OperatorsHandler(_AirflowHandler):
    """Serve the operator registry to the editor palette + node forms.

    This endpoint does not talk to Airflow; it reads the bundled (and optional
    user) operator YAML registry. The file I/O still runs off the event loop via
    :meth:`respond`/``run_in_executor``.
    """

    @tornado.web.authenticated
    async def get(self):
        await self.respond(client_view)


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
        await self.respond(get_client().list_dags, limit=limit, offset=offset)


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
        await self.respond(get_client().set_paused, dag_id, is_paused)


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


def _url(base_url, act):
    return url_path_join(base_url, NAMESPACE, act)


def setup_handlers(web_app):
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]
    handlers = [
        (_url(base_url, "health"), HealthHandler),
        (_url(base_url, "operators"), OperatorsHandler),
        (_url(base_url, "generate"), GenerateHandler),
        (_url(base_url, "dags"), DagsHandler),
        (_url(base_url, "dags/pause"), DagPauseHandler),
        (_url(base_url, "dags/trigger"), DagTriggerHandler),
        (_url(base_url, "dagruns"), DagRunsHandler),
    ]
    web_app.add_handlers(host_pattern, handlers)
