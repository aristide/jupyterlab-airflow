# TODO — task history

Append-only log of non-trivial tasks and their status. Add a task when you start
it, flip it to **In progress**, then **Done** when finished — never delete
completed entries (this is the history). Each line: `YYYY-MM-DD — description`.
Status markers: `- [ ]` pending · `- [~]` in progress · `- [x]` done ·
`- [-]` cancelled/blocked (note why). Newest first within each group.

## In progress

_(none)_

## Pending

Next steps follow the MVP milestones in `docs/PRD.md` (§5, §14).

- [ ] Manager: add import-errors view, task instances, task logs, clear/retry, delete; fix `client.list_dags` `only_active` → v2 `exclude_stale`/`paused`
- [ ] Debounce the IR→model commit to node drag-stop (currently commits on every drag tick — `StudioApp.tsx`)
- [ ] Bump `requires-python` to `>=3.9` in `pyproject.toml` (Airflow 3 needs 3.9+)

## Done

- [x] 2026-06-15 — Deploy lifecycle (PRD §6.5.4): `AirflowClient.deploy_status(dag_id, filename)` composes `GET /importErrors` (basename-matched) + `GET /dags/{id}` (404 = not-yet) into a tri-state (`registered`/`failed`/`processing`); `list_import_errors`; `deploy/status` + `importerrors` routes. Frontend `deployStatus`/`listImportErrors` handlers; `StudioApp` runs the Writing→Waiting→(Registered|Failed|Processing) state machine with bounded backoff polling (2s→8s, 3-min timeout, cancel-on-unmount/dismiss); `DeployBanner` renders the tri-state with Unpause&trigger / traceback expander / Keep-waiting / Dismiss. Tests: client deploy_status (3 states), deploy/status + importerrors endpoints, deployStatus handler.
- [x] 2026-06-14 — Server `POST validate` + `POST deploy`: `validation.py` runs the full Appendix E pipeline (stages 1–6 from codegen + best-effort black format + stage 7 `DagBag` import in an isolated, secret-scrubbed, timed subprocess that reports `skipped` when Airflow is absent); `deploy.py` `SharedVolumeTarget` does the atomic co-located-temp + `os.replace` write with provenance-header collision safety (refuses to clobber non-Studio files), path-traversal guards, `list`/`verify`/`delete`, and `.airflowignore`; `deploy_dag` validates-then-writes. `ValidateHandler`/`DeployHandler` routes; `AIRFLOW_DAGS_DIR` config (+ devcontainer env). Frontend `validateDag`/`deployDag` + a top-bar Deploy button with status (disabled on client errors). Tests: validation, deploy, endpoints, deploy handler. Note: subprocess sandboxing is timeout + env-scrub only (full resource/network limits = hardening follow-up); the post-deploy import poll (tri-state UI) is the next task.
- [x] 2026-06-14 — Registry-driven RJSF forms + four inspector tabs (DAG / NODE / CODE / SAVED): `forms.ts` builds JSON-Schema/uiSchema from the registry; `AfdagForm` (RJSF + ajv8) with custom `code`/`json`/`schedule` widgets; `code`/`json` fields use an embedded CodeMirror 6 editor (`CodeMirrorField`); NODE form is registry-generated, DAG form fixed; CODE tab gains Generate-DAG + client/server validation panel; SAVED tab lists workspace `.afdag` docs via Contents API (services threaded index→factory→widget→app). Deps: @rjsf/* + @codemirror/*. Tests: `forms.spec.ts`. Follow-ups: per-node common params (retries/retry_delay/depends_on_past) need an IR slot before wiring into the NODE form + codegen `common`; SAVED deploy-status marking deferred to manager correlation.
- [x] 2026-06-14 — Server `POST generate`: IR → Airflow 3.x TaskFlow Python via `codegen.py` (Jinja2 `autoescape=False` + `pyrepr`/`pyargs`, registry templates, Kahn topo order, Appendix E validation: identifiers/cycle/`ast.parse`/`compile`, provenance header); `GenerateHandler` route; frontend `generateDag` + `CodePanel` (debounced) wired as a Build/Code tab in the inspector; tests added (codegen, endpoint, handler)
- [x] 2026-06-14 — Server `GET operators`: YAML operator registry (`jupyterlab_airflow/operators/*.yaml` + `registry.py` with mtime hot-reload + `AIRFLOW_OPERATORS_DIR` override) served via `OperatorsHandler`; `src/operators.ts` now fetches/caches the registry (`loadOperators`) instead of a hardcoded list; tests added (registry, endpoint, loader)
- [x] 2026-06-13 — Establish this `TODO.md` task-history convention (documented in `CLAUDE.md`)
- [x] 2026-06-13 — Add `CLAUDE.md` (commands + architecture for future instances)
- [x] 2026-06-13 — Scaffold the `.afdag` document widget: IR/model/factory/widget + ReactFlow editor + two-plugin wiring; `tsc` clean and jest passing
- [x] 2026-06-13 — Write the product PRD (`docs/PRD.md`) with grounded research + adversarial review
