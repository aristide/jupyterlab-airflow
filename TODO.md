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

- [ ] Server `POST validate` + `POST deploy`: validation pipeline (PRD Appendix E), atomic shared-volume write, provenance header
- [ ] Deploy lifecycle: poll `/api/v2/dags` + `/api/v2/importErrors`, tri-state UI (Writing / Waiting / Registered-Failed-Processing)
- [ ] Manager: add import-errors view, task instances, task logs, clear/retry, delete; fix `client.list_dags` `only_active` → v2 `exclude_stale`/`paused`
- [ ] Debounce the IR→model commit to node drag-stop (currently commits on every drag tick — `StudioApp.tsx`)
- [ ] Bump `requires-python` to `>=3.9` in `pyproject.toml` (Airflow 3 needs 3.9+)

## Done

- [x] 2026-06-14 — Registry-driven RJSF forms + four inspector tabs (DAG / NODE / CODE / SAVED): `forms.ts` builds JSON-Schema/uiSchema from the registry; `AfdagForm` (RJSF + ajv8) with custom `code`/`json`/`schedule` widgets; `code`/`json` fields use an embedded CodeMirror 6 editor (`CodeMirrorField`); NODE form is registry-generated, DAG form fixed; CODE tab gains Generate-DAG + client/server validation panel; SAVED tab lists workspace `.afdag` docs via Contents API (services threaded index→factory→widget→app). Deps: @rjsf/* + @codemirror/*. Tests: `forms.spec.ts`. Follow-ups: per-node common params (retries/retry_delay/depends_on_past) need an IR slot before wiring into the NODE form + codegen `common`; SAVED deploy-status marking deferred to manager correlation.
- [x] 2026-06-14 — Server `POST generate`: IR → Airflow 3.x TaskFlow Python via `codegen.py` (Jinja2 `autoescape=False` + `pyrepr`/`pyargs`, registry templates, Kahn topo order, Appendix E validation: identifiers/cycle/`ast.parse`/`compile`, provenance header); `GenerateHandler` route; frontend `generateDag` + `CodePanel` (debounced) wired as a Build/Code tab in the inspector; tests added (codegen, endpoint, handler)
- [x] 2026-06-14 — Server `GET operators`: YAML operator registry (`jupyterlab_airflow/operators/*.yaml` + `registry.py` with mtime hot-reload + `AIRFLOW_OPERATORS_DIR` override) served via `OperatorsHandler`; `src/operators.ts` now fetches/caches the registry (`loadOperators`) instead of a hardcoded list; tests added (registry, endpoint, loader)
- [x] 2026-06-13 — Establish this `TODO.md` task-history convention (documented in `CLAUDE.md`)
- [x] 2026-06-13 — Add `CLAUDE.md` (commands + architecture for future instances)
- [x] 2026-06-13 — Scaffold the `.afdag` document widget: IR/model/factory/widget + ReactFlow editor + two-plugin wiring; `tsc` clean and jest passing
- [x] 2026-06-13 — Write the product PRD (`docs/PRD.md`) with grounded research + adversarial review
