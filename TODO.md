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

- [ ] Server `GET operators`: serve the YAML operator registry; replace the placeholder `src/operators.ts`
- [ ] Server `POST generate`: IR → Airflow 3.x Python (Jinja2 + registry); wire the CODE inspector tab preview
- [ ] Registry-driven RJSF forms + the four inspector tabs (DAG / NODE / CODE / SAVED)
- [ ] Server `POST validate` + `POST deploy`: validation pipeline (PRD Appendix E), atomic shared-volume write, provenance header
- [ ] Deploy lifecycle: poll `/api/v2/dags` + `/api/v2/importErrors`, tri-state UI (Writing / Waiting / Registered-Failed-Processing)
- [ ] Manager: add import-errors view, task instances, task logs, clear/retry, delete; fix `client.list_dags` `only_active` → v2 `exclude_stale`/`paused`
- [ ] Debounce the IR→model commit to node drag-stop (currently commits on every drag tick — `StudioApp.tsx`)
- [ ] Bump `requires-python` to `>=3.9` in `pyproject.toml` (Airflow 3 needs 3.9+)

## Done

- [x] 2026-06-13 — Establish this `TODO.md` task-history convention (documented in `CLAUDE.md`)
- [x] 2026-06-13 — Add `CLAUDE.md` (commands + architecture for future instances)
- [x] 2026-06-13 — Scaffold the `.afdag` document widget: IR/model/factory/widget + ReactFlow editor + two-plugin wiring; `tsc` clean and jest passing
- [x] 2026-06-13 — Write the product PRD (`docs/PRD.md`) with grounded research + adversarial review
