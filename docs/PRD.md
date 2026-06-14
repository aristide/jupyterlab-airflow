# Airflow Studio — Product Requirements Document

| | |
|---|---|
| **Product** | Airflow Studio — a no‑code / low‑code visual DAG editor **and** operations manager for Apache Airflow, delivered as a JupyterLab 4.x extension |
| **Repo** | `jupyterlab-airflow` (package `jupyterlab_airflow`) |
| **Status** | Draft v1 — for review |
| **Date** | 2026‑06‑13 |
| **Target runtime** | JupyterLab ≥ 4.1, Apache **Airflow 3.x only** |
| **Builds on** | Existing scaffolded extension: left‑sidebar DAG list + server extension proxying Airflow `/api/v2` with JWT |

> This PRD is grounded in (a) the existing codebase, (b) the reference product "Airflow Studio" (the Medium article + frame‑by‑frame analysis of its demo GIFs), and (c) research verified against current Airflow 3.x, JupyterLab 4.x, ReactFlow, and RJSF documentation. Where a claim drove a design decision, the relevant API/endpoint is named inline so engineering can act without re‑deriving it.

---

## 0. TL;DR

Airflow Studio turns Airflow DAG authoring into a drag‑and‑drop experience inside JupyterLab, while keeping the produced artifact a **real, version‑controllable `.py` DAG that Airflow runs unchanged**. It has two surfaces in one extension:

1. **Studio editor** — a main‑area document (a `.afdag` JSON graph) rendered as a ReactFlow canvas with an operator palette, a tabbed inspector (DAG / NODE / CODE / SAVED), live validation, a generated‑Python preview, and one‑click **Deploy**.
2. **Manager** — the existing left sidebar, extended into a full operations panel (list, pause, trigger, runs, **task instances, logs, import errors, retry/clear, delete**).

**Four scope decisions are locked** for this release:

1. **Deploy via shared volume first** — write the generated `.py` straight into Airflow's `dags` folder on a shared mount, behind a pluggable `DeployTarget` interface (Git / S3 are later targets).
2. **Reopen Studio‑created DAGs only** — the `.afdag` graph JSON is the source of truth; hand‑written DAGs are read‑only in the manager.
3. **Operators + code‑editor task nodes** — predefined registry operators for everyone, plus Python/`@task` "code" nodes with an embedded editor for advanced users.
4. **Airflow 3.x only** — `airflow.sdk` imports, `airflow.providers.standard.*` operators, `/api/v2`, JWT.

**The single biggest product risk** (and the thing the whole design is organized around): *Deploy ≠ Appears ≠ Runs.* Writing a file does not register a DAG — Airflow's standalone dag‑processor parses on an interval and there is **no on‑demand refresh REST API in Airflow 3.0.x**. The deploy flow must therefore be an observable, polled, tri‑state lifecycle, and the manager must surface **import errors** in plain language so a non‑technical user is never left staring at an empty list.

---

## 1. Vision & problem

Authoring Airflow DAGs requires Python fluency, knowledge of operators and their parameters, correct scheduling/`start_date`/`catchup` semantics, and a deploy workflow. This excludes analysts and domain experts who understand the *workflow* but not the *boilerplate* — which the reference app estimates at ~70% of authoring effort.

**Airflow Studio** lets a non‑technical user assemble a workflow visually and ship it, while giving advanced users a code escape hatch and a clean, reviewable `.py` artifact. It lives where data people already work (JupyterLab), reuses Jupyter's auth/server, and keeps Airflow credentials on the server.

## 2. Goals, non‑goals, success metrics

### Goals
- **G1.** A non‑technical user can build, validate, deploy, and run a simple DAG (e.g. two Bash/Python tasks in sequence) without writing Python or touching the filesystem.
- **G2.** The generated `.py` is idiomatic Airflow 3.x, parses cleanly, and is human‑readable/version‑controllable.
- **G3.** Deploy is **honest**: the user always knows whether the DAG was picked up by Airflow, is still processing, or failed to import — and *why*, in plain language.
- **G4.** Advanced users can drop into a code node without leaving the canvas, and the output stays a normal DAG.
- **G5.** The manager covers the day‑to‑day operations loop: list, trigger, watch runs, read logs, retry, delete.
- **G6.** Adding a new operator is **data‑only** (a registry YAML entry), no React/Python changes.

### Non‑goals (v1)
- **NG1.** Reverse‑engineering arbitrary hand‑written `.py` DAGs back into the canvas. (Round‑trip is Studio‑created DAGs only.)
- **NG2.** Real‑time multi‑user collaborative editing of a single `.afdag` (RTC).
- **NG3.** Replacing the Airflow web UI for deep run forensics (Gantt, lineage graphs). Studio links out / shows essentials.
- **NG4.** A full RBAC/identity system inside the extension — we lean on Airflow's auth manager and document the trust model.
- **NG5.** Git and S3 deploy targets (interface only in v1; implementations later).

### Success metrics
- **Time‑to‑first‑DAG** (open Studio → green run) < 10 minutes for a new non‑technical user.
- **Deploy clarity:** ≥ 95% of failed deploys show a node/field‑mapped, plain‑language reason (not a raw traceback).
- **Codegen correctness:** 0 import errors across the golden‑file + integration test suite on the pinned Airflow image.
- **Extensibility:** a new standard operator added by editing only registry YAML, verified by a test.

## 3. Personas & primary use cases

| Persona | Needs | Studio answer |
|---|---|---|
| **Dana — domain analyst (non‑technical)** | Schedule a recurring data pull + transform without code | Palette + forms + Deploy; plain‑language errors; guided first‑DAG |
| **Ravi — analytics/data engineer** | Standardize DAG authoring, avoid boilerplate, review diffs | Clean generated `.py`, registry conventions, `.afdag` in Git |
| **Mei — advanced platform engineer** | Custom logic, branching, sensors | Code‑editor `@task`/Python nodes, Branch/ShortCircuit, validation |
| **Sam — DAG operator/on‑call** | Trigger, watch, read logs, retry failures, pause noisy DAGs | The Manager sidebar (runs, task instances, logs, clear/retry, delete) |

**Representative user stories**
- *As Dana,* I drag a "Bash operator" onto the canvas, fill in a command, set the schedule to `@daily`, click Deploy, and within a couple of minutes see my DAG appear in the list and run green.
- *As Dana,* when my DAG fails to load, I see "Your DAG couldn't be loaded — the **Bash Command** field on node *fetch_data* is empty," with a button to fix it.
- *As Mei,* I add a Python code node, write a 10‑line transform, connect it downstream of a sensor, and the generated `@task` wraps my code correctly.
- *As Sam,* I open the sidebar, expand a DAG, see the last run failed on task *load*, read its logs, and clear/retry just that task.

## 4. Product overview — two surfaces, one extension

```
JupyterLab
├── Left sidebar  ── Manager (extends existing AirflowPanel)
│     list DAGs · pause · trigger · runs · task instances · logs · import errors · retry/clear · delete
└── Main area     ── Studio editor (new DocumentWidget on .afdag files)
      ┌───────────────────────────────────────────────────────────────────────────┐
      │ TopBar: logo · dag_id · N nodes · ✕ N errors · [Traditional|TaskFlow]       │
      │         · Undo · Reset · Save · Generate DAG · Deploy                       │
      ├───────────┬───────────────────────────────────────────┬───────────────────┤
      │ OPERATORS │  ReactFlow canvas (nodes, edges,           │ Inspector tabs:   │
      │ (palette, │   minimap, zoom controls, empty‑state)     │ DAG · NODE ·      │
      │  searchable, categorized) │                            │ CODE · SAVED      │
      └───────────┴───────────────────────────────────────────┴───────────────────┘
```

Both surfaces talk to the **same Jupyter server extension** (namespace `jupyterlab-airflow`), which (a) proxies Airflow `/api/v2` (REST, for the manager + deploy verification) and (b) owns code generation + validation + the filesystem deploy (the labextension cannot reach Airflow's dags volume).

## 5. Scope & phased release plan

The locked decisions are honored; the phasing applies the pre‑mortem's "ruthless MVP" guidance so the no‑code core is proven before the expensive long tail.

### MVP — v0.1 "vertical slice that actually runs"
- **Editor:** ReactFlow canvas, searchable/categorized palette, four inspector tabs, top‑bar with live error badge, empty‑state, minimap + zoom, save/reopen via `.afdag`.
- **Operators (core set):** `Empty`, `Bash`, `Python`/Custom `@task` (the code node — decision #3), `Branch` (BranchPython), `TriggerDagRun`. (~5–6 nodes covering the common shapes: linear, fan‑in/out, branch.)
- **Codegen:** **TaskFlow backend only** (matches the repo's existing example DAG). The Traditional↔TaskFlow *toggle* is built into the IR/UI but defaults to (and only emits) TaskFlow in MVP. *Rationale: shipping both backends doubles the codegen + test surface; see §6.3.*
- **Validation:** client‑side cycle detection + required‑field checks → live error badge & node dots; **server‑side authoritative re‑validation + parse‑check** before deploy.
- **Deploy:** `SharedVolumeTarget` (atomic write) + **lifecycle polling** (appears? import error?) with tri‑state UI.
- **Manager:** extend sidebar with **import errors**, **task instances + states**, **task logs**, **clear/retry**, **delete** (file + history), plus the existing list/pause/trigger/runs.
- **Foundations:** `DeployTarget` interface, operator‑registry mechanism, provenance + collision/namespacing model, secrets guidance, accessibility baseline.

### v1.1 — "dual syntax & breadth"
- Traditional operator codegen backend + the working **Traditional↔TaskFlow toggle** (with golden‑file equivalence tests).
- More standard operators + first provider operators (HTTP, common sensors) with provider‑availability validation.
- One‑click **Tidy layout** (dagre), richer undo/redo, optional minimap toggle.

### v1.2 — "beyond a single shared volume"
- **Git** and **S3 / object‑storage** `DeployTarget` implementations (Airflow DAG‑bundle aware).
- **Per‑user identity** on JupyterHub (Hub‑injected Airflow creds / OIDC) + Studio action audit trail.
- Asset/dataset‑driven scheduling; provider sensor catalog (GCS/BigQuery/EMR/Glue/Dataproc) gated on installed providers.

### Explicitly deferred / out
Arbitrary `.py` import to canvas (NG1); RTC (NG2); in‑extension RBAC engine (NG4).

---

## 6. Functional requirements

### 6.1 Visual DAG Editor

**6.1.1 Canvas (ReactFlow, `@xyflow/react` v12).**
- Controlled graph via `useNodesState` / `useEdgesState`; one **node = one Airflow task**, one **edge = one dependency** (`a >> b`).
- Custom node card: category label, operator name, `task_id`, a **validity indicator that is icon + text, not color‑only** (a11y), source/target `Handle`s. Branch/ShortCircuit nodes expose multiple labeled source handles for follow‑paths.
- `onConnect` creates a dependency edge; `isValidConnection` rejects self‑loops and (optionally) duplicate edges. Arrow markers (`MarkerType.ArrowClosed`).
- `Background`, `MiniMap` (bottom‑right), `Controls` (bottom‑left) — matching the reference UI. Empty‑state overlay "Drop operators here" when `nodes.length === 0`.
- Drag‑from‑palette: HTML5 DnD writes the operator id to `dataTransfer`; canvas `onDrop` uses `screenToFlowPosition` and creates a node with an auto‑generated `task_id` (e.g. `bash_6`).
- **Performance:** `nodeTypes`/`edgeTypes` defined at module scope; handlers `useCallback`; node component `React.memo`; narrow store selectors. (DAGs are typically tens of nodes; memoization matters more than viewport culling.)
- **Accessibility (required, not optional):** a keyboard path to add a node (palette → Enter), connect nodes (select source → "connect to…" → target), and edit it (open inspector). Drag‑drop is an *enhancement*, never the only way. Full ARIA labeling; focus management across inspector tabs.

**6.1.2 Operator palette (left).** Searchable, grouped by category (Python/Bash, Flow Control, HTTP, Sensors…). **Generated from the operator registry** (a `GET operators` server endpoint at activation, cached). Each item shows label + category and is draggable / keyboard‑activatable.

**6.1.3 Inspector tabs (right).**
- **DAG** — `dag_id`, description, **schedule** (dropdown of presets `@once/@hourly/@daily/@weekly/@monthly/None` + custom cron + `timedelta`), `start_date` (date picker), `catchup` (**default false** — Airflow 3 default), `retries`, `retry_delay`, `tags`, `owner`, `params`, `default_args`.
- **NODE** — operator‑specific form **generated from the registry** (see §6.2), with required‑field validation feeding the error badge; common fields (`retries`, `retry_delay`, `depends_on_past`); JSON/dict fields (env vars, params) via a JSON editor widget; code fields via an embedded CodeMirror editor.
- **CODE** — live generated‑Python preview (read‑only), a **Generate DAG** button, and a validation panel that shows **both** client‑side messages (e.g. *"DAG contains a cycle — Airflow does not support cyclic dependencies"*) **and**, after deploy, the **authoritative Airflow import status**.
- **SAVED** — lists `.afdag` documents in the workspace (via Contents API) to reopen; marks which are deployed.

**6.1.4 Top bar.** Logo · live `dag_id` · node count · **live error badge** (`✕ N errors`, with text not just color) · Traditional↔TaskFlow toggle (v1.1; disabled/ taskflow‑locked in MVP) · Undo · **Reset** (revert to last saved IR) · **Save** (writes the `.afdag` via the document context) · **Generate DAG** (server codegen preview) · **Deploy**.

**6.1.5 Save / reopen.** The editor is a JupyterLab **document** bound to the `.afdag` file; Save/dirty/restore come from the Contents API. Reopening loads the IR (never the generated `.py`). See §8.2–8.3.

### 6.2 Operator registry

A directory of **YAML files, one per operator**, read by **both** the client (palette + form schema) and the server (Jinja2 codegen). Adding an operator is pure data — no React/Python edits (G6). Each entry declares: `id`, `label`, `category`, `provider` + `airflow_min_version`, the **import line(s)**, required/optional **params** (name, type, widget, default, validation, required), `common_params`, handle topology, `task_id_prefix`, and **two code templates** (`template_traditional`, `template_taskflow`). See **Appendix A**.

Requirements:
- The registry is the single source of truth for: palette grouping/search, NODE‑tab JSON Schema (rendered with RJSF), and server codegen import paths + templates.
- A param `widget: code` (Python) or `widget: json` (dict) selects the embedded editors.
- Each entry records its **provider package** so the system can warn when an operator's provider isn't installed in the *target Airflow* (not just the Jupyter env).
- Operators with no TaskFlow equivalent (`Empty`, `TriggerDagRun`) declare `taskflow: operator` so the toggle renders them as operators even in TaskFlow mode.

### 6.3 Code generation

- **Authoritative codegen is server‑side** (Python + Jinja2), because only the server can parse‑check against an Airflow install and because templates + import paths live with the deploy target. Client TS does *instant, non‑authoritative* hints only.
- The **IR is syntax‑agnostic**; the syntax mode selects a template family:
  - **TaskFlow** (`from airflow.sdk import dag, task`): `@dag(...)` wrapping `@task`‑decorated functions; dependencies expressed by function calls and/or `chain(...)`. Code nodes are TaskFlow‑native.
  - **Traditional** (`from airflow.sdk import DAG` + provider operator imports): `with DAG(...) as dag:` + operator instances + `>>`/`chain()`/`cross_downstream()` from the edge list. A code node renders as `PythonOperator(python_callable=...)`.
- **Airflow 3.x correctness (verified):** emit `airflow.sdk` for `DAG`/`dag`/`task`/`chain`, and **`airflow.providers.standard.*`** for operators/sensors. **Never** emit Airflow‑2 paths (`airflow.operators.bash`, `airflow.models.DAG`, `airflow.decorators.task`) — they fail to import in Airflow 3. Defaults: `catchup=False`; `retry_delay` as `timedelta`; `start_date` as `datetime`; `schedule` handled distinctly for `None`/preset/cron/`timedelta`.
- **Determinism:** format output with `black`/`ruff format` so identical IR → byte‑identical file (idempotent deploys, clean diffs for the future Git target).
- **Toggle = two backends that must be semantically equivalent.** This is a top correctness risk; v1.1 ships it only with golden‑file equivalence tests (§10). MVP emits TaskFlow only.

See **Appendix C** for example output.

### 6.4 Validation & live errors

Two layers (client = instant UX, server = authority):

- **Client (instant):** Kahn topological sort for **cycle detection** (also yields a topo order for codegen) → drives the cycle message; per‑node **required‑field** checks from the registry → red/green (icon+text) node dots; the top‑bar badge = `cycleError + Σ node errors`. The IR is the single source of truth; ReactFlow state and RJSF form data are projections.
- **Server (authoritative, before deploy):** re‑validate the untrusted `.afdag` IR (schema + cycle + required), sanitize identifiers, render, then run the parse pipeline (Appendix E). **Client validation is never trusted** — the IR is just JSON a client can hand‑craft.
- **Post‑deploy (the real verdict):** Airflow's own parser. Studio polls `/api/v2/importErrors` and surfaces the result. *The server parse‑check is explicitly best‑effort* (Jupyter env ≠ Airflow worker env; provider packages/connections may differ).

### 6.5 Deployment & sharing

**6.5.1 `DeployTarget` interface** — `write(filename, content)` (atomic), `exists`, `list` (managed files + provenance), `read`, `delete`, `verify`, and a **consistency flag** (synchronous‑visible vs eventually‑consistent) so the verification poll adapts. v1 ships `SharedVolumeTarget`; Git/S3 implement the same interface later (mapping to Airflow Git/S3 **DAG bundles**).

**6.5.2 Shared‑volume deploy (atomic).** Write a temp file **in the same directory** as the target, `fsync`, then `os.replace(tmp, final)` (atomic + overwrite on POSIX/Windows; cross‑filesystem rename is **not** atomic, so temp must be co‑located). Filename is deterministic and **namespaced** (see §8.9). Drop an `.airflowignore` (glob syntax in Airflow 3) covering the temp/staging pattern and `.afdag` sidecars.

**6.5.3 Collision & overwrite safety.** Before writing: read back the target dir; **refuse to overwrite any file lacking the Studio provenance header** (it's a hand‑written, read‑only DAG); detect `dag_id` duplication; on a managed file that was hand‑edited (body hash ≠ recorded `ir-hash`), prompt *"modified outside Studio — reopen read‑only or overwrite?"* See §9.

**6.5.4 Deploy lifecycle (the central success path).** Because Airflow 3 has **no on‑demand bundle‑refresh REST API** and the dag‑processor scans on `refresh_interval` / re‑parses on `min_file_process_interval` (and standalone has a known refresh‑timing bug), Deploy is an **observable tri‑state**:
1. *Writing…* → atomic write succeeds.
2. *Waiting for Airflow to pick it up…* → poll `GET /api/v2/dags` for the `dag_id` **and** `GET /api/v2/importErrors` filtered to the filename, with bounded backoff and an explicit timeout (communicate "up to a few minutes").
3. Resolve to **Registered** (dag appears, no import error) · **Failed to import** (import error → friendly message + traceback expander + map to node/field) · **Still processing** (timeout → keep polling / let the user dismiss).
- On success, the DAG is created **paused**; offer "unpause & trigger".

### 6.6 Resource Manager (sidebar, extended)

Extends the existing `AirflowPanel`. Requirements (endpoints in Appendix D):
- **List** with tag filter + `dag_id` search; flag DAGs with `has_import_errors=true`. *(Fix the existing `only_active` → v2 `exclude_stale`/`paused`; send list params form‑exploded.)*
- **DAG detail / source** (read‑only view for hand‑written DAGs via `dagSources`).
- **Pause/unpause** (existing, correct).
- **Trigger** with a **conf form derived from the DAG's `params`** (`/dags/{id}/details`); allow null `logical_date` for an immediate run (Airflow 3).
- **Runs** → **task instances + states** → **task logs** (paged by continuation token, tail while running).
- **Import errors** view (`/api/v2/importErrors`) — *the recovery surface*; translate `stack_trace` to plain language.
- **Clear/retry** (`clearTaskInstances`, `dry_run` preview first) and **mark success/failed/skipped** (with dry‑run preview).
- **Delete** = remove the namespaced `.py` + `.afdag` via `DeployTarget` **first** (so it isn't re‑imported), **then** `DELETE /api/v2/dags/{id}` to purge history; irreversible‑action confirmation.
- **Refresh:** tiered visibility‑gated polling keyed off `autoRefreshSeconds` (collapsed list ~15–30s; active run 3–5s; open running‑log tail 2–3s); pause when hidden/offscreen; back off on 429/5xx. (No websockets in Airflow `/api/v2`; the experimental single‑run `wait` ndjson stream may be proxied later.)

### 6.7 Advanced code‑editor task nodes (decision #3)

- A registry entry whose single param is `code` (`widget: code`, CodeMirror 6 reused from JupyterLab). The user's code is emitted **inside** a `@task` function body (TaskFlow) or wrapped as `PythonOperator(python_callable=...)` (Traditional) — **never at module top level**, so a user error can't break the whole file's import.
- **This is an intentional arbitrary‑code‑execution surface** (the code runs on Airflow workers with their privileges). It is governed by the trust boundary in §9: linted via AST/ruff, parse‑checked in an isolated subprocess, gated by who may deploy, and documented. For the non‑technical majority the code editor is hidden unless a Python/Custom‑`@task` node is selected.

---

## 7. UX / UI specification

- **Layout & theming.** Match the reference UI shape (top bar / palette / canvas / inspector). Style **exclusively with JupyterLab CSS variables** (`--jp-layout-color*`, `--jp-ui-font-color*`, `--jp-border-color*`, `--jp-brand-color1`, `--jp-error-color1`, `--jp-success-color1`); map ReactFlow's CSS vars onto `--jp-*` so dark mode reskins automatically.
- **First‑run onboarding.** Beyond "Drop operators here," provide a guided first‑DAG (seed a template DAG config; a 3‑step coachmark: add node → configure → deploy).
- **Deploy feedback.** A persistent tri‑state indicator (Writing / Waiting / Registered‑Failed‑Processing) with timeout copy; never a silent success.
- **Failure recovery (make‑or‑break).** On import error: pull `stack_trace`, **map back to the offending node/field** where possible, show a plain‑language card ("Your DAG couldn't be loaded — …") with a *Show technical details* expander and a **one‑click "Open in Studio to fix"** that loads the `.afdag` (not the broken `.py`). Provide **undeploy / rollback to previous working version**.
- **Conflict/overwrite UX.** Clear dialogs for: filename/`dag_id` already exists; about to clobber another user's DAG; `.py` modified outside Studio.
- **Severity language.** The error badge, node dots, CODE‑tab messages, and post‑deploy import status share one severity vocabulary and surface **both** client validation and Airflow's verdict.
- **Manager safety.** Trigger/pause/delete/clear show confirmations and (in multi‑user) attribution; dry‑run previews for clear/mark‑state.
- **Accessibility (WCAG).** Keyboard‑operable canvas alternative; non‑color‑only state (icon+text+ARIA) on the badge and node dots; screen‑reader labels; inspector focus order. **Color is never the only signal.**
- **i18n.** All Studio chrome via `trans.__()`. Explicitly **not localized:** raw Airflow error strings, generated code, user code. State this in‑product.

---

## 8. Architecture & implementation guidelines

### 8.1 High‑level architecture

```
 Browser (labextension, TS/React 18)                 Jupyter server (Python ext)            Airflow 3.x
 ┌───────────────────────────────┐     requestAPI    ┌──────────────────────────────┐  REST  ┌──────────────┐
 │ Manager sidebar (AirflowPanel)│◀────/api──────────▶│ handlers.py (Tornado APIHandler│──/api/v2─▶│ /auth/token │
 │ Studio editor (DocumentWidget │   jupyterlab-      │   + thread‑pool executor)      │  JWT   │ /dags, /dagRuns│
 │   on .afdag → ReactFlow + RJSF)│   airflow ns      │ AirflowClient (REST proxy)     │        │ /taskInstances │
 └───────────────────────────────┘                   │ Codegen (Jinja2 + registry)    │        │ /importErrors │
        │  Contents API (.afdag in workspace)         │ Validation pipeline (Appendix E)│       └──────────────┘
        ▼                                             │ DeployTarget → atomic write ───┼──── shared volume ──▶ /opt/airflow/dags
 JupyterLab Drive                                     └──────────────────────────────┘
```

Ship **two `JupyterFrontEndPlugin`s** from one index (array default export) sharing `CommandIDs` and server endpoints:
- `jupyterlab-airflow:plugin` — the Manager (left area; existing).
- `jupyterlab-airflow:editor` — the Studio document factory (main area; new). Lazy‑load the heavy editor bundle so the lightweight manager isn't penalized.

### 8.2 Frontend: JupyterLab integration

Register a custom file type + document widget so JupyterLab owns open/save/dirty/restore:
- `app.docRegistry.addFileType({ name:'afdag', displayName:'Airflow DAG', extensions:['.afdag'], mimeTypes:['application/json'], fileFormat:'text', contentType:'afdag', icon: airflowIcon })`.
- `addModelFactory(new AfdagModelFactory())` — implements `DocumentRegistry.IModelFactory` (`name:'afdag-model'`, `contentType`, `fileFormat:'text'`, `createNew`). **Use `fileFormat:'text'`** and serialize JSON yourself in `toString`/`fromString` (avoids Contents `'json'` format constraints).
- `addWidgetFactory(new AfdagWidgetFactory({ name:FACTORY, modelName:'afdag-model', fileTypes:['afdag'], defaultFor:['afdag'] }))` extending `ABCWidgetFactory`; `createNewWidget(context)` returns `AfdagDocWidget extends DocumentWidget<AfdagEditorPanel, AfdagModel>`.
- **Model** implements `DocumentRegistry.IModel` directly (mirror `extension-examples/documents/model.ts`): `dirty`, `readOnly`, `contentChanged`, `stateChanged`, `toString/fromString`, `toJSON/fromJSON`, a `sharedModel` (YDocument storing the whole IR as one source string; RTC off in v1). Set `dirty=true` on IR mutation.
- **Content panel** = `AfdagEditorPanel extends ReactWidget` (reuse the repo's `AirflowPanel` pattern; import `ReactWidget`/`UseSignal` from **`@jupyterlab/ui-components`** in JL4). Wrap app in `<ReactFlowProvider>`; drive React from `model.contentChanged`. **Override `onResize`/`onAfterShow`** to bump state so ReactFlow (ResizeObserver) remeasures — otherwise the canvas renders 0×0.
- **React singletons:** keep `jupyterlab.sharedPackages.react`/`react-dom` = `{ bundled:false, singleton:true }` (already correct). **Do not** singleton `@xyflow/react`.
- **New‑file / open / commands:** a "New Airflow DAG" command runs `docmanager:new-untitled` (`ext:'afdag'`) then `docmanager:open` (`factory:FACTORY`); surface in `ILauncher` (category "Airflow"), `ICommandPalette`, and `app.contextMenu` (selector `.jp-DirListing-item`). Resolve the target folder via `IFileBrowserFactory.tracker.currentWidget.model.path`.
- **Restore:** `WidgetTracker({namespace:'airflow-studio'})` + `restorer.restore(tracker, { command:'docmanager:open', args: w=>({path:w.context.path, factory:FACTORY}), name: w=>w.context.path })`. Leave the sidebar's existing `restorer.add` untouched (distinct trackers; no conflict).
- **Forms:** RJSF (`@rjsf/core` + `@rjsf/validator-ajv8`) rendered from registry‑derived JSON Schema + uiSchema; custom widgets: `json` (JSON/dict editor), `code` (CodeMirror 6 from `@jupyterlab/codemirror`), schedule/date pickers. `onChange` writes back into the IR (single source of truth).
- **Layout:** `@dagrejs/dagre` (maintained fork) for one‑click "Tidy layout" (v1.1); elkjs behind a flag for dense graphs.
- **New deps:** `@xyflow/react`, `@rjsf/core`, `@rjsf/validator-ajv8`, `@dagrejs/dagre`, plus `@jupyterlab/docregistry`, `@jupyterlab/docmanager`, `@jupyterlab/launcher`, `@jupyterlab/filebrowser`, `@jupyterlab/codemirror`.

### 8.3 The `.afdag` document & IR schema

Versioned IR JSON: `{ schema_version, provenance, syntax_style, dag, nodes[], edges[], layout? }`. `node.id` is the stable ReactFlow id; `task_id` is the Airflow id (validated identifier, unique). `op` references a registry id (keeps IR decoupled from operator impl). `position` lives in the IR so layout round‑trips. `provenance` (`afdag_id`, `studio_version`, `ir-hash`) is **also embedded in the generated `.py`** so the manager can tell Studio‑created (editable) from hand‑written (read‑only) DAGs and detect drift. See **Appendix B**.

### 8.4 Codegen pipeline & trust boundary

Server‑side, in this fixed order, **short‑circuit on first failure**, each error tagged with its stage + line/stacktrace for the CODE tab (full detail in **Appendix E**):
1. IR schema validation (jsonschema/pydantic).
2. Graph semantics — cycle detection + required‑field checks (mirrors the client; client is untrusted).
3. **Identifier safety** — `dag_id`/`task_id` must be `str.isidentifier()` and not `keyword.iskeyword()` (reject soft keywords/dunders); deterministic de‑dup/sanitization **before** templating.
4. Jinja2 render with **`autoescape=False`** (HTML escaping corrupts Python). All values emitted via a safe emitter: `repr()`/`json.dumps` for strings, `json.dumps`→Python‑literal for dicts. **No raw interpolation, ever.** Templates come only from the registry (no user‑supplied template strings).
5. `ast.parse` (syntax; **no execution** — safe on untrusted input).
6. `compile(src, filename, 'exec')` (stricter; still no execution).
7. **`DagBag` import in an isolated subprocess** — `from airflow.dag_processing.dagbag import DagBag` (Airflow‑3 path), check `import_errors == {}` and `get_dag(dag_id)`. **This executes top‑level code** → the trust boundary sits between (6) and (7). Run with resource limits (CPU/mem/wall‑time), restricted env (no Airflow secrets/connections), controlled network egress.
- **Format** (black/ruff) after (6) passes, before write.
- **Trust statement:** registry operators = constrained/trusted templates; **code nodes = arbitrary user Python**, bounded to the subprocess at validation time and to the Airflow worker (which already trusts any DAG in its bundle) at run time. Treat "who can write to the dags folder" as "who can run code as the Airflow worker."

### 8.5 Operator registry implementation

- Location: bundled with the extension (default) + an optional user/server config dir for custom operators; server reads via `yaml.safe_load`. A `GET operators` endpoint serves the client palette/schema; consider hot‑reload (re‑scan on change) so adding YAML doesn't require a server restart.
- Jinja2 `Environment(autoescape=False)` with custom filters `pyrepr` (safe literal) and `pyargs` (common‑params kwargs). De‑duplicate collected import lines.

### 8.6 Server extension endpoints

Reuse the existing `_AirflowHandler.respond` + `run_in_executor` pattern and `url_path_join(base_url, 'jupyterlab-airflow', act)`. **Existing:** `health`, `dags`, `dags/pause`, `dags/trigger`, `dagruns`. **Add:** `operators` (registry), `generate` (IR→validated code preview), `validate`, `deploy` (validate→format→atomic write→verify), `dags/details`, `dags/source`, `dags/delete`, `dagruns/state`, `dagruns/clear`, `taskinstances`, `taskinstances/logs`, `taskinstances/state`, `taskinstances/clear`, `importerrors`, `assets/events`. Extend `AirflowClient` with one method per endpoint group (Appendix D). **Fix** `list_dags` v2 param drift.

### 8.7 `DeployTarget` abstraction

Interface in §6.5.1. `SharedVolumeTarget` reads its dags path from an env var (e.g. `AIRFLOW_DAGS_DIR`, default the mounted `/opt/airflow/dags`). Owns **namespacing** (so Git/S3 reuse it) and the atomic write. Git target → commit/push (+ Airflow `GitDagBundle`); S3 target → put objects (+ S3 bundle). The consistency flag drives the verification‑poll timeout.

### 8.8 Airflow 3.x integration specifics

- REST `/api/v2` (FastAPI), JWT via `POST /auth/token` → Bearer (already implemented). `execution_date` is gone → `logical_date` (nullable for now‑runs). Pause = `PATCH /dags/{id}?update_mask=is_paused`. Trigger = `POST /dags/{id}/dagRuns {logical_date?, conf}`.
- Default DAG bundle `dags-folder` = `LocalDagBundle` over `[core] dags_folder` — the shared‑volume model needs **no bundle reconfiguration**. `LocalDagBundle` has **no versioning** (always runs latest on disk) → don't edit a deployed file during an active run.
- `.airflowignore` default syntax is **glob** in Airflow 3 (was regexp).
- **Discovery latency is real:** `dag_dir_list_interval` (~300s) for new files, `min_file_process_interval` (~30s) for changed ones; no on‑demand refresh API → §6.5.4 polling is mandatory.

### 8.9 File layout, naming, namespacing, provenance

- **One DAG per file.** Deterministic, sanitized filename. **Namespace per user** in shared deployments: `users/{username}/{slug}.py`, `dag_id = f"{username}__{slug}"`, DAG `owner = username`. Path‑traversal safe (reject `..`, absolute paths, symlinks).
- `.afdag` source of truth lives in the **Jupyter workspace** (Contents‑API reachable for SAVED/reopen); the `.py` is deployed to the shared volume. Re‑associate via the embedded `afdag_id`/`ir-hash`.
- Provenance header in the `.py` (managed flag, `studio_version`, `ir-hash`, `dag_id`, syntax mode, correlation id) → distinguishes editable vs read‑only and detects out‑of‑band edits.

---

## 9. Security, multi‑user & governance

- **Deploy is privileged.** Writing a `.py` into the dags folder == running code as the Airflow worker (with its connections/secrets/cloud creds). Treat the `deploy` endpoint as a privileged operation, **not** a default‑on capability for every Jupyter user. Document who may deploy.
- **Codegen is a security‑critical compiler.** Safe literal emission only (§8.4); Bash/HTTP/env values escaped, never shell/path‑concatenated; the `.afdag` is **untrusted adversarial JSON** — schema‑validate and re‑run checks server‑side and bound sizes.
- **Code nodes** = arbitrary code; lint + isolated‑subprocess validation; document the blast radius; (later) optional review/approval gate or separate worker queue.
- **Multi‑user reality.** Today the server uses **one shared service account** (process‑wide env creds, one module‑global cached JWT). On JupyterHub each user gets their own server process, so for real per‑user attribution/authorization, inject **per‑user Airflow creds/OIDC** at spawn (`c.Spawner.environment`/`auth_state`); keep env‑var creds as a single‑user/dev fallback. **Document prominently** that, until then, any Jupyter user acts as one Airflow admin and the shared dags folder is a shared trust boundary (Airflow's multi‑team isolation is experimental and does not isolate task execution/secrets).
- **Collision protection** (§6.5.3): pre‑write uniqueness/ownership check; refuse to overwrite non‑Studio files; duplicate‑`dag_id` handling; "modified outside Studio" flow.
- **Secrets guidance.** Steer users to **Airflow Connections/Variables** instead of pasting API keys/passwords into env‑var/HTTP/code fields (which would be written in plaintext into the dags folder and `.afdag`). Warn on `AIRFLOW_VERIFY_SSL=false` for any non‑local target (MITM of JWT).
- **Token lifecycle.** The single cached JWT refreshed once on 401 is fragile under rotation/clock skew; make it per‑process and, with Hub‑injected tokens, refresh from the Hub/auth_state rather than re‑POSTing static creds.
- **Audit.** Log every deploy/trigger/delete/clear with `{user, action, dag_id, correlation_id}` even before full per‑user identity lands.

## 10. Testing & QA strategy

- **Golden‑file tests:** IR → expected `.py` for **every operator** and **every escaping edge case** (quotes, newlines, unicode, backslashes, dict/JSON params, reserved/duplicate `task_id`s, identifier sanitization).
- **Round‑trip property test:** IR → `.py` → reopen `.afdag` → identical IR.
- **Toggle equivalence (v1.1):** Traditional and TaskFlow output for the same IR parse to semantically equivalent DAGs.
- **Real‑Airflow integration:** parse generated DAGs in the pinned `apache/airflow:3.0.2` image; assert **zero import errors** and a **successful run** — not just `compile()`.
- **REST contract tests:** new `/api/v2` endpoints (importErrors, taskInstances, logs, clear/retry, delete) — shapes differ from `/api/v1`.
- **Concurrency:** two simultaneous deploys to the shared folder; collision/overwrite behavior.
- **Security:** injection attempts via params/code nodes; path‑traversal filenames; oversized/adversarial `.afdag`.
- **Frontend:** validation (cycle/required) unit tests; a11y (keyboard path, ARIA) checks; existing jest setup extended.
- **Env fix to verify:** bump `requires-python` to ≥ 3.9 (Airflow 3 needs 3.9+); current `>=3.8` is inconsistent if the validator imports airflow.

## 11. Observability & telemetry

Structured per‑request server logs `{user, action, dag_id, airflow_status, latency_ms, correlation_id}`; counters `deploy_success` / `deploy_parse_error` / `trigger` / `clear` / `log_fetch` + latency histograms for Airflow round‑trips; a correlation id shared between the `.py` provenance and logs (trace a failed import back to a Studio session); a diagnostics view backed by `health`. Optionally forward to OpenTelemetry to correlate with Airflow's own OTel traces.

## 12. Risks, assumptions & mitigations

| # | Risk / assumption | Mitigation |
|---|---|---|
| R1 | **Deploy ≠ appears ≠ runs**; latency + no on‑demand refresh API | Tri‑state polled lifecycle (§6.5.4); honest timeout copy |
| R2 | Server parse‑check is **false‑green** (Jupyter env ≠ Airflow env, missing providers) | Authoritative verdict from `/importErrors`; validate with the worker image/venv; registry records provider deps |
| R3 | **Codegen injection / broken Python** into an executed folder | Safe literal emission, `autoescape=False`, golden + security tests (§8.4, §10) |
| R4 | **Shared‑folder collisions** (duplicate `dag_id`, clobbering) | Namespacing + pre‑write ownership check + provenance refuse‑overwrite (§8.9, §9) |
| R5 | **Round‑trip drift** (`.py` hand‑edited; `.afdag`/`.py` two sources) | `ir-hash` checksum; "modified outside Studio" reopen flow |
| R6 | **Single shared admin** → no attribution/authz; fragile cached JWT | Hub‑injected per‑user creds (v1.2); audit now; per‑process token |
| R7 | **Toggle** = two backends that can silently diverge | Defer to v1.1 with equivalence golden tests; TaskFlow‑only MVP |
| R8 | **Code node = RCE** on shared workers | Isolated‑subprocess validation; deploy is privileged; document; (later) sandbox/queue |
| R9 | **Scope creep** (sensors, Git/S3, dual backend) | Phased plan §5; keep only the `DeployTarget` interface in v1 |
| R10 | **Prod may not have a writable shared volume** | `DeployTarget` is load‑bearing from day one, not "later" |

## 13. Open questions / decisions needed

1. **Where does the parse‑check run?** Jupyter and Airflow are separate containers; the Jupyter ext can't `import airflow` to DagBag‑check. Options: (a) `py_compile` in Jupyter + rely on post‑deploy `/importErrors`; (b) exec/`reserialize` in the Airflow container; (c) ship a thin matching airflow venv in the Jupyter image for validation. **Recommendation:** (a) for MVP + always poll `/importErrors`; pursue (c) for fidelity.
2. **Pin the Airflow + providers versions** for the devcontainer and validator; confirm `airflow.dag_processing.dagbag` path and standard‑provider module names on the pinned `3.0.2` image.
3. **JupyterLab minor target** (repo pins `^4.1.6`); `IContentProvider`/`contentProviderId` need 4.4+ (not required for v1).
4. **JupyterHub credential injection mechanism** (shared OIDC IdP vs per‑user `auth_state`) — sets the token‑refresh path and whether `/auth/token` is used per user.
5. **`/importErrors` server‑side filename filtering** — confirm against the running instance's OpenAPI, else fetch + match client‑side.
6. **Branch/ShortCircuit multi‑output modeling** in the IR/edges (labeled edges vs multiple source handles) and its render to `BranchPythonOperator` follow‑paths.
7. **Code node in Traditional mode** — wrap as `PythonOperator(python_callable=...)` vs force TaskFlow.
8. **Validation subprocess sandbox policy** (CPU/mem/wall‑time, network egress) — concrete since code nodes are arbitrary by design.

## 14. Milestones & acceptance criteria

| Milestone | Acceptance |
|---|---|
| **M0 — Editor shell** | `.afdag` opens as a ReactFlow document; add/connect/delete nodes; save/reopen; dirty‑state; restore after reload |
| **M1 — Registry + forms** | Palette + NODE forms generated from registry YAML for the core operator set; adding a YAML operator needs no code change (test) |
| **M2 — Validation** | Client cycle/required → error badge + node dots; server re‑validates untrusted IR |
| **M3 — Codegen (TaskFlow)** | IR → idiomatic Airflow‑3 TaskFlow `.py`; golden‑file tests green; safe literal emission verified by escaping tests |
| **M4 — Deploy + lifecycle** | Atomic namespaced write; tri‑state polling; integration test deploys to `apache/airflow:3.0.2`, asserts **zero import errors + a green run** |
| **M5 — Manager ops** | Import‑errors view, task instances, logs, clear/retry, delete (file+history); list param drift fixed |
| **M6 — Recovery UX + a11y** | Friendly import‑error → node/field mapping + "Open in Studio to fix" + undeploy; keyboard path + non‑color‑only indicators |
| **v1.1** | Traditional backend + working toggle (equivalence tests); Tidy layout; more operators |
| **v1.2** | Git + S3 `DeployTarget`; per‑user identity + audit; asset scheduling |

---

## Appendix A — Operator registry YAML (example)

```yaml
id: bash
label: Bash operator
category: Python/Bash
provider: apache-airflow-providers-standard      # bundled with Airflow 3 base image
airflow_min_version: '3.0'
import: 'from airflow.providers.standard.operators.bash import BashOperator'
import_taskflow: 'from airflow.sdk import task'
handles: { in: true, out: true }                 # branch sets out: [true, false]
taskflow: native                                 # native | operator (Empty/TriggerDagRun = operator)
task_id_prefix: bash                             # -> bash_6, bash_7
params:
  - { name: bash_command, label: 'Bash Command', type: string, required: true, widget: textarea }
  - { name: env,          label: 'Environment Vars', type: object, required: false, widget: json, default: {} }
  - { name: cwd,          label: 'Working dir', type: string, required: false }
common_params: [retries, retry_delay, depends_on_past]
template_traditional: |
  {{ task_id }} = BashOperator(
      task_id={{ task_id | pyrepr }},
      bash_command={{ params.bash_command | pyrepr }},
      {% if params.env %}env={{ params.env | pyrepr }},{% endif %}
      {{ common | pyargs }}
  )
template_taskflow: |
  @task.bash(task_id={{ task_id | pyrepr }})
  def {{ task_id }}():
      return {{ params.bash_command | pyrepr }}
```

A **code node** is a registry entry whose param is `{ name: code, type: string, widget: code }` and whose template emits the user's body verbatim **inside** a `@task` function (the only place inline code is allowed).

## Appendix B — IR JSON (example `.afdag`)

```json
{
  "schema_version": "1.0",
  "provenance": {
    "generator": "airflow-studio",
    "studio_version": "0.1.0",
    "afdag_id": "uuid-…",
    "ir_hash": "sha256-…",
    "created_at": "2026-06-13T10:00:00Z",
    "updated_at": "2026-06-13T10:05:00Z"
  },
  "syntax_style": "taskflow",
  "dag": {
    "dag_id": "my_etl", "description": "", "schedule": "@daily",
    "start_date": "2026-01-01", "catchup": false,
    "retries": 1, "retry_delay_seconds": 300,
    "tags": ["studio"], "owner": "dana", "params": {}, "default_args": {}
  },
  "nodes": [
    { "id": "n1", "op": "bash", "task_id": "extract",
      "params": { "bash_command": "echo hi", "env": {} }, "code": null,
      "position": { "x": 120, "y": 80 } },
    { "id": "n2", "op": "python_task", "task_id": "transform",
      "params": {}, "code": "def transform(value):\n    return value.upper()",
      "position": { "x": 360, "y": 80 } }
  ],
  "edges": [ { "source": "n1", "target": "n2" } ]
}
```

## Appendix C — Generated DAG (Airflow 3.x)

**TaskFlow (MVP default):**
```python
# airflow-studio: managed  studio=0.1.0  ir_hash=sha256-…  dag_id=my_etl  syntax=taskflow
from datetime import datetime, timedelta
from airflow.sdk import dag, task

@dag(
    dag_id="my_etl",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=300)},
    tags=["studio"],
)
def my_etl():
    @task.bash(task_id="extract")
    def extract():
        return "echo hi"

    @task(task_id="transform")
    def transform(value):
        return value.upper()

    transform(extract())

my_etl()
```

**Traditional (v1.1):**
```python
# airflow-studio: managed  …  syntax=traditional
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="my_etl", schedule="@daily", start_date=datetime(2026, 1, 1),
    catchup=False, default_args={"retries": 1, "retry_delay": timedelta(seconds=300)},
    tags=["studio"],
) as dag:
    extract = BashOperator(task_id="extract", bash_command="echo hi")
    # … transform as PythonOperator(python_callable=…) …
    extract >> transform
```

## Appendix D — Server endpoint catalog (Airflow `/api/v2`)

| Manager action | Airflow `/api/v2` | Notes |
|---|---|---|
| List DAGs | `GET /dags` | params: `limit, offset, order_by, tags, owners, dag_id_pattern, paused, exclude_stale, has_import_errors, last_dag_run_state, bundle_name` — **form‑exploded**; `fields` removed; use `exclude_stale` not `only_active` |
| DAG detail / params | `GET /dags/{id}/details` | drives the trigger conf form |
| DAG source (read‑only) | `GET /dagSources/{id}?version_number=N` | keyed by `dag_id` in v2; 404 if unparsed |
| Tags | `GET /dagTags` | tag filter UI |
| Pause/unpause | `PATCH /dags/{id}?update_mask=is_paused` | existing, correct |
| Trigger | `POST /dags/{id}/dagRuns` | `{logical_date?, conf?, note?}`; null `logical_date` = now |
| Runs | `GET /dags/{id}/dagRuns?order_by=-logical_date` | |
| Set run state | `PATCH /dags/{id}/dagRuns/{run}` | queued/success/failed |
| Clear run | `POST /dags/{id}/dagRuns/{run}/clear` | |
| Task instances | `GET /dags/{id}/dagRuns/{run}/taskInstances` | + `/{task}`, `/dependencies`, `/tries` |
| **Task logs** | `GET /…/taskInstances/{task}/logs/{try}` | `full_content`, `token`, `map_index`; ndjson tail |
| **Mark state** | `PATCH /…/taskInstances/{task}` (+ `/dry_run`) | success/failed/skipped |
| **Clear/retry** | `POST /dags/{id}/clearTaskInstances` | `dry_run=true` preview first |
| **Import errors** | `GET /api/v2/importErrors` | fields `import_error_id, timestamp, filename, bundle_name, stack_trace`; **the recovery surface** |
| Delete DAG | `DELETE /dags/{id}` | purges DB only → also remove file via `DeployTarget` **first** |
| Assets / events | `GET /assets`, `GET/POST /assets/events` | "datasets" → "assets" in v3 |
| Auth | `POST /auth/token` → `access_token` | not under `/api/v2` |

## Appendix E — Codegen validation pipeline (server, fail‑fast)

| Stage | Action | Executes code? | On failure |
|---|---|---|---|
| 1 | IR schema validation | No | "Invalid graph" |
| 2 | Cycle + required‑field checks | No | Cycle message / field errors → CODE tab + node dots |
| 3 | `dag_id`/`task_id` identifier sanitize (`isidentifier` & not `iskeyword`, de‑dup) | No | "Invalid/duplicate name" |
| 4 | Jinja2 render (`autoescape=False`, `pyrepr`/`pyargs`) | No | Template error (internal) |
| 5 | `ast.parse` | **No (safe)** | SyntaxError + lineno |
| 6 | `compile(..., 'exec')` | **No (safe)** | Name/scoping error + lineno |
| 7 | `DagBag` import in **isolated subprocess** | **Yes (trust boundary)** | import_errors/stacktrace |
| — | `black`/`ruff format` (after 6) | No | — |
| post‑deploy | poll `/dags` + `/importErrors` | Airflow | Friendly "couldn't load" + node/field map |

## Appendix F — Glossary

- **`.afdag`** — the Studio document: a versioned JSON IR of the DAG graph. Source of truth; opened by the editor.
- **IR** — intermediate representation (the `.afdag` content): dag config + nodes + edges + layout, syntax‑agnostic.
- **DeployTarget** — pluggable sink for generated `.py` (shared volume now; Git/S3 later).
- **Provenance** — machine‑readable marker (header comment + `ir_hash` + `afdag_id`) distinguishing Studio‑managed (editable) from hand‑written (read‑only) DAGs and detecting out‑of‑band edits.
- **Registry** — YAML‑per‑operator data driving palette, forms, and codegen.
- **Manager** — the left‑sidebar operations surface; **Studio** — the main‑area visual editor.
```
