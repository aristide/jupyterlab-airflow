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

1. **Studio editor** — a main‑area document (a `.afdag` JSON graph) rendered as a ReactFlow canvas with an operator palette, a tabbed inspector (DAG / NODE / **INFO** / CODE / SAVED), live validation, a generated‑Python preview, and one‑click **Deploy**. The canvas supports full graph editing — add, **delete**, connect, and **reconnect** nodes — with **collapsible side panels** so the canvas can take the whole width, and an **INFO** tab plus inline field help that double the editor as a way to *learn* Airflow.
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
      ├──────────◂┬───────────────────────────────────────────┬▸──────────────────┤
      │ OPERATORS │  ReactFlow canvas (nodes, rounded‑corner   │ Inspector tabs:   │
      │ (palette, │   arrow edges, note cards, minimap, zoom,  │ DAG · NODE · INFO │
      │  searchable, categorized) │   empty‑state)             │ · CODE · SAVED    │
      └──────────◂┴───────────────────────────────────────────┴▸──────────────────┘
      (◂ ▸ = each side panel collapses to a thin rail to give the canvas more room)
```

Both surfaces talk to the **same Jupyter server extension** (namespace `jupyterlab-airflow`), which (a) proxies Airflow `/api/v2` (REST, for the manager + deploy verification) and (b) owns code generation + validation + the filesystem deploy (the labextension cannot reach Airflow's dags volume).

## 5. Scope & phased release plan

The locked decisions are honored; the phasing applies the pre‑mortem's "ruthless MVP" guidance so the no‑code core is proven before the expensive long tail.

### MVP — v0.1 "vertical slice that actually runs"
- **Editor:** ReactFlow canvas with **full graph editing** (add / **delete** / connect / **reconnect** nodes and edges), **rounded‑corner arrow edges**, searchable/categorized palette, **five inspector tabs** (DAG / NODE / **INFO** / CODE / SAVED) with **inline contextual field help**, **collapsible side panels**, top‑bar with live error badge, empty‑state, minimap + zoom, save/reopen via `.afdag`.
- **Operators (core set):** `Empty`, `Bash`, `Python`/Custom `@task` (the code node — decision #3), `Branch` (BranchPython), `TriggerDagRun`. (~5–6 nodes covering the common shapes: linear, fan‑in/out, branch.) The catalogue's growth path — the next **P0** standard ops + first **Sensors**, then gated provider ops, with the user‑requested `KubernetesPodOperator` at **P2** — is the prioritized roadmap in **§6.2.1**.
- **Codegen:** **TaskFlow backend only** (matches the repo's existing example DAG). The Traditional↔TaskFlow *toggle* is built into the IR/UI but defaults to (and only emits) TaskFlow in MVP. *Rationale: shipping both backends doubles the codegen + test surface; see §6.3.*
- **Validation:** client‑side cycle detection + required‑field checks → live error badge & node dots; **server‑side authoritative re‑validation + parse‑check** before deploy.
- **Deploy:** `SharedVolumeTarget` (atomic write) + **lifecycle polling** (appears? import error?) with tri‑state UI.
- **Manager:** extend sidebar with **import errors**, **task instances + states**, **task logs**, **clear/retry**, **delete** (file + history), plus the existing list/pause/trigger/runs.
- **Foundations:** `DeployTarget` interface, operator‑registry mechanism, provenance + collision/namespacing model, secrets guidance, accessibility baseline.

### v1.1 — "dual syntax & breadth"
- Traditional operator codegen backend + the working **Traditional↔TaskFlow toggle** (with golden‑file equivalence tests).
- **Operator breadth + provider gating (§6.2.1):** the **P1** tier — the **provider‑availability gating mechanism** (the prerequisite for any gated op), then `HTTP` (`HttpOperator`) and `SQL` (`SQLExecuteQueryOperator`/`SqlSensor`); plus any remaining **P0** standard ops/sensors (`ShortCircuit`, `LatestOnly`, `File`/`ExternalTask`/`DateTime`/`TimeDelta` sensors) not shipped in the MVP.
- **Annotation / note cards** (§6.1.7) — resizable on‑canvas notes (Markdown) stored in IR `notes[]`, excluded from codegen/validation, for team documentation.
- One‑click **Tidy layout** (dagre), richer undo/redo, optional minimap toggle.

### v1.2 — "beyond a single shared volume"
- **Git** and **S3 / object‑storage** `DeployTarget` implementations (Airflow DAG‑bundle aware).
- **Per‑user identity** on JupyterHub (Hub‑injected Airflow creds / OIDC) + Studio action audit trail.
- Asset/dataset‑driven scheduling; provider sensor catalog (GCS/BigQuery/EMR/Glue/Dataproc) gated on installed providers. **Cloud/Kubernetes operators (§6.2.1 P2):** the user‑requested `KubernetesPodOperator` (`cncf‑kubernetes`) + cloud sensors (`S3KeySensor`, `GCSObjectExistenceSensor`, `BigQueryInsertJobOperator`).

### Explicitly deferred / out
Arbitrary `.py` import to canvas (NG1); RTC (NG2); in‑extension RBAC engine (NG4).

---

## 6. Functional requirements

### 6.1 Visual DAG Editor

**6.1.1 Canvas (ReactFlow, `@xyflow/react` v12).**
- Controlled graph via `useNodesState` / `useEdgesState`; one **node = one Airflow task**, one **edge = one dependency** (`a >> b`).
- Custom node card: category label, operator name, `task_id`, a **validity indicator that is icon + text, not color‑only** (a11y), source/target `Handle`s. Branch/ShortCircuit nodes expose multiple labeled source handles for follow‑paths.
- **Connect:** `onConnect` creates a dependency edge. A single **`isValidConnection`** guard — shared by connect *and* reconnect — rejects self‑loops **and duplicate `(source, target)` pairs**. The duplicate check is **required, not optional**: the IR↔flow mapping derives a deterministic edge id `e_{source}__{target}`, so two edges between the same pair would collide on reload — `isValidConnection` is what prevents it.
- **Delete a node:** removable via the `Delete`/`Backspace` key (ReactFlow `deleteKeyCode`), a hover **✕** button on the node card (an in‑card button revealed on `:hover`/`:focus-within`/selection — simpler and more reliable than `NodeToolbar`), and a **Delete task** action in the NODE tab. The ✕ carries `nodrag nopan` + `stopPropagation` so it never starts a drag or re‑selects. Deleting a node **cascades to its incident edges** (the deps reproject `nodes`/`edges` so no dangling dependency persists; ReactFlow's own keyboard delete also removes connected edges).
- **Delete an edge (connector):** a dependency edge is **independently deletable** without touching the nodes it joins. Affordances: (1) **select the edge** (click — it highlights with `--jp-brand-color1`) and press `Delete`/`Backspace`; (2) a **✕ button on the edge** — the custom edge renders a delete control at its midpoint via `EdgeLabelRenderer`, **shown when the edge is selected** (the button lives in a portal, so reveal is keyed off the reliable `selected` prop rather than cross‑portal CSS hover). Either removes the edge from the live graph; `flowToIR` reprojects `edges[]` so the dependency is gone from the IR (and the regenerated `.py`) on the next commit — the two nodes remain.
- **Disconnect / reconnect an edge:** an existing edge can be grabbed by either endpoint and dropped onto a different node to **rewire the dependency without deleting and redrawing it** — `onReconnect` + the `reconnectEdge` helper (`@xyflow/react` ≥ 12), edges flagged `reconnectable`. An invalid or empty drop is rejected by the shared `isValidConnection` guard and the edge **snaps back unchanged** — deletion stays explicit (✕ / `Delete`) so a missed drop never silently destroys a dependency. (Cycle check remains authoritative server‑side.)
- **Edge rendering:** **rounded‑corner orthogonal arrows** — a small custom edge (`AfdagEdge`) draws a `getSmoothStepPath` (`borderRadius: 8`) with `markerEnd: MarkerType.ArrowClosed`, applied to every edge via `defaultEdgeOptions` + the IR→flow mapping; the connection‑drag preview uses `ConnectionLineType.SmoothStep` to match. Stroke is themed from `--jp-*` (selected/hover → brand color) so it tracks light/dark. (Rationale: orthogonal routing with rounded corners reads as a clearer dependency than the default bézier and matches the reference UI.)
- `Background`, `MiniMap` (bottom‑right), `Controls` (bottom‑left) — matching the reference UI. Empty‑state overlay "Drop operators here" when `nodes.length === 0`.
- Drag‑from‑palette: HTML5 DnD writes the operator id to `dataTransfer`; canvas `onDrop` uses `screenToFlowPosition` and creates a node with an auto‑generated `task_id` (e.g. `bash_6`).
- **Performance:** `nodeTypes`/`edgeTypes` defined at module scope; handlers `useCallback`; node component `React.memo`; narrow store selectors. (DAGs are typically tens of nodes; memoization matters more than viewport culling.)
- **Keyboard deletion (required):** nodes **and** edges are deletable from the keyboard, not just the mouse. ReactFlow keeps nodes/edges focusable (`nodesFocusable`/`edgesFocusable`, default true) — `Tab`/arrow to a node or click‑less‑focus an edge, `Enter`/`Space` to select, then **`Delete` or `Backspace`** removes it. Set `deleteKeyCode={['Delete', 'Backspace']}` so both keys work (some keyboards lack a dedicated `Delete`). Critically, the delete key **must not fire while the user is typing** in an inspector form, the palette search, or the code editor — ReactFlow ignores key events sourced from `input`/`textarea`/`contentEditable` by default; keep that behavior (don't bind a global document listener that bypasses it). Multi‑select (`Shift`/marquee) + `Delete` removes several elements at once. Node deletion still cascades to incident edges; edge deletion leaves the nodes.
- **Accessibility (required, not optional):** a keyboard path to add a node (palette → Enter), connect nodes (select source → "connect to…" → target), edit it (open inspector), and **delete it (focus → `Delete`/`Backspace`)** — for both nodes and edges. Drag‑drop is an *enhancement*, never the only way. Full ARIA labeling (each node/edge has an `aria-label`); focus management across inspector tabs.

**6.1.2 Operator palette (left).** Searchable, grouped by category (Python/Bash, Flow Control, HTTP, Sensors…). **Generated from the operator registry** (a `GET operators` server endpoint at activation, cached). Each item shows label + category and is draggable / keyboard‑activatable.

**6.1.3 Inspector tabs (right).**
- **DAG** — `dag_id`, description, **schedule** (dropdown of presets `@once/@hourly/@daily/@weekly/@monthly/None` + custom cron + `timedelta`), `start_date` (date picker), `catchup` (**default false** — Airflow 3 default), `retries`, `retry_delay`, `tags`, `owner`, `params`, `default_args`.
- **NODE** — operator‑specific form **generated from the registry** (see §6.2), with required‑field validation feeding the error badge; common fields (`retries`, `retry_delay`, `depends_on_past`); JSON/dict fields (env vars, params) via a JSON editor widget; code fields via an embedded CodeMirror editor. **Each field carries inline contextual help** — a one‑line description rendered under the label (RJSF `description` / `ui:help`, already styled as `.field-description`) sourced from the registry param's `help`, so a non‑technical user understands *what a field is for and what a valid value looks like* without leaving the form. Longer or example‑bearing help can surface as an `(i)` tooltip.
- **INFO** *(learn‑Airflow surface)* — a **read‑only educational tab** about the **currently selected node/operator**: a plain‑language description of what the operator does, when to use it, its required vs optional inputs (rendered from the registry param metadata), a worked example, the provider/`airflow_min_version` it needs, and a **"docs ↗" deep link** to the official Airflow/provider page. With no node selected it shows DAG‑level concepts (schedule/`start_date`/`catchup`/retries explained). Content is **data‑only**, sourced from new registry fields (`description`, `docs_url`, per‑param `help`; see §6.2) so adding an operator also teaches it — no code change (G6). This tab is the concrete expression of a secondary product goal: Studio should help users *learn* Airflow components, not just wire them.
- **CODE** — live generated‑Python preview (read‑only), a **Generate DAG** button, and a validation panel that shows **both** client‑side messages (e.g. *"DAG contains a cycle — Airflow does not support cyclic dependencies"*) **and**, after deploy, the **authoritative Airflow import status**.
- **SAVED** — lists `.afdag` documents in the workspace (via Contents API) to reopen; marks which are deployed.
- **Tab order** is DAG · NODE · INFO · CODE · SAVED; selecting a node focuses NODE, and INFO sits beside it so "configure" and "understand" are one click apart.

**6.1.4 Top bar.** Logo · live `dag_id` · node count · **live error badge** (`✕ N errors`, with text not just color) · Traditional↔TaskFlow toggle (v1.1; disabled/ taskflow‑locked in MVP) · Undo · **Reset** (revert to last saved IR) · **Save** (writes the `.afdag` via the document context) · **Generate DAG** (server codegen preview) · **Deploy**.

**6.1.5 Save / reopen.** The editor is a JupyterLab **document** bound to the `.afdag` file; Save/dirty/restore come from the Contents API. Reopening loads the IR (never the generated `.py`). See §8.2–8.3.

**6.1.6 Collapsible side panels.** Both the **left** operator palette and the **right** inspector can be **collapsed to a thin rail and re‑expanded** so the canvas can use the full window width when the user is arranging a large graph (and re‑expanded when they need the palette or a form). Each panel has a **chevron toggle in its header** (`«`/`»`, with an `aria-label` + `aria-expanded`); collapsed, it shows a ~30px rail with an **expand chevron** and a rotated panel label — the expand control is keyboard‑reachable, so the palette's add‑node path is always one click away (drag‑drop is never the only way in). The body is a flexbox (`palette · canvas · inspector`); collapsing sets the side panel's `flex-basis` to the rail width and the `flex:1` canvas reclaims the space (animated with a ≤150 ms `flex-basis` transition). **ReactFlow must remeasure** after the width change — but the change is *internal* (the Lumino widget itself doesn't resize, so the panel's `resized` signal never fires); nudge `rfRef.fitView()` on a short `setTimeout` keyed to the collapse state once the transition has settled, so the graph never renders against a stale viewport. Collapse state is **ephemeral UI state** in MVP (plain `useState`, not persisted in the `.afdag` — writing it into the IR would dirty the document on every toggle); persisting it later belongs in an IR `ui`/`layout` block or JupyterLab `IStateDB`, not the task graph.

**6.1.7 Annotation / note nodes (post‑MVP — see §5).** A **note card** is a draggable, **resizable** sticky‑note on the canvas holding free‑form text (Markdown later) so a workflow designer can leave explanations for teammates ("this branch only runs on month‑end", "owner: data‑eng"). It is **annotation only**: it has **no source/target handles**, takes part in **no dependency edge**, and is **excluded from codegen, cycle detection, and required‑field validation** — it never becomes an Airflow task. Modeling (decided in §8.3): notes live in a **separate `notes[]` array in the IR**, *not* in `nodes[]`, so the task graph that codegen/validation iterate (`ir["nodes"]`/`ir["edges"]`) is untouched and zero codegen changes are needed. On the canvas, task nodes and note cards are merged into one ReactFlow `nodes` array with distinct `type`s (`afdagNode` / `noteNode`, the latter a `NodeResizer` text card) and split back apart on persist. Notes round‑trip through save/reopen like any IR content.

### 6.2 Operator registry

A directory of **YAML files, one per operator**, read by **both** the client (palette + form schema) and the server (Jinja2 codegen). Adding an operator is pure data — no React/Python edits (G6). Each entry declares: `id`, `label`, `category`, `provider` + `airflow_min_version`, the **import line(s)**, required/optional **params** (name, type, widget, default, validation, required, **`help`**), `common_params`, handle topology, `task_id_prefix`, **documentation fields** (`description`, `docs_url`, optional `example`) that feed the **INFO** tab, and **two code templates** (`template_traditional`, `template_taskflow`). See **Appendix A**.

Requirements:
- The registry is the single source of truth for: palette grouping/search, NODE‑tab JSON Schema (rendered with RJSF), the **INFO‑tab learning content**, and server codegen import paths + templates.
- A param `widget: code` (Python) or `widget: json` (dict) selects the embedded editors.
- A param's **`help`** string is the **inline contextual help** (§6.1.3 NODE) — it must be forwarded to the client; the operator‑level **`description`/`docs_url`/`example`** feed the INFO tab, which also surfaces **`provider`/`airflow_min_version`** (now shipped to the client as `provider`/`airflowMinVersion` for the "what this needs" line — previously withheld as codegen‑only). The server's `client_view()` projection (`_CLIENT_PARAM_FIELDS` + a `_CLIENT_DOC_FIELDS` map) must include these keys, and the TS `IOperatorDef`/`IOperatorParam` types must add them. These fields are **documentation, never executed** (rendered as React‑escaped plain text, §9), and are independent of codegen templates (imports + `template_*` stay server‑only).
- **Help/INFO text is untrusted content** — the registry can be extended from a user/server `AIRFLOW_OPERATORS_DIR`, so `description`/`help`/`example`/`docs_url` must be rendered as **plain text (or sanitized Markdown), never raw HTML** (no `dangerouslySetInnerHTML` of registry strings), and `docs_url` links use `rel="noopener"`. See §9.
- Each entry records its **provider package** so the system can warn when an operator's provider isn't installed in the *target Airflow* (not just the Jupyter env).
- Operators with no TaskFlow equivalent (`Empty`, `TriggerDagRun`) declare `taskflow: operator` so the toggle renders them as operators even in TaskFlow mode.

**6.2.1 Operator catalogue roadmap (prioritized) & provider‑availability gating.** The palette UI (search · categories · drag · keyboard‑add) and the registry mechanism are **built**; the catalogue is intentionally small — 5 ops: `Empty`, `Bash`, `Python`/`@task`, `Branch`, `TriggerDagRun` (all standard provider). Growth is **data‑only** (one YAML per operator, §6.2 / Appendix A) and is sequenced by *impact × gating cost*. Class names / import paths / provider packages below are verified against Airflow 3.x provider docs (use the **non‑deprecated** Airflow‑3 paths). The reference UI's palette (HTTP + a full Sensors group) is the breadth target; **do not** re‑build the palette — only add YAML.

| Pri | Operator (class) | Provider pkg | Category | Impact / why |
|---|---|---|---|---|
| **P0** | `ShortCircuit` (ShortCircuitOperator) | standard *(bundled)* | Flow Control | Conditional gate that skips **all** downstream — the #2 flow primitive after the existing `Branch`; reuses the Python code‑node form. `@task.short_circuit` exists. |
| **P0** | `LatestOnly` (LatestOnlyOperator) | standard *(bundled)* | Flow Control | Skip downstream on backfill/catchup so only the latest interval runs; **zero** required params; cheapest add. Render as operator in both modes. |
| **P0** | `FileSensor` | standard *(bundled)* | Sensors | "Wait for input data to land" — most intuitive sensor; **establishes the Sensors category** + the sensor `common_params` (`mode` poke/reschedule · `poke_interval` · `timeout`). |
| **P0** | `ExternalTaskSensor` | standard *(bundled)* | Sensors | Cross‑DAG wait; **read‑side complement** to the existing `TriggerDagRun` for no‑code multi‑DAG pipelines. Highest‑effort P0 (`execution_delta` vs `execution_date_fn` — mutually exclusive; needs careful help copy). |
| **P0** | `DateTimeSensor` · `TimeDeltaSensor` | standard *(bundled)* | Sensors | Wait until a wall‑clock target / a relative delta; low‑effort, teachable. `TimeDelta` reuses the `timedelta` widget already needed for `retry_delay`. |
| **P1** | `HTTP` (HttpOperator) | apache‑airflow‑providers‑http | HTTP | Call any REST/webhook/SaaS endpoint — universally useful; the **first gated op**. Use `HttpOperator`, **not** the deprecated `SimpleHttpOperator`; steer users to an Airflow HTTP **Connection**, not raw URLs/secrets. |
| **P1** | `SQL query` (SQLExecuteQueryOperator) | apache‑airflow‑providers‑common‑sql | SQL | DB‑agnostic SQL — the Airflow‑3 path that **supersedes** per‑DB operators (`PostgresOperator`…); one op + a Connection covers Postgres/MySQL/Snowflake/… |
| **P1** | `SqlSensor` | apache‑airflow‑providers‑common‑sql | Sensors | Poll a DB until a query returns truthy (row‑count / flag / partition‑loaded) — data‑readiness gate under the same provider. |
| **P2** | `KubernetesPodOperator` | apache‑airflow‑providers‑cncf‑kubernetes | Kubernetes | Run any container image as a pod — the universal non‑Python/heavy‑job escape hatch (**the user's explicit ask**). HIGH impact / LOW breadth; flagship gated op — see below. |
| **P2** | `S3KeySensor` | apache‑airflow‑providers‑amazon | Sensors/AWS | Wait for an S3 object — cloud analogue of `FileSensor`. |
| **P2** | `GCSObjectExistenceSensor` | apache‑airflow‑providers‑google | Sensors/GCP | Wait for a GCS object — GCP analogue of `FileSensor`. |
| **P2** | `BigQueryInsertJobOperator` | apache‑airflow‑providers‑google | Cloud/GCP | Run a BigQuery job (current non‑deprecated path; supersedes `BigQueryExecuteQueryOperator`). Nested‑JSON `configuration` → high‑effort form. |

- **KubernetesPodOperator specifics.** Import `airflow.providers.cncf.kubernetes.operators.pod` (the legacy `operators.kubernetes_pod` module is **gone** in current providers). Ship a **starter** param set first — `image`* · `name` · `namespace` · `cmds` · `arguments` (lists → `json` widget) · `env_vars` (dict) · `on_finish_action` (enum: `delete_pod`/`delete_succeeded_pod`/`keep_pod`/`delete_active_pod`, replaces the old `is_delete_operator_pod` bool) — and **defer** the advanced surface (`volumes`, `secrets`, `affinity`, `container_resources`, `pod_template_file`) behind an "advanced" disclosure, or the node form is overwhelming for the no‑code audience. **Gating:** needs the `cncf‑kubernetes` provider in the *target* Airflow **and** cluster/executor access (`in_cluster` or `kubernetes_conn_id`/`config_file` + a K8s‑capable executor) — Studio can verify the **provider** but **not** the cluster, so surface the cluster prerequisites in the **INFO** tab. Caveat the INFO tab heavily: this runs an arbitrary image with worker/cluster privileges — the **same ACE blast‑radius** as code nodes (§9).
- **Provider‑availability gating (new requirement — P1 prerequisite for every Tier‑2/3 op).** Gate on what's installed in the **target Airflow**, never the Jupyter/server env (the server parse‑check is best‑effort / false‑green, R2). Mechanism: (1) `provider` (already on every YAML) is the gating key; treat `apache‑airflow‑providers‑standard` / `(bundled)` as **always‑available** (standard is a core Airflow‑3 dep, present even in the slim image) so all **P0** ops are never gated. (2) Add a server capability that reads the target's installed providers (`GET /api/v2/providers` via the existing `AirflowClient`) and caches the package‑name+version set with a **short TTL / manual refresh** (installing a provider changes availability without a Studio restart). (3) `client_view()` annotates each palette entry `available | missing‑provider | version‑too‑old` (from target‑providers × `provider` × `airflow_min_version`). (4) UI: keep unavailable ops **visible but dimmed** with an `(i)` "Requires `apache‑airflow‑providers‑X` in your Airflow" tooltip + a copy‑paste `pip install` hint — **don't hide them** (they're teachable via INFO and the target may change), non‑color‑only, help‑never‑blocks. (5) **Hard‑gate at deploy:** the validate/deploy step re‑checks the IR's providers against the live target set and **fails fast** with a plain‑language "provider not installed in target Airflow" *before* writing the file, instead of an opaque `/importErrors` later. (6) `/api/v2/importErrors` stays the **authoritative** post‑deploy verdict (the worker env can still differ — provider present on the API node but a connection/cluster missing), so gating is a fast pre‑filter, not a correctness guarantee.

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
- **Reclaimable canvas.** The left palette and right inspector each **collapse to a rail and re‑expand** (§6.1.6) via a header chevron; the canvas grows to fill the freed width and ReactFlow re‑fits. A collapsed panel still exposes its **expand** control (keyboard‑reachable, so the user is never trapped and the palette's add‑node path stays one click away). Transitions are quick (≤150 ms) and the toggle has an ARIA label + state.
- **First‑run onboarding.** Beyond "Drop operators here," provide a guided first‑DAG (seed a template DAG config; a 3‑step coachmark: add node → configure → deploy).
- **Learning & contextual help (the "teach Airflow" goal).** Studio is also a way to *learn* Airflow: every NODE field shows a plain‑language one‑liner (what it is, a valid example), and the **INFO** tab explains the selected operator (purpose, when to use it, required inputs, provider/version, docs deep link) and, with nothing selected, core DAG concepts (schedule/`start_date`/`catchup`/retries). Help text avoids jargon, never blocks the form, and is non‑color‑only (an `(i)` glyph + text). All such copy goes through `trans.__()` (raw Airflow errors and generated code are **not** localized).
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

Versioned IR JSON: `{ schema_version, provenance, syntax_style, dag, nodes[], edges[], notes?[], layout? }`. `node.id` is the stable ReactFlow id; `task_id` is the Airflow id (validated identifier, unique). `op` references a registry id (keeps IR decoupled from operator impl). `position` lives in the IR so layout round‑trips. `provenance` (`afdag_id`, `studio_version`, `ir-hash`) is **also embedded in the generated `.py`** so the manager can tell Studio‑created (editable) from hand‑written (read‑only) DAGs and detect drift. See **Appendix B**.

**Annotation notes (§6.1.7)** live in an **optional, separate `notes[]` array** — `{ id, text, position, size? }` — deliberately **outside `nodes[]`/`edges[]`** so the executable task graph that codegen and validation read (`ir["nodes"]`/`ir["edges"]`) is unaffected and note cards can never become tasks, edges, or cycle/required‑field errors. The IR/flow mapping merges `notes[]` into ReactFlow `nodes` as `type:'noteNode'` and splits them back out on persist. `notes[]` is absent on older `.afdag` files (back‑compatible: default to `[]`).

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
| **M0.5 — Editing & learning UX** | **Delete an edge** (select+Delete, hover ✕, or drag‑off) leaving both nodes; **reconnect** an edge to a new node (drag endpoint); deleting a node **cascades to its edges** in the IR; edges render as **rounded‑corner arrows**; **palette + inspector collapse/expand** and the canvas re‑fits; **INFO** tab explains the selected operator and **per‑field inline help** renders from registry `description`/`help` |
| **M1 — Registry + forms** | Palette + NODE forms generated from registry YAML for the core operator set; adding a YAML operator (incl. `description`/`docs_url`/param `help`) needs no code change (test) |
| **M2 — Validation** | Client cycle/required → error badge + node dots; server re‑validates untrusted IR |
| **M3 — Codegen (TaskFlow)** | IR → idiomatic Airflow‑3 TaskFlow `.py`; golden‑file tests green; safe literal emission verified by escaping tests |
| **M4 — Deploy + lifecycle** | Atomic namespaced write; tri‑state polling; integration test deploys to `apache/airflow:3.0.2`, asserts **zero import errors + a green run** |
| **M5 — Manager ops** | Import‑errors view, task instances, logs, clear/retry, delete (file+history); list param drift fixed |
| **M6 — Recovery UX + a11y** | Friendly import‑error → node/field mapping + "Open in Studio to fix" + undeploy; keyboard path + non‑color‑only indicators |
| **v1.1** | Traditional backend + working toggle (equivalence tests); Tidy layout; more operators |
| **v1.2** | Git + S3 `DeployTarget`; per‑user identity + audit; asset scheduling |

## 15. Wireframes (screen drafts)

Low‑fidelity ASCII drafts of every Studio + Manager surface, reconstructed from a **frame‑by‑frame analysis of the reference product's demo GIFs** (extracted under `design-reference/airflow-studio/` — `gifs/`, `frames/<clip>/all/`, plus a feature‑analysis report) and **reconciled with the current implementation**. They are layout/skeleton drafts for a Data4Now‑branded redesign — *not* pixel specs; reconcile styling with the `data4now-design` skill + `--jp-*` theming.

> **Keep these in sync** (CLAUDE.md): any UI change updates the matching wireframe in the same commit; a new screen/tab/dialog gets a new wireframe. Reference frames cited as `clip f####`.

**Legend** — status of each screen vs. the codebase:
✅ built · 📝 specced in this PRD, not yet built · 🔭 planned (recommended by this analysis)
Controls: `«`/`»` collapse a side panel · `▾` group · `●` node validity dot · `*` required field.

### 15.1 Studio editor — shell + DAG tab ✅

The 3‑pane document: full‑width top bar, then collapsible **palette « · canvas · » inspector**. *(src: 04-main-demo f0000, 03-demo-b f0120)*

```
┌───────────────────────────────────────────────────────────────────────────────┐
│ ✦ Airflow Studio   my_dag.afdag · 4 nodes · ✓ no errors                         │
│            [ Traditional │▣TaskFlow ]   ↶ ↷   Reset   Save   ⚙ Generate DAG   ▶ Deploy │
├──── OPERATORS ───«─┬─────────────── CANVAS ───────────────┬─»── INSPECTOR ───────┤
│ 🔍 Search…         │                                      │ [DAG] NODE INFO CODE SAVED │
│ ▾ PYTHON / BASH    │        ┌────────────────────┐        │ ─────────────────────│
│   Bash operator    │        │ PYTHON_BASH        │        │ DAG CONFIGURATION    │
│   Branch operator  │        │ ▷ Bash operator  ✕ │ ● green│ DAG ID    [ my_dag ] │
│   Python operator  │        │ task_id: print1    │        │ DESCRIPTION [      ] │
│   Custom @task     │        └─────────┬──────────┘        │ SCHEDULE  [ @daily ▾]│
│ ▾ FLOW CONTROL     │                  ▼  (rounded         │ START DATE[01/01/2024]│
│   Empty operator   │        ┌────────────────────┐  arrow)│ OWNER     [ data-team]│
│   Short-circuit op │        │ ▷ Bash operator    │ ● green│ RETRIES[1] RTY-DLY[5]│
│   Trigger DAG run  │        │ task_id: print2    │        │ TAGS  [ etl, prod  ] │
│ ▾ HTTP   (P1 🔭)   │        └─────────┬──────────┘        │ PARAMS  { }          │
│   HTTP             │                  ▼                   │ CATCHUP  ◯ off       │
│ ▾ SENSORS (P0+ 🔭) │        ┌────────────────────┐        │                      │
│   File / External… │        │  … print3 / print4 │ ● green│                      │
│ ＋ Add note        │        └────────────────────┘ ┌────┐ │                      │
│                    │     ⊕ ⊖ ⤢ (zoom/fit)         │mmap│ │                      │
└────────────────────┴──────────────────────────────┴────┴─┴──────────────────────┘
```
Built: palette (search/categories/drag) · rounded‑corner arrow edges · minimap/zoom · DAG form (id/description/schedule/start_date/owner/retries/retry_delay/tags/params/catchup) · live `✓ no errors` badge · Reset/Save/Generate/Deploy. **Locked to TaskFlow** until the Traditional backend (v1.1). Palette **catalogue** grows per §6.2.1 (HTTP/Sensors 🔭).

### 15.2 Studio editor — empty‑state / onboarding ✅

0 nodes → drop‑zone. *(src: 01-small-demo f0000; the clip also demos the syntax toggle.)*

```
│ … palette …  │            ╭───────────────────────╮            │ DAG CONFIG … │
│              │            │        ⬚  (icon)        │            │ DAG ID [my_dag]│
│              │            │   Drop operators here   │            │  …           │
│              │            │ Drag from the left      │            │              │
│              │            │ panel to get started    │            │              │
│              │            ╰───────────────────────╯            │              │
                 top bar shows “0 nodes”; [Traditional│▣TaskFlow] toggle is the clip’s subject
```
Beyond the drop hint, §7 adds a 3‑step coachmark (add → configure → deploy) 🔭.

### 15.3 Studio editor — NODE tab + live validation ✅

Select a node → operator form; required‑field gaps drive the badge + the node's red `●`. *(src: 02-demo-a f0150/f0600)*

```
top bar:  … my_dag · 2 nodes · ✕ 2 errors      ← red while required fields empty

  canvas node (invalid)              INSPECTOR — NODE tab
  ┌────────────────────┐            ┌ DAG [NODE] INFO CODE SAVED ─────────────┐
  │ PYTHON_BASH       ✕│            │ ⚠ 2 errors on this node                 │
  │ ▷ Bash operator    │ ● red      │ BASH OPERATOR        node_173…_6        │
  │ task_id: bash_7    │            │ TASK ID *         [ bash_7            ]  │
  └────────────────────┘            │ BASH COMMAND *    [                  ]⛔│ ← red outline
                                    │ ENVIRONMENT VARS  [ { }              ]  │
                                    │ RETRIES           [ 1 ]                 │
                                    │ RETRY DELAY (MIN) [ 5 ]                 │
                                    │ DEPENDS ON PAST   ◯ off                 │
                                    │ ─────────────────────────────────────  │
                                    │                       [ 🗑 Delete task ] │
                                    └─────────────────────────────────────────┘
```
Built: registry‑generated form, `validateNodeParams` required‑field check (red outline), top‑bar `✕ N errors` decrementing live, per‑node dots, in‑card ✕ + “Delete task”. New operators just add param YAML — no form code.

### 15.4 Studio editor — CODE tab (+ cycle‑error variant) ✅

Live generated‑Python preview + Copy; the cycle path replaces the code until the graph is acyclic. *(src: 03-demo-b f0500)*

```
 INSPECTOR — CODE tab (valid)                 cycle‑detection variant
 ┌ DAG NODE INFO [CODE] SAVED ───────────┐    ┌ … [CODE] … ──────────────────────┐
 │ GENERATED CODE            [ ⧉ Copy ]  │    │ ✕ Validation                     │
 │ # airflow-studio: managed … taskflow  │    │ DAG contains a cycle — Airflow   │
 │ from airflow.sdk import dag, task     │    │ does not support cyclic deps.    │
 │ @dag(schedule="@daily", …)            │    │ Remove an edge on the path:      │
 │ def my_dag():                         │    │     print3 → print1              │
 │   @task.bash(task_id="print1")        │    │ (code preview hidden until the   │
 │   def print1(): return "echo Hello"   │    │  graph is acyclic)               │
 │   …                                   │    │                                  │
 │   # --- Dependencies ---              │    │ [ ⚙ Generate DAG ]               │
 │   print1 >> print2 >> print3 >> print4│    └──────────────────────────────────┘
 │ [ ⚙ Generate DAG ]        ✓ Valid     │
 └───────────────────────────────────────┘
```
Built: server codegen (TaskFlow), Copy, validation panel showing client errors **and** post‑deploy Airflow import status. **Traditional output is v1.1** (templates exist; backend + toggle unlock pending).

### 15.5 Studio editor — INFO tab (learn‑Airflow) ✅

Read‑only teaching surface for the selected operator (DAG concepts when nothing is selected). *(Studio enhancement — the reference UI has no INFO tab.)*

```
 ┌ DAG NODE [INFO] CODE SAVED ───────────────────────────────┐
 │ ℹ Bash operator                          provider: standard│
 │ Runs a shell command on an Airflow worker. Use it for      │
 │ scripts, CLI tools, or quick file/data operations.         │
 │ NEEDS  apache-airflow-providers-standard · Airflow ≥ 3.0   │
 │ REQUIRED · Bash Command — the shell command, e.g.          │
 │            `python etl.py --date {{ ds }}`                  │
 │ OPTIONAL · Environment Vars · Working dir                  │
 │ EXAMPLE   echo "hello $NAME"                                │
 │ 📖 Docs ↗   (rel=noopener)                                  │
 └────────────────────────────────────────────────────────────┘
```
Built: registry‑driven (`description`/`docs_url`/`example`/per‑param `help`/`provider`/`airflow_min_version`), all rendered as escaped plain text (registry is user‑extensible → no raw HTML). For **gated** ops (§6.2.1) this tab also states the missing provider + any non‑checkable prerequisite (e.g. a K8s cluster) 🔭.

### 15.6 Deploy — top‑bar action + tri‑state banner ✅

Deploy is an *observable* lifecycle (§6.5.4), not a silent success. *(src: 04-main-demo build→deploy→native‑Airflow run)*

```
 top-bar:   ▶ Deploy  ─►  ⏳ Deploying…  ─►  ✓ Deployed     /     ✕ Failed

 DeployBanner (under the top bar):
 ① ┌ ⏳ Writing my_dag.py to the dags folder…                          ┐
 ② ┌ ⏳ Waiting for Airflow to pick it up (up to a few minutes)…       ┐
   │                                       [ Keep waiting ]    [ × ]   │
 ③a┌ ✓ Registered — my_dag is live (paused).  [ Unpause & trigger ][×]┐
 ③b┌ ✕ Couldn’t load — “Bash Command” on node fetch_data is empty.    ┐
   │   [ Show technical details ▾ ]                                    │
   │   [ Open in Studio to fix ]   [ Undeploy ]                  [ × ] │
```
Built: atomic `SharedVolumeTarget` write + post‑deploy polling of `/dags` + `/importErrors`; banner renders writing/waiting/registered/failed/processing with the recovery actions.

### 15.7 Palette — provider‑availability states 🔭

How gated (Tier‑2/3) operators appear once gating lands (§6.2.1). Unavailable ops stay **visible but dimmed** — never hidden.

```
 ▾ HTTP
   HTTP            ⚠ dimmed   ⓘ Requires apache-airflow-providers-http in your Airflow
                                pip install apache-airflow-providers-http
 ▾ SENSORS
   File sensor               (available — standard provider is never gated)
   S3 key sensor   ⚠ dimmed   ⓘ Requires …-amazon  + an AWS Connection
   Kubernetes pod  ⚠ dimmed   ⓘ Requires …-cncf-kubernetes  + a reachable cluster
```
Server reads the **target** Airflow’s `/api/v2/providers`; palette payload annotates `available | missing-provider | version-too-old`; deploy hard‑fails on a missing provider before writing the file.

### 15.8 Manager — DAG list (left sidebar) ✅

The operations surface. *(Mirrors what the demo shows running in the **native** Airflow UI — src: 04-main-demo f0400 — but rendered inside JupyterLab.)*

```
 ┌ Airflow — DAGs ──────────────────────────────── ⟳ ┐
 │ 🔍 Search dag_id…                      [ Tags ▾ ]  │
 │ ⚠ 1 import error  ▾ (expand → plain-language fix)  │
 │ ──────────────────────────────────────────────────│
 │ ◐ my_dag         @daily   etl,prod      ⏸  ▶  🗑   │   ◐ run-status donut
 │ ● ingestion_dag  15m  ⏵running          ⏸  ▶  🗑   │   ⏸ pause/unpause
 │ ⚠ load_dag       (import error)         ⏸  ▶  🗑   │   ▶ trigger · 🗑 delete(purge)
 │ ──────────────────────────────────────────────────│
 └────────────────────────────────────────────────────┘
```
Built: list (search/tag filter, `exclude_stale`), pause, trigger, run‑status, `has_import_errors` badge + import‑errors panel, delete (file‑then‑history). *Trigger is bare* today — see 15.10.

### 15.9 Manager — run / task drill‑down + logs ✅

Expand a DAG → runs → task instances → logs. *(Mirrors native grid/logs — src: 04-main-demo f0600/f0850.)*

```
 ┌ my_dag ─────────────────────────────────┐   Log modal
 │ RUNS                                     │   ┌ print2 · attempt 2 ▾ ───────────┐
 │ ▾ 2026-03-14 17:25  ✓ success            │   │ [INFO] Running BashOperator…     │
 │    • print1  ✓ success  try 1            │   │ [INFO] echo Hello                │
 │    • print2  ✕ failed   try 2  [ logs ]──┼──▶│ [INFO] Command exited 0          │
 │    • print3  ◷ queued                    │   │ …                                │
 │ ▸ 2026-03-13 …      ✓                     │   │ [ Wrap ] [ Download ]      [ × ] │
 │ ──────────────────────────────────────── │   └──────────────────────────────────┘
 │ [ Clear/Retry ▸ dry-run preview ]  [ Mark state… ]                              │
 └──────────────────────────────────────────┘
```
Built: task instances + states, paged logs with attempt selector, clear/retry (dry‑run preview → confirm), mark success/failed/skipped. *(Native grid/Gantt/XCom stay in Airflow’s own UI — NG3; optional deep‑link.)*

### 15.10 Manager — trigger‑with‑conf dialog 📝

The **only** missing piece of “triggers”: a conf form derived from the DAG’s `params` (already specced in §6.6; `triggerDag(id, conf)` + the server handler already accept a `conf` dict — UI‑only).

```
 ┌ Trigger my_dag ──────────────────────────────┐
 │ This DAG accepts parameters:                 │   ← fields from GET /dags/{id}/details
 │   start_date  [ 2026-06-15              ]     │
 │   region      [ eu-west-1   ▾           ]     │
 │   dry_run     [ ◯ off ]                       │
 │ ─────────────────────────────────────────── │
 │   logical_date  ◉ now    ○ [ pick…      ]     │   ← null logical_date = run now (AF3)
 │              [ Cancel ]       [ ▶ Trigger ]   │
 └───────────────────────────────────────────────┘
   DAGs with no params skip the dialog → instant bare trigger (today’s behavior).
```

> **Triggers — already covered (do not re‑add):** the **TriggerDagRunOperator** ships as a palette operator (`operators/trigger_dagrun.yaml`) for composing multi‑DAG pipelines, and the Manager's **manual one‑click DAG‑run trigger** works end‑to‑end (`ManagerApp` → `triggerDag` → `POST /dags/trigger`). The single gap is the conf form above (15.10).

---

## Appendix A — Operator registry YAML (example)

```yaml
id: bash
label: Bash operator
category: Python/Bash
provider: apache-airflow-providers-standard      # bundled with Airflow 3 base image
airflow_min_version: '3.0'
description: >                                    # INFO tab: plain-language "what & when"
  Runs a shell command on an Airflow worker. Use it for scripts, CLI tools,
  or quick file/data operations. The command runs in a temporary working
  directory; set env vars below rather than pasting secrets.
docs_url: https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/bash.html
import: 'from airflow.providers.standard.operators.bash import BashOperator'
import_taskflow: 'from airflow.sdk import task'
handles: { in: true, out: true }                 # branch sets out: [true, false]
taskflow: native                                 # native | operator (Empty/TriggerDagRun = operator)
task_id_prefix: bash                             # -> bash_6, bash_7
params:                                          # `help` -> inline field help (INFO + NODE tabs)
  - { name: bash_command, label: 'Bash Command', type: string, required: true, widget: textarea,
      help: 'The shell command to run, e.g. `python etl.py --date {{ ds }}`. Jinja templating is supported.' }
  - { name: env,          label: 'Environment Vars', type: object, required: false, widget: json, default: {},
      help: 'JSON object of name→value passed to the command as environment variables.' }
  - { name: cwd,          label: 'Working dir', type: string, required: false,
      help: 'Directory to run the command in. Leave blank to use a temp dir.' }
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
  "edges": [ { "source": "n1", "target": "n2" } ],
  "notes": [
    { "id": "note1", "text": "Runs nightly; owner = data-eng. Holiday calendar TBD.",
      "position": { "x": 120, "y": 220 }, "size": { "width": 220, "height": 90 } }
  ]
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
