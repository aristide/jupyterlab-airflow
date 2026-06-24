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
- **Codegen:** **both backends shipped ✅** — the Traditional↔TaskFlow toggle selects the IR's `syntax_style` and codegen emits `@dag`/`@task` (TaskFlow, the default) or `with DAG(…)` + operator instances + `>>` (Traditional), with a task‑graph equivalence test (§6.3). *(Originally MVP-TaskFlow-only; the Traditional backend landed 2026‑06‑20 once the per-op `template_traditional` set was complete.)*
- **Validation:** client‑side cycle detection + required‑field checks → live error badge & node dots; **server‑side authoritative re‑validation + parse‑check** before deploy.
- **Deploy:** `SharedVolumeTarget` (atomic write) + **lifecycle polling** (appears? import error?) with tri‑state UI.
- **Manager:** extend sidebar with **import errors**, **task instances + states**, **task logs**, **clear/retry**, **delete** (file + history), plus the existing list/pause/trigger/runs.
- **Foundations:** `DeployTarget` interface, operator‑registry mechanism, provenance + collision/namespacing model, secrets guidance, accessibility baseline.

### v1.1 — "dual syntax & breadth"
- Traditional operator codegen backend + the working **Traditional↔TaskFlow toggle** (with a task‑graph equivalence test) — **shipped ✅ (2026‑06‑20, §6.3)**.
- **Operator breadth + provider gating (§6.2.1):** the **P1** tier — the **provider‑availability gating mechanism** (the prerequisite for any gated op), then `HTTP` (`HttpOperator`) and `SQL` (`SQLExecuteQueryOperator`/`SqlSensor`); plus any remaining **P0** standard ops/sensors (`ShortCircuit`, `LatestOnly`, `File`/`ExternalTask`/`DateTime`/`TimeDelta` sensors) not shipped in the MVP.
- **Annotation / note cards** (§6.1.7) — resizable on‑canvas notes (Markdown) stored in IR `notes[]`, excluded from codegen/validation, for team documentation.
- One‑click **Tidy layout** (dagre) ✅, richer undo/redo, optional minimap toggle.

### v1.2 — "beyond a single shared volume"
- **Git + S3 `DeployTarget` ✅ (2026‑06‑23)** — commit/push generated DAGs to a git working tree an Airflow `GitDagBundle` tracks (`AIRFLOW_DEPLOY_TARGET=git`), or put them as S3 objects an S3 DAG bundle reads (`AIRFLOW_DEPLOY_TARGET=s3`; AWS S3 or MinIO). Both DAG‑bundle aware, same `DeployTarget` interface + `get_deploy_target()` factory (§6.5.1 / §8.7).
- **Studio action audit trail — shipped ✅ (2026‑06‑23, §9):** every mutating action emits a `{ts, user, action, dag_id, correlation_id, outcome}` record on the `jupyterlab_airflow.audit` logger, stamped with the authenticated Jupyter user. **Per‑user identity** on JupyterHub (Hub‑injected Airflow creds / OIDC at spawn) is a **deployment‑config** path (documented in §9 / `config.py`), not an in‑extension RBAC system (NG4) — the audit attributes actions per server process today.
- **Asset/dataset‑driven scheduling — shipped ✅ (2026‑06‑23, §6.9):** `dag.schedule_assets` (a DAG scheduled on asset updates) with a **match mode** (`all` → `AssetAll`/list · `any` → `AssetAny`) and an optional **combine‑with‑time** flag (→ `AssetOrTimeSchedule(timetable=CronTriggerTimetable(...), assets=…)`, guarded against `@once`), plus per‑task `outlets`/`inlets` (produce/consume assets); IR‑data‑only, asset/timetable imports collected only when used, both families. Provider sensor catalog (EMR/Glue/Dataproc…) gated on installed providers stays 🔭. **Cloud/Kubernetes operators (§6.2.1 P2) — shipped early ✅ (2026‑06‑20):** the gating mechanism made them cheap to land as registry YAML, so the user‑requested `KubernetesPodOperator` (`cncf‑kubernetes`) + cloud sensors (`S3KeySensor`, `GCSObjectExistenceSensor`, `BigQueryInsertJobOperator`) are already in the catalogue. The **advanced KPO surface — shipped ✅ (2026‑06‑23):** dedicated fields for `node_selector`/`labels`/`annotations`/`service_account_name`/`priority_class_name`/`security_context` (plain dict/string), plus `pod_template_file`/`pod_template_dict` as the declarative escape hatch for volumes/secrets/affinity/tolerations/resource limits (a raw JSON pod manifest Airflow deserializes + merges — no typed `k8s.V1*` objects or imports, so it stays data‑only and editor‑friendly).

### v1.3 — "lakehouse breadth & UX polish" (the 2026‑06‑22 feature set)
- **Lakehouse operator expansion (§6.2.2):** Storage (MinIO/S3 object ops), Ingestion (SFTP/FTP/IMAP), Compute (Spark, Papermill), Data Quality (SQL checks + GX), Governance (OpenMetadata), and Notification **operators** (Email/Slack/…) — gated registry YAML. **P0 + P1 + P2 + third‑party P3 shipped ✅** (26 ops, catalogue → 44); GX checkpoint + OpenMetadata lineage ride a distinct un‑gated `third-party` state (§13 Q13).
- **Friendly log viewer ✅ (§6.6):** the raw `<pre>` is replaced by a level‑coloured, searchable, attempt‑aware viewer (Copy/Download/Wrap/autoscroll‑to‑first‑error).
- **Field info bubbles ✅ (§6.1.3):** help on every DAG field + a hoverable `ⓘ` bubble on DAG **and** NODE fields.
- **Notifications & alerting ✅ (DAG‑level + per‑task; §6.8):** IR `dag.callbacks` **and** `node.callbacks` blocks + a notifier registry (5 channels: Smtp/Slack/Apprise/Discord/Opsgenie) + a **Notifications inspector tab** (DAG) and a **NODE‑tab Notifications section** (per‑task, incl. the task‑only `on_retry`) sharing one editor + codegen into `on_*_callback`, so a failed/succeeded/retrying DAG **or task** alerts via email/Slack/Teams/Discord/Opsgenie — the callback half the canvas can't model.

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
- **DAG** — `dag_id`, description, **schedule** (dropdown of presets `@once/@hourly/@daily/@weekly/@monthly/None` + custom cron + `timedelta`), **schedule on assets ✅** (a comma‑separated `schedule_assets` field — data‑aware scheduling, §6.9; when set it overrides the time schedule → `schedule=[Asset(...)]`), `start_date` (date picker), `catchup` (**default false** — Airflow 3 default), `retries`, `retry_delay`, `tags`, `owner`, `params`, `default_args`. **Every DAG field now carries contextual help ✅.** Each field has a plain‑language `description` — `schedule` ("how often the DAG runs: a preset like `@daily`, a cron `0 9 * * *`, or None for manual/triggered‑only"), `start_date`, `catchup`, `retries`/`retry_delay`, `description`, `owner`, `params`, `default_args` (`dag_id`/`tags` already had one) — reusing the INFO‑tab `DAG_CONCEPTS` wording, surfaced through the shared `ⓘ` info bubble below. Filled in `forms.ts` (data‑only, no registry change).
- **NODE** — operator‑specific form **generated from the registry** (see §6.2), with required‑field validation feeding the error badge; a **"Common settings" section** (✅, built) for the op's per‑task common fields — `retries`, `retry_delay`, `depends_on_past`, plus sensor `mode`/`poke_interval`/`timeout` — that **override the DAG defaults** (stored in `node.common`, emitted by codegen with `retry_delay` → `timedelta`; only explicitly‑set values are written); JSON/dict fields (env vars, params) via a JSON editor widget; code fields via an embedded CodeMirror editor. An **"Assets (data‑aware scheduling)" section ✅** (§6.9) edits the task's `outlets` (assets it produces — triggering DAGs scheduled on them) and `inlets` (assets it consumes — lineage), each a comma‑separated `Asset` name/URI list, stored in `node.outlets`/`node.inlets`. **Each field carries inline contextual help** — a one‑line description rendered under the label (RJSF `description` / `ui:help`, already styled as `.field-description`) sourced from the registry param's `help`, so a non‑technical user understands *what a field is for and what a valid value looks like* without leaving the form. **Hoverable `ⓘ` info bubble ✅:** each field's help surfaces from an `ⓘ` glyph (rendered where RJSF places the field description) — a small accessible tooltip (`role="tooltip"`, `aria-describedby`, opens on **hover, keyboard focus, and click/tap**, dismisses on `Escape`/blur) so the form stays uncluttered but the explanation is one hover away. Wired **once** as a custom RJSF `DescriptionFieldTemplate` in the shared `AfdagForm` (the new `InfoBubble` primitive), so it upgrades **both** the DAG and NODE forms at one point. This is the user's ask: *"add field information bubbles to explain each field."*
- **INFO** *(learn‑Airflow surface)* — a **read‑only educational tab** about the **currently selected node/operator**: a plain‑language description of what the operator does, when to use it, its required vs optional inputs (rendered from the registry param metadata), a worked example, the provider/`airflow_min_version` it needs, and a **"docs ↗" deep link** to the official Airflow/provider page. With no node selected it shows DAG‑level concepts (schedule/`start_date`/`catchup`/retries explained). Content is **data‑only**, sourced from new registry fields (`description`, `docs_url`, per‑param `help`; see §6.2) so adding an operator also teaches it — no code change (G6). This tab is the concrete expression of a secondary product goal: Studio should help users *learn* Airflow components, not just wire them.
- **CODE** — live generated‑Python preview (read‑only), a **Generate DAG** button, and a validation panel that shows **both** client‑side messages (e.g. *"DAG contains a cycle — Airflow does not support cyclic dependencies"*) **and**, after deploy, the **authoritative Airflow import status**. The preview is rendered in a **read‑only CodeMirror 6 editor with Python syntax highlighting and a left line‑number gutter** (not a plain `<pre>`), reusing the same `CodeMirrorField` that backs the `code`/`json` node fields (`language="python"`, `readOnly`) so the generated DAG is **colorized, gutter‑numbered, selectable, and scrollable**, and is **theme‑aware via `--jp‑*`** (light/dark). Implementation + the one missing piece (a CodeMirror *highlight style*) are in §8.2.
- **SAVED** — lists `.afdag` documents in the workspace (via Contents API) to reopen; marks which are deployed.
- **Tab order** is DAG · NODE · INFO · NOTIFY · CODE · SAVED; selecting a node focuses NODE, and INFO sits beside it so "configure" and "understand" are one click apart. **NOTIFY** (§6.8) edits DAG‑level notification callbacks; the **NODE** tab carries a matching "Notifications" section for per‑task callbacks (incl. `on_retry`).

**6.1.4 Top bar.** Logo · live `dag_id` · node count · **live error badge** (`✕ N errors`, with text not just color) · **Traditional↔TaskFlow toggle ✅ (§6.3)** — a segmented control that flips the IR's `syntax_style`, persists it, and regenerates the CODE preview / next Deploy · **`≣ Tidy` ✅ (§8.2)** — one‑click auto‑layout (dagre) that re‑positions the task nodes top‑to‑bottom, persists them, and re‑fits the view (disabled when empty; leaves note cards in place) · Undo · **Reset** (revert to last saved IR) · **Save** (writes the `.afdag` via the document context) · **Generate DAG** (server codegen preview) · **Deploy**.

**6.1.5 Save / reopen.** The editor is a JupyterLab **document** bound to the `.afdag` file; Save/dirty/restore come from the Contents API. Reopening loads the IR (never the generated `.py`). See §8.2–8.3. **Renaming** the document vs changing the `dag_id` (and what each does to a deployed/running pipeline) is **§6.1.8**.

**6.1.6 Collapsible side panels.** Both the **left** operator palette and the **right** inspector can be **collapsed to a thin rail and re‑expanded** so the canvas can use the full window width when the user is arranging a large graph (and re‑expanded when they need the palette or a form). Each panel has a **chevron toggle in its header** (`«`/`»`, with an `aria-label` + `aria-expanded`); collapsed, it shows a ~30px rail with an **expand chevron** and a rotated panel label — the expand control is keyboard‑reachable, so the palette's add‑node path is always one click away (drag‑drop is never the only way in). The body is a flexbox (`palette · canvas · inspector`); collapsing sets the side panel's `flex-basis` to the rail width and the `flex:1` canvas reclaims the space (animated with a ≤150 ms `flex-basis` transition). **ReactFlow must remeasure** after the width change — but the change is *internal* (the Lumino widget itself doesn't resize, so the panel's `resized` signal never fires); nudge `rfRef.fitView()` on a short `setTimeout` keyed to the collapse state once the transition has settled, so the graph never renders against a stale viewport. Collapse state is **ephemeral UI state** in MVP (plain `useState`, not persisted in the `.afdag` — writing it into the IR would dirty the document on every toggle); persisting it later belongs in an IR `ui`/`layout` block or JupyterLab `IStateDB`, not the task graph.

**6.1.7 Annotation / note nodes (post‑MVP — see §5).** A **note card** is a draggable, **resizable** sticky‑note on the canvas holding free‑form text (Markdown later) so a workflow designer can leave explanations for teammates ("this branch only runs on month‑end", "owner: data‑eng"). It is **annotation only**: it has **no source/target handles**, takes part in **no dependency edge**, and is **excluded from codegen, cycle detection, and required‑field validation** — it never becomes an Airflow task. Modeling (decided in §8.3): notes live in a **separate `notes[]` array in the IR**, *not* in `nodes[]`, so the task graph that codegen/validation iterate (`ir["nodes"]`/`ir["edges"]`) is untouched and zero codegen changes are needed. On the canvas, task nodes and note cards are merged into one ReactFlow `nodes` array with distinct `type`s (`afdagNode` / `noteNode`, the latter a `NodeResizer` text card) and split back apart on persist. Notes round‑trip through save/reopen like any IR content.

**6.1.8 Rename a Studio DAG — document vs `dag_id` (deploy‑aware).** Renaming splits into **two** operations with very different blast radius; the UI keeps them distinct because one is free and the other is a migration.

- **(A) Rename the *document* (the `.afdag` file)** — *safe, local, no Airflow impact.* `dag_id` is a free IR field **decoupled** from the filename (only `createEmptyIR`/new seeds it from the path via `dagIdFromPath`), and the deploy artifact is keyed on `dag_id`/`afdag_id`, **never** the `.afdag` name — so renaming the file changes nothing Airflow sees. Reuse JupyterLab's **`docmanager:rename`**: the open `DocumentWidget` context and the `WidgetTracker` restore key follow the rename automatically, and the SAVED tab re‑lists by the new path. The stable **`afdag_id`** preserves identity, so an already‑deployed `.py` stays associated. Surface it as a top‑bar / File‑menu **"Rename…"**; when the DAG isn't deployed this is the whole story. *(This is the "rename the notebook" ask — made explicit and impact‑free.)*
- **(B) Change the `dag_id` (the Airflow identity)** — a **guided, deploy‑state‑aware migration, not a rename.** Airflow 3 has **no `dag_id` rename** (`PATCH /dags/{id}` only toggles `is_paused`): a new `dag_id` is a **brand‑new DAG with no run history**, and because the deployed file is `{dag_id}.py` the old file is **orphaned**. Today `dag_id` is freely editable in the DAG form with **zero guard** — this feature **intercepts** that edit and routes it by the state of the *current* `dag_id`:
  - **Draft (never deployed):** trivial — validate the new id (`str.isidentifier()` & not a keyword, §8.4 ③) + collision check (no existing managed/hand‑written `{new_id}.py`, no duplicate `dag_id`, §6.5.3), set `ir.dag.dag_id`, keep `afdag_id`. Nothing to migrate.
  - **Deployed, idle (no active run):** a **"Rename & redeploy"** dialog that states the consequences up front — a new DAG `{new_id}` is created **paused** with **fresh history**, and the old `{old_id}` history **does not carry over**. Order: validate + collision → regenerate → **deploy `{new_id}.py` and verify it registers** (tri‑state §6.5.4) → **then** reconcile the old DAG (**write‑new‑then‑remove‑old**, so there is never a zero‑file gap; the new DAG is paused so the brief two‑DAG window is harmless). Old‑DAG handling is the user's choice, defaulting to the **non‑destructive** option:
    - **Keep history (default):** pause `{old_id}` and **remove `{old_id}.py`** so it isn't re‑parsed; Airflow retains the old run history (the dag becomes fileless/`stale`, still viewable via `exclude_stale=false`).
    - **Purge old:** `purge_dag(old_id)` — remove the file **and** `DELETE /dags/{old_id}` (destroys history; irreversible; explicit opt‑in, with the standard destructive‑action confirm).
  - **Deployed, run ACTIVE (running/queued):** **blocked by default.** Cutting `{old_id}.py` mid‑run **strands the in‑flight run** — `LocalDagBundle` has no versioning and §8.8 forbids editing a deployed file during an active run. Offer **"wait for the current run to finish"** (watch the run; auto‑continue when it leaves running/queued) or an explicit, heavily‑warned **override** that proceeds and **accepts that in‑flight runs on the old id are lost** (defer the old‑file removal until the run is no longer active where possible).
- **Identity & re‑association.** Keep `afdag_id` **constant** across either rename. The provenance header **must also carry `afdag_id`** (it currently emits only `dag_id` + `ir_hash` + `syntax`, §8.9) so the manager can recognize a deployed DAG as "the renamed‑from version of this `.afdag`" — detect the rename, re‑link, and warn on drift — instead of treating old and new as unrelated.
- **Validation, collision, state.** Reuse identifier safety (§8.4 ③) and the pre‑write ownership/duplicate checks (§6.5.3); active‑run detection uses `GET /dags/{id}/dagRuns` filtered to running/queued (the `list_dag_runs` client method). The migration is a **thin server orchestration over existing primitives** — `deploy_dag` (write new) + pause / `delete`‑file / `purge_dag` (reconcile old) — not new deploy machinery.

### 6.2 Operator registry

A directory of **YAML files, one per operator**, read by **both** the client (palette + form schema) and the server (Jinja2 codegen). Adding an operator is pure data — no React/Python edits (G6). Each entry declares: `id`, `label`, `category`, `provider` + `airflow_min_version`, the **import line(s)**, required/optional **params** (name, type, widget, default, validation, required, **`help`**), `common_params`, handle topology, `task_id_prefix`, **documentation fields** (`description`, `docs_url`, optional `example`) that feed the **INFO** tab, and **two code templates** (`template_traditional`, `template_taskflow`). See **Appendix A**.

Requirements:
- The registry is the single source of truth for: palette grouping/search, NODE‑tab JSON Schema (rendered with RJSF), the **INFO‑tab learning content**, and server codegen import paths + templates.
- A param `widget: code` (Python) or `widget: json` (dict) selects the embedded editors.
- A param's **`help`** string is the **inline contextual help** (§6.1.3 NODE) — it must be forwarded to the client; the operator‑level **`description`/`docs_url`/`example`** feed the INFO tab, which also surfaces **`provider`/`airflow_min_version`** (now shipped to the client as `provider`/`airflowMinVersion` for the "what this needs" line — previously withheld as codegen‑only). The server's `client_view()` projection (`_CLIENT_PARAM_FIELDS` + a `_CLIENT_DOC_FIELDS` map) must include these keys, and the TS `IOperatorDef`/`IOperatorParam` types must add them. These fields are **documentation, never executed** (rendered as React‑escaped plain text, §9), and are independent of codegen templates (imports + `template_*` stay server‑only).
- **Help/INFO text is untrusted content** — the registry can be extended from a user/server `AIRFLOW_OPERATORS_DIR`, so `description`/`help`/`example`/`docs_url` must be rendered as **plain text (or sanitized Markdown), never raw HTML** (no `dangerouslySetInnerHTML` of registry strings), and `docs_url` links use `rel="noopener"`. See §9.
- Each entry records its **provider package** so the system can warn when an operator's provider isn't installed in the *target Airflow* (not just the Jupyter env).
- Operators with no TaskFlow equivalent (`Empty`, `TriggerDagRun`) declare `taskflow: operator` so the toggle renders them as operators even in TaskFlow mode.

**6.2.1 Operator catalogue roadmap (prioritized) & provider‑availability gating.** The palette UI (search · categories · drag · keyboard‑add) and the registry mechanism are **built**; the catalogue started at 5 ops (`Empty`, `Bash`, `Python`/`@task`, `Branch`, `TriggerDagRun`, all standard provider). The **P0** tier below is now **shipped (✅, 2026‑06‑19)** — 6 more bundled standard‑provider ops (catalogue → 11): `ShortCircuit` + `LatestOnly` in **Flow Control**, and the new **Sensors** category (`File`, `ExternalTask`, `DateTime`, `TimeDelta`), which also established the sensor `common_params` (`mode`/`poke_interval`/`timeout`, declared on each sensor YAML; **per‑node `common` wiring is now built ✅ — §6.1.3** — so they're editable in the NODE "Common settings" section and emitted, overriding the DAG defaults). The **P1** tier — the first **gated** ops — is now **shipped (✅, 2026‑06‑20)** on top of the gating mechanism: `HTTP` (`HttpOperator`, `providers‑http`), `SQL query` (`SQLExecuteQueryOperator`) + `SqlSensor` (`providers‑common‑sql`) — catalogue → 14. Each steers users to an Airflow **Connection** (not raw URLs/secrets); they dim in the palette + hard‑fail deploy when their provider isn't in the target Airflow (§15.7). The **P2** tier — the cloud/Kubernetes ops — is now **shipped (✅, 2026‑06‑20)** too: `KubernetesPodOperator` (`cncf‑kubernetes`, the user's explicit ask; starter params + ACE/cluster caveats), `S3KeySensor` (`amazon`) + `GCSObjectExistenceSensor` (`google`) in **Sensors**, and `BigQueryInsertJobOperator` (`google`, **Cloud**) — catalogue → 18, all gated. Growth is **data‑only** (one YAML per operator, §6.2 / Appendix A) and is sequenced by *impact × gating cost*. Class names / import paths / provider packages below are verified against Airflow 3.x provider docs (use the **non‑deprecated** Airflow‑3 paths). The reference UI's palette (HTTP + a full Sensors group) is the breadth target; **do not** re‑build the palette — only add YAML.

| Pri | Operator (class) | Provider pkg | Category | Impact / why |
|---|---|---|---|---|
| **P0 ✅** | `ShortCircuit` (ShortCircuitOperator) | standard *(bundled)* | Flow Control | Conditional gate that skips **all** downstream — the #2 flow primitive after the existing `Branch`; reuses the Python code‑node form. `@task.short_circuit` exists. |
| **P0 ✅** | `LatestOnly` (LatestOnlyOperator) | standard *(bundled)* | Flow Control | Skip downstream on backfill/catchup so only the latest interval runs; **zero** required params; cheapest add. Render as operator in both modes. |
| **P0 ✅** | `FileSensor` | standard *(bundled)* | Sensors | "Wait for input data to land" — most intuitive sensor; **establishes the Sensors category** + the sensor `common_params` (`mode` poke/reschedule · `poke_interval` · `timeout`). |
| **P0 ✅** | `ExternalTaskSensor` | standard *(bundled)* | Sensors | Cross‑DAG wait; **read‑side complement** to the existing `TriggerDagRun` for no‑code multi‑DAG pipelines. Highest‑effort P0 (`execution_delta` vs `execution_date_fn` — mutually exclusive; needs careful help copy). |
| **P0 ✅** | `DateTimeSensor` · `TimeDeltaSensor` | standard *(bundled)* | Sensors | Wait until a wall‑clock target / a relative delta; low‑effort, teachable. `TimeDelta` reuses the `timedelta` widget already needed for `retry_delay`. |
| **P1 ✅** | `HTTP` (HttpOperator) | apache‑airflow‑providers‑http | HTTP | Call any REST/webhook/SaaS endpoint — universally useful; the **first gated op**. Use `HttpOperator`, **not** the deprecated `SimpleHttpOperator`; steer users to an Airflow HTTP **Connection**, not raw URLs/secrets. |
| **P1 ✅** | `SQL query` (SQLExecuteQueryOperator) | apache‑airflow‑providers‑common‑sql | SQL | DB‑agnostic SQL — the Airflow‑3 path that **supersedes** per‑DB operators (`PostgresOperator`…); one op + a Connection covers Postgres/MySQL/Snowflake/… |
| **P1 ✅** | `SqlSensor` | apache‑airflow‑providers‑common‑sql | Sensors | Poll a DB until a query returns truthy (row‑count / flag / partition‑loaded) — data‑readiness gate under the same provider. |
| **P2 ✅** | `KubernetesPodOperator` | apache‑airflow‑providers‑cncf‑kubernetes | Kubernetes | Run any container image as a pod — the universal non‑Python/heavy‑job escape hatch (**the user's explicit ask**). HIGH impact / LOW breadth; flagship gated op — see below. |
| **P2 ✅** | `S3KeySensor` | apache‑airflow‑providers‑amazon | Sensors/AWS | Wait for an S3 object — cloud analogue of `FileSensor`. |
| **P2 ✅** | `GCSObjectExistenceSensor` | apache‑airflow‑providers‑google | Sensors/GCP | Wait for a GCS object — GCP analogue of `FileSensor`. |
| **P2 ✅** | `BigQueryInsertJobOperator` | apache‑airflow‑providers‑google | Cloud/GCP | Run a BigQuery job (current non‑deprecated path; supersedes `BigQueryExecuteQueryOperator`). Nested‑JSON `configuration` → high‑effort form. |

- **KubernetesPodOperator specifics — built ✅ (2026‑06‑20).** Import `airflow.providers.cncf.kubernetes.operators.pod` (the legacy `operators.kubernetes_pod` module is **gone** in current providers; verified against `cncf‑kubernetes 10.5.0`). Shipped the **starter** param set — `image`* · `name` · `namespace` · `cmds` · `arguments` (lists → `json` widget) · `env_vars` (dict) · `on_finish_action` (enum `delete_pod`/`delete_succeeded_pod`/`keep_pod`, replaces the old `is_delete_operator_pod` bool) · `kubernetes_conn_id`. The **advanced surface — shipped ✅ (2026‑06‑23, verified against `cncf‑kubernetes 10.18.0`):** dedicated fields for `node_selector` · `labels` · `annotations` · `security_context` (plain `dict`/`json`) and `service_account_name` · `priority_class_name` (`str`), plus `pod_template_file` (`str` path) and `pod_template_dict` (a raw JSON pod manifest consumed via `PodGenerator.deserialize_model_dict` and merged with the fields above). The typed‑object params (`volumes`/`volume_mounts` = `list[k8s.V1Volume/V1VolumeMount]`, `secrets` = `list[Secret]`, `affinity` = `k8s.V1Affinity`, `container_resources` = `k8s.V1ResourceRequirements`, `tolerations` = `list[k8s.V1Toleration]`) are **deliberately routed through `pod_template_dict`/`pod_template_file`** rather than dedicated fields — they require nested `k8s.V1*`/`Secret` constructions a JSON form can't model uniformly, and the declarative manifest path is the Airflow‑idiomatic way that needs no Python or `kubernetes` imports (keeping codegen data‑only with zero unused imports). **Gating:** needs the `cncf‑kubernetes` provider in the *target* Airflow **and** cluster/executor access (`in_cluster` or `kubernetes_conn_id`/`config_file` + a K8s‑capable executor) — Studio verifies the **provider** (palette dim + deploy hard‑fail) but **not** the cluster, so the cluster prerequisites + the **ACE caveat** (this runs an arbitrary image with worker/cluster privileges — the **same blast‑radius** as code nodes, §9) ride in the operator **`description`** that the INFO tab renders.
- **Provider‑availability gating (P1 prerequisite for every Tier‑2/3 op) — built ✅ (2026‑06‑19; `providers.py`, §15.7).** Gate on what's installed in the **target Airflow**, never the Jupyter/server env (the server parse‑check is best‑effort / false‑green, R2). Mechanism: (1) `provider` (already on every YAML) is the gating key; treat `apache‑airflow‑providers‑standard` / `(bundled)` as **always‑available** (standard is a core Airflow‑3 dep, present even in the slim image) so all **P0** ops are never gated. (2) Add a server capability that reads the target's installed providers (`GET /api/v2/providers` via the existing `AirflowClient`) and caches the package‑name+version set with a **short TTL / manual refresh** (installing a provider changes availability without a Studio restart). (3) `client_view()` annotates each palette entry `available | missing‑provider | version‑too‑old` (from target‑providers × `provider` × `airflow_min_version`). (4) UI: keep unavailable ops **visible but dimmed** with an `(i)` "Requires `apache‑airflow‑providers‑X` in your Airflow" tooltip + a copy‑paste `pip install` hint — **don't hide them** (they're teachable via INFO and the target may change), non‑color‑only, help‑never‑blocks. (5) **Hard‑gate at deploy:** the validate/deploy step re‑checks the IR's providers against the live target set and **fails fast** with a plain‑language "provider not installed in target Airflow" *before* writing the file, instead of an opaque `/importErrors` later. (6) `/api/v2/importErrors` stays the **authoritative** post‑deploy verdict (the worker env can still differ — provider present on the API node but a connection/cluster missing), so gating is a fast pre‑filter, not a correctness guarantee.

**6.2.2 Lakehouse operator expansion — the self‑hosted open‑source stack (P0 + P1 + P2 + third‑party P3 shipped ✅ → catalogue 44).** The §6.2.1 catalogue (18 ops) proved the mechanism on the standard + first gated providers; this tier grows it to cover a **self‑hosted open‑source lakehouse**: **MinIO** (S3‑compatible object store), **Trino + Iceberg / Postgres / MySQL / MSSQL** (query + tables), **Apache NiFi** (ingest), **Spark** (compute), **JupyterLab notebooks**, **FTP/SFTP/IMAP** (movement), **OpenMetadata** (governance), and **email/chat** notifications. Source research: `scope/airflow3-lakehouse-operators.md`. Growth stays **data‑only** (one YAML per op, §6.2 / Appendix A), gated by the existing provider‑availability mechanism (§6.2.1 / §15.7), and held to the same **wheel‑verification + live‑deploy bar** as every prior tranche (verify the import path, class, and *every* emitted constructor kwarg against the exact live provider version; render through `generate_dag` to `ast.parse`+`compile`; deploy end‑to‑end on `apache/airflow:3.0.2`). Adds six palette **categories**: **Storage**, **Ingestion**, **Compute**, **Data Quality**, **Notifications**, **Governance**.

Airflow‑3 ground rules (from the research doc, baked into the roadmap):
- **Per‑DB SQL operators are deprecated** — `SQLExecuteQueryOperator` (already shipped, `sql.yaml`) covers **Trino, Postgres, MySQL, MSSQL** purely by **Connection**; do **not** add per‑DB operator YAMLs. The DB‑specific work is instead a **connection‑type / `conn_id` picker** on the existing SQL node (and documenting that the `trino`/`postgres`/`mysql`/`microsoft‑mssql` provider packages must be installed for those connection types to exist). The only genuinely new `common‑sql` additions are the **data‑quality checks** below.
- **MinIO == S3** — every `amazon` S3 op reaches MinIO by pointing the AWS Connection's `endpoint_url` at the MinIO host; no AWS account needed (surface this in the op `help`).
- **NiFi has no provider** — orchestrate it via its REST API with the existing `HttpOperator` (`http.yaml`); ship an HttpOperator **preset/help** documenting NiFi's flow‑control endpoints rather than a NiFi YAML.
- **JupyterLab notebooks** run via `PapermillOperator` (no notebook operator exists) — a strong fit since Studio lives inside JupyterLab.
- **Email** was **removed from core in 3.0** — `EmailOperator` now imports from `airflow.providers.smtp.operators.smtp`.
- **Dask** has no Airflow‑3 operator/executor (provider removed) — run it from a `PythonOperator`/`KubernetesPodOperator` against an external Dask `Client`; **no Dask palette node**.
- **Not operators — never palette nodes:** the **object‑storage XCom backend** (`common‑io`, a configured backend for spilling large artifacts to MinIO), the **OpenLineage** auto‑lineage **listener** (`openlineage`), and OpenMetadata's **managed‑apis** ingestion **plugin**. These belong (if anywhere) in a deploy‑environment / settings surface, not the operator palette.
- **Operator vs notifier:** Slack/Discord/Telegram/Opsgenie/SMTP each ship **both** a **task operator** (a graph node) **and** a **callback notifier** (`*Notifier`) from the same package. The IR models task nodes + edges only — it has **no callback concept** — so **operators ship here as palette nodes; notifiers are deferred to §6.8** (a callbacks surface). The registry/palette must disambiguate the two so a single "send Slack" concept doesn't conflate them.

| Pri | Operator (class) | Provider pkg | Category | Impact / why |
|---|---|---|---|---|
| **P0 ✅** | S3 create object (`S3CreateObjectOperator`) | amazon | Storage | The core **MinIO write** path (+ "generate artifact in Python → upload"); pairs with the shipped `S3KeySensor`. Highest‑value storage op. |
| **P0 ✅** | SFTP transfer (`SFTPOperator`) | sftp | Ingestion | Core ingest/egress for landing zones; deferrable `SFTPTrigger`. |
| **P0 ✅** | SQL column check (`SQLColumnCheckOperator`) | common‑sql | Data Quality | Built‑in null/unique/min/max DQ over any SQL conn (Trino/PG/MySQL/MSSQL); zero extra deps — same provider as `sql.yaml`. |
| **P0 ✅** | SQL table check (`SQLTableCheckOperator`) | common‑sql | Data Quality | Row‑count / custom‑predicate table assertions; native alternative to Great Expectations with no third‑party install. |
| **P0 ✅** | Spark submit (`SparkSubmitOperator`) | apache‑spark | Compute | Primary lakehouse compute (Spark + Iceberg); heavy blast radius like KPO (worker `spark‑submit` / a Spark conn). |
| **P0 ✅** | Papermill (`PapermillOperator`) | papermill | Compute | The **only** way to run a JupyterLab `.ipynb` as a task — strong product fit; the executed notebook is itself an artifact. |
| **P0 ✅** | Email (`EmailOperator`) | smtp | Notifications | A send‑email **task** (not the `SmtpNotifier` callback). Moved out of core in 3.0 → `airflow.providers.smtp.operators.smtp`. |
| **P0 ✅** | Slack post (`SlackAPIPostOperator`) | slack | Notifications | Post a message as a graph node (bot‑token conn). The operator, not `SlackNotifier`. |
| **P1 ✅** | S3 copy / list / delete (`S3CopyObjectOperator`, `S3ListOperator`, `S3DeleteObjectsOperator`) | amazon | Storage | Promote/copy, fan‑out listing, cleanup. **Delete is destructive** — blast‑radius warning in the `description` + form `help`. |
| **P1 ✅** | SFTP sensor (`SFTPSensor`) · SFTP→S3 (`SFTPToS3Operator`) | sftp · **amazon** | Sensors · Ingestion | Wait‑for‑remote‑file + one‑shot SFTP→MinIO transfer (needs an sftp **and** an s3 conn). **`SFTPToS3Operator` lives in the `amazon` provider's `transfers`, not `sftp`** (wheel‑verified — the research doc was wrong). |
| **P1 ✅** | Spark SQL (`SparkSqlOperator`) | apache‑spark | Compute | Spark SQL via the `spark‑sql` CLI; same worker requirement as submit (`conn_id` default `spark_sql_default`). |
| **P1 ✅** | Slack webhook (`SlackWebhookOperator`) | slack | Notifications | Incoming‑webhook message (simpler auth than the API operator); `slack_webhook_conn_id` is required. |
| **P2 ✅** | FTP transmit / sensor (`FTPFileTransmitOperator`, `FTPSensor`) | ftp | Ingestion · Sensors | Legacy/insecure feeds; lower priority than SFTP. |
| **P2 ✅** | IMAP attachment → S3 / sensor (`ImapAttachmentToS3Operator`, `ImapAttachmentSensor`) | **amazon** · imap | Ingestion · Sensors | Email‑attachment ingest into MinIO. **`ImapAttachmentToS3Operator` lives in the `amazon` transfers** (like `SFTPToS3`); only the sensor is in `imap`. |
| **P2 ✅** | Spark JDBC (`SparkJDBCOperator`) · Spark‑on‑K8s (`SparkKubernetesOperator`) | apache‑spark · cncf‑kubernetes | Compute | Spark↔JDBC bulk transfer; Spark CR submission (`SparkKubernetesOperator` lives in **cncf‑kubernetes**, already shipped for KPO; needs one of `application_file`/`template_spec`). |
| **P2 ✅** | Discord / Telegram / Opsgenie (`DiscordWebhookOperator`, `TelegramOperator`, `OpsgenieCreateAlertOperator`) | discord · telegram · opsgenie | Notifications | More notification **operators** (on the constraints file); their `*Notifier` callback siblings stay in §6.8. |
| **P3 ✅** | Notifiers (`SmtpNotifier`, `SlackNotifier`, `AppriseNotifier`, `DiscordNotifier`, `OpsgenieNotifier`) | smtp · slack · apprise · discord · opsgenie | Notifications | **Not task nodes** — callbacks, on the §6.8 surface. 5 shipped 2026‑06‑22 (notifier registry). `AppriseNotifier` is the multi‑channel path (incl. **Microsoft Teams** via a Power Automate Workflows webhook — old `webhook.office.com` connectors retire May 2026). **No `TelegramNotifier`** — the telegram provider ships no notifications module (wheel‑verified; the operator covers Telegram). |
| **P3 ✅** | Great Expectations (`GXValidateCheckpointOperator`) | airflow‑provider‑great‑expectations ¹ | Data Quality | **Third‑party** (off the constraints file). Shipped 2026‑06‑23 as a **code‑first** node. Wheel‑check catch: the legacy `GreatExpectationsOperator` was **removed in v1.0.0** → it's now three `GXValidate*Operator` classes that take Python **callables**; we model `GXValidateCheckpointOperator` (single `configure_checkpoint` callable). Prefer the native P0 SQL checks for simple assertions. |
| **P3 ✅** | OpenMetadata lineage (`OpenMetadataLineageOperator`) | openmetadata‑ingestion ¹ | Governance | **Third‑party**, and the package must match the OpenMetadata **server** version, not Airflow's — a hard pin caveat; high integration cost. Shipped 2026‑06‑23 as a **code‑first** node (`server_config` is a constructed `OpenMetadataConnection` object, not a string/dict). Reads the DAG's task inlets/outlets to push lineage. |

¹ **Third‑party packages are off the Airflow constraints file** — they can't be installed via the constrained `apache‑airflow[…]==3.x.y -c constraints` command and need separate, independently‑pinned installs (OpenMetadata tracks its **server** version). A palette entry for one is (a) flagged `third_party: true` in the registry with its own `version`, and (b) given a distinct **`third‑party`** availability state — *shown, never gate‑blocked* — because `/api/v2/providers` is not an authoritative install signal for off‑constraints packages in general (some don't register as Airflow providers at all, and it can never confirm OpenMetadata's *server*‑version match). The palette shows a pinned‑install note (`pip install <pkg>==<version>`) and `/importErrors` (+ the §7 friendly recovery) is the deploy‑time verdict. **Resolved 2026‑06‑23 (§13 Q13, option B):** the `provider_block_errors` deploy hard‑gate skips third‑party ops. *(Aside: both shipped P3 packages happen to register an `apache_airflow_provider` entry point, so they would appear in `/api/v2/providers` when installed — but the gating deliberately does not rely on that, since it doesn't generalize to twilio‑like SDKs.)*

**Heavy‑blast‑radius ops** (`SparkSubmit`/`SparkSql`/`SparkKubernetes`, the shipped `KubernetesPodOperator`, and the destructive `S3DeleteObjectsOperator`) run arbitrary jobs or delete data on the worker/cluster — they share the `kubernetes_pod.yaml` treatment: an install/prereq note + ACE caveat in the operator `description`, palette dim + deploy hard‑fail when the provider is absent, and (for delete) a clear blast‑radius warning in the form `help` (§9).

**P0 shipped (✅ 2026‑06‑22) — catalogue 18 → 26.** The 8 P0 ops landed as registry YAML (both `template_taskflow` + `template_traditional`): `S3CreateObjectOperator` (Storage), `SFTPOperator` (Ingestion), `SQLColumnCheckOperator` + `SQLTableCheckOperator` (Data Quality), `SparkSubmitOperator` + `PapermillOperator` (Compute), `EmailOperator` (Notifications, `smtp`), `SlackAPIPostOperator` (Notifications). **Import paths, classes, and every emitted constructor kwarg were verified against the real provider wheels** (amazon 9.30, sftp 5.8, common‑sql 2.0, apache‑spark 6.1, papermill 3.13, smtp 3.0, slack 9.10) — catching, e.g., that `SFTPOperator` takes **`ssh_conn_id`** (not `sftp_conn_id`) with `operation="put"`, and that the Slack default conn is `slack_api_default`. Each renders valid Airflow‑3 Python in **both** families (`ast.parse`+`compile`), with optional kwargs `{% if %}`‑guarded (required‑first ordering, blank‑stripped) and the per‑node `common` settings emitted. They gate on their provider like the other tiers (palette dim + deploy hard‑fail when absent). **Remaining gate:** a live end‑to‑end deploy on the devcontainer's `apache/airflow:3.0.2` with the providers installed (the Jupyter env here has neither Airflow nor the providers) — the standard live‑deploy step from prior tranches.

**P1 shipped (✅ 2026‑06‑22) — catalogue 26 → 33.** 7 more wheel‑verified ops: `S3CopyObjectOperator` · `S3ListOperator` · `S3DeleteObjectsOperator` (Storage, amazon), `SFTPSensor` (Sensors, sftp — full sensor `common_params`), `SFTPToS3Operator` (Ingestion, **amazon**), `SparkSqlOperator` (Compute), `SlackWebhookOperator` (Notifications). Verified against amazon 9.30 / sftp 5.8 / apache‑spark 6.1 / slack 9.10, both template families render + `compile`. Catches the wheel‑check + a focused adversarial review surfaced: (1) **`SFTPToS3Operator` is in the `amazon` provider's `aws/transfers`, not `sftp`** (the research doc's listing was wrong); (2) the `S3DeleteObjectsOperator` `keys` param **collides with the dict `.keys` method in Jinja** — *both* `params.keys` (attr) **and** `params['keys']` (subscript falls back to the attr when the key is absent) return the bound method, which broke a legitimate **prefix‑only** delete (emitted `keys=<built-in method…>`); the template uses **`params.get('keys')`** (an explicit call → `None` when absent). The P1 review caught the prefix‑only breakage; +regression test for the keys **and** prefix‑only paths. `S3DeleteObjects` requires **Keys or Prefix** (exactly one — the `description` says so; with neither, Airflow raises a clear import error, surfaced by the §7 recovery) and carries the destructive blast‑radius warning (§9). (3) The `SlackWebhookOperator` `channel` help wrongly implied an override worked — **standard Slack Incoming Webhooks ignore `channel`** (only legacy custom‑integration webhooks honour it; verified in the slack 9.10 hook's `LEGACY_INTEGRATION_PARAMS` + `UserWarning`) → reworded. Same remaining gate: a live deploy on the devcontainer `3.0.2`.

**P2 shipped (✅ 2026‑06‑22) — catalogue 33 → 42.** 9 more wheel‑verified ops (ftp 3.15, imap 3.11, amazon 9.30, apache‑spark 6.1, cncf‑kubernetes 10.18, discord 3.12, telegram 4.9, opsgenie 5.10), each driven by a parallel **spec‑verification workflow** (one verifier per provider): `FTPFileTransmitOperator` + `FTPSensor` (ftp), `ImapAttachmentToS3Operator` (**amazon** transfers) + `ImapAttachmentSensor` (imap), `SparkJDBCOperator` (apache‑spark) + `SparkKubernetesOperator` (cncf‑kubernetes), `DiscordWebhookOperator` (discord) + `TelegramOperator` (telegram) + `OpsgenieCreateAlertOperator` (opsgenie). Boolean kwargs are declared `type: boolean` so codegen emits a Python `True`/`False` (not a quoted string); dict/list kwargs (`template_spec`, `tags`, `details`) use `type: object`/`array` + `widget: json`. Wheel‑check catches baked in: `ImapAttachmentToS3Operator` is in **amazon**, not imap (like `SFTPToS3`); the IMAP **sensor**'s conn kwarg is `conn_id` (not `imap_conn_id`); Telegram's real message kwarg is **`text`** (not `message`); Discord's required conn kwarg is **`http_conn_id`**; `SparkKubernetes` needs **at least one** of `application_file`/`template_spec` (the file wins if both; neither → an `AirflowException` at **run time**, not at parse — the P2 review caught the YAML wrongly saying "won't load") and carries the ACE blast‑radius warning (§9). Same remaining gate: a live deploy on the devcontainer `3.0.2`.

**P3 third‑party shipped (✅ 2026‑06‑23) — catalogue 42 → 44.** The two off‑constraints ops, plus the §13 Q13 **gating extension** (`third_party: true` + own `version` → a distinct **`third‑party`** availability state that is shown but never deploy‑blocked; `/importErrors` is the verdict). Both wheel‑verified (airflow‑provider‑great‑expectations 1.0.0, openmetadata‑ingestion 1.13.0.0) and rendered through `generate_dag` (`ast.parse`+`compile`) in both families. Both are inherently **code‑first** nodes (a `code`‑widget param holding a user callable body, mirroring `python_task`/`branch`): GE wraps `def configure_checkpoint_<tid>(context): …` and passes the **function**; OpenMetadata wraps `def server_config_<tid>(): …` and passes the **called** result (an `OpenMetadataConnection`). The optional kwargs use Jinja `{%- if -%}` whitespace trimming (the operator‑block blank‑strip is skipped for code‑param ops, to preserve the user body) so an omitted optional leaves no stray blank. **Wheel‑check catch:** GE's legacy `GreatExpectationsOperator` no longer exists in v1.0.0 (3 `GXValidate*` callable‑based operators replace it) — the research doc / earlier PRD name was stale. The §6.8 notifier callbacks already shipped (5 channels). Remaining gate (shared with P0–P2): a live deploy on the devcontainer `3.0.2`.

### 6.3 Code generation

- **Authoritative codegen is server‑side** (Python + Jinja2), because only the server can parse‑check against an Airflow install and because templates + import paths live with the deploy target. Client TS does *instant, non‑authoritative* hints only.
- The **IR is syntax‑agnostic** (`syntax_style`); the mode selects a template family — **both built ✅ (2026‑06‑20)**, switched by the top‑bar toggle (§6.1.4) and `_render` keying on `ir.syntax_style`:
  - **TaskFlow** (`from airflow.sdk import dag, task`): `@dag(...)` wrapping `@task`‑decorated functions; a native op is instantiated by a `task_id_task = task_id()` call; dependencies expressed by `>>`. Code nodes are TaskFlow‑native.
  - **Traditional** (`from airflow.sdk import DAG` + operator‑class imports): `with DAG(...) as dag:` + operator instances + `>>` wiring. **Every** op renders as an operator instance via its `template_traditional` (a code node as `PythonOperator(python_callable=…)` / `BranchPythonOperator` etc.). (`chain()`/`cross_downstream()` collapse for the common fan shapes is a follow‑up 🔭.) Verified: `from airflow.sdk import DAG` is exported by the Airflow‑3 task SDK; output parses + compiles.
- **Airflow 3.x correctness (verified):** emit `airflow.sdk` for `DAG`/`dag`/`task`/`chain`, and **`airflow.providers.standard.*`** for operators/sensors. **Never** emit Airflow‑2 paths (`airflow.operators.bash`, `airflow.models.DAG`, `airflow.decorators.task`) — they fail to import in Airflow 3. Defaults: `catchup=False`; `retry_delay` as `timedelta`; `start_date` as `datetime`; `schedule` handled distinctly for `None`/preset/cron/`timedelta`.
- **Determinism:** format output with `black`/`ruff format` so identical IR → byte‑identical file (idempotent deploys, clean diffs for the future Git target).
- **Toggle = two backends that must be semantically equivalent.** This is a top correctness risk (R7). **Shipped ✅** with a codegen **task‑graph equivalence test** (`test_codegen.py::test_taskflow_and_traditional_yield_the_same_task_graph`): the same IR renders in both families and is asserted to yield the **same tasks + the same `>>` dependency edges** (handles resolved to `task_id`s). **Caveat:** the families pass Airflow **context** to a code node differently — TaskFlow `@task` (use `get_current_context()`) vs Traditional `PythonOperator(python_callable=…, **context)` — so a context‑dependent user body is not transparently portable; the graph is equivalent, the body contract differs.

See **Appendix C** for example output.

### 6.4 Validation & live errors

Two layers (client = instant UX, server = authority):

- **Client (instant):** Kahn topological sort for **cycle detection** (also yields a topo order for codegen) → drives the cycle message; per‑node **required‑field** checks from the registry → red/green (icon+text) node dots; the top‑bar badge = `cycleError + Σ node errors`. The IR is the single source of truth; ReactFlow state and RJSF form data are projections.
- **Server (authoritative, before deploy):** re‑validate the untrusted `.afdag` IR (schema + cycle + required), sanitize identifiers, render, then run the parse pipeline (Appendix E). **Client validation is never trusted** — the IR is just JSON a client can hand‑craft.
- **Post‑deploy (the real verdict):** Airflow's own parser. Studio polls `/api/v2/importErrors` and surfaces the result. *The server parse‑check is explicitly best‑effort* (Jupyter env ≠ Airflow worker env; provider packages/connections may differ).

### 6.5 Deployment & sharing

**6.5.1 `DeployTarget` interface** — `write(filename, content)` (atomic), `exists`, `list` (managed files + provenance), `read`, `delete`, `verify`, and a **consistency flag** (synchronous‑visible vs eventually‑consistent) so the verification poll adapts. `SharedVolumeTarget` (✅) and the **`GitDeployTarget` (✅ 2026‑06‑23)** implement it; a `get_deploy_target()` factory selects by `AIRFLOW_DEPLOY_TARGET` (`shared_volume` default · `git`), so the handlers/`deploy_dag`/rollback/purge/orphan/drift paths are target‑agnostic. **`GitDeployTarget`** *extends* `SharedVolumeTarget` (reusing namespacing, atomic write, the provenance‑header collision guard, backup/rollback, `.airflowignore`); each mutating op additionally `git add` + `git commit`s the change in the repo's DAG subdir (and `git push`es when `AIRFLOW_GIT_DAGS_REMOTE` is set), mapping to an Airflow **`GitDagBundle`**. The `.bak` backup stays untracked (only the `.py` + `.airflowignore` are staged); git calls are timeout‑bounded so an unreachable remote can't hang the deploy. **`S3DeployTarget` (✅ 2026‑06‑23)** writes each DAG as an S3 object under `AIRFLOW_S3_DAGS_PREFIX` (default `dags`) in `AIRFLOW_S3_DAGS_BUCKET` (AWS S3 or any S3‑compatible store via `AIRFLOW_S3_ENDPOINT_URL`, e.g. MinIO), reusing the same namespacing/provenance/collision/backup semantics (`put_object` is the atomic write; the `.bak` is a sibling object; `list` paginates `ListObjectsV2`); the boto3 client is lazily created + injectable, so the module imports without boto3 and tests run against a faithful in‑memory fake. All three targets share `get_deploy_target()`.

**6.5.2 Shared‑volume deploy (atomic).** Write a temp file **in the same directory** as the target, `fsync`, then `os.replace(tmp, final)` (atomic + overwrite on POSIX/Windows; cross‑filesystem rename is **not** atomic, so temp must be co‑located). Filename is deterministic and **namespaced** (see §8.9). Drop an `.airflowignore` (glob syntax in Airflow 3) covering the temp/staging pattern and `.afdag` sidecars.

**6.5.3 Collision & overwrite safety.** Before writing: read back the target dir; **refuse to overwrite any file lacking the Studio provenance header** (it's a hand‑written, read‑only DAG); detect `dag_id` duplication; on a managed file that was hand‑edited (its `code=sha256` body hash, recorded in the provenance header, no longer matches the file body) the deploy preflight flags **drift** and the editor prompts *"modified outside Studio — overwrite or cancel?"* before re‑deploying (**implemented**, §6.5.5; the manager‑side "reopen read‑only" is a later surface). See §9.

**6.5.4 Deploy lifecycle (the central success path).** Because Airflow 3 has **no on‑demand bundle‑refresh REST API** and the dag‑processor scans on `refresh_interval` / re‑parses on `min_file_process_interval` (and standalone has a known refresh‑timing bug), Deploy is an **observable tri‑state**:
1. *Writing…* → atomic write succeeds.
2. *Waiting for Airflow to pick it up…* → poll `GET /api/v2/dags` for the `dag_id` **and** `GET /api/v2/importErrors` filtered to the filename, with bounded backoff and an explicit timeout (communicate "up to a few minutes").
3. Resolve to **Registered** (dag appears, no import error) · **Failed to import** (import error → friendly message + traceback expander + map to node/field) · **Still processing** (timeout → keep polling / let the user dismiss).
- **Run on deploy (required; decision 2026‑06‑17 — every deploy runs).** Deploy does **not** stop at *Registered*: once the dag registers, the server **unpauses then triggers one run** over the Airflow API — `PATCH /dags/{id}?update_mask=is_paused` (`is_paused=false`), **then** `POST /dags/{id}/dagRuns` with a null `logical_date`. **Order matters:** a run triggered while the dag is still **paused** is created but sits `queued` and never executes until it is unpaused (§8.8), so unpause must come first. This is a **direct API round‑trip** (not pickup‑dependent), so the banner advances *Registered → Running* and exposes **Stop run** (§6.6). The §6.5.5 active‑run guard still gates the *write* (a re‑deploy over an in‑flight run is blocked); a first deploy has no prior run to strand. A "deploy paused / don't run" escape hatch is an open follow‑up (§13).

**6.5.5 Updating a deployed DAG (re‑deploy).** Editing a Studio DAG and deploying again **overwrites the same `{dag_id}.py`** in place (atomic `os.replace`; Studio‑managed files overwrite freely, hand‑written are refused, §6.5.3) and re‑runs the deploy lifecycle (§6.5.4). **Active‑run guard (required):** because `LocalDagBundle` has no versioning and always runs the *latest file on disk* (§8.8), overwriting a DAG with a run **in flight** can corrupt it (removed/renamed tasks orphan; structure shifts under the running scheduler). Before a re‑deploy the editor runs the **shared dag‑state preflight** — the `active_runs` of the current `dag_id` from `list_dag_runs` (running/queued), the *same* check the rename migration uses — and, if the DAG is registered with an active run, **blocks** with *Cancel* / *Deploy anyway* (an explicit override). A preflight failure falls through to deploy (the user clicked Deploy; if Airflow is unreachable nothing is running). This is **distinct from a rename**: an update keeps the `dag_id` (same file, same history); only a `dag_id` *change* is the migration in §6.1.8(B). **Out‑of‑band drift** detection is **implemented**: the deploy preflight compares the file body to a `code=sha256` hash stamped in its provenance header, and a drifted file prompts *overwrite or cancel* before re‑deploy (§6.5.3). **Undeploy / rollback ✅ (§7):** every overwrite‑deploy first backs up the prior managed `.py` to `{dag_id}.py.bak`, so the deploy banner can **Roll back to previous** after a failed import, and **Undeploy** (file + history) the open DAG. Caveat: for an update the tri‑state's `registered` verdict can't yet distinguish *new version parsed* from *old version still live* (Airflow's REST API doesn't expose the on‑disk `ir‑hash`).

**6.5.6 Undeploy & orphan reconciliation (delete the DAG when its `.afdag` is deleted).** A deployed DAG's source of truth is its `.afdag`; deleting that design file should **delete the deployed DAG** (decision 2026‑06‑17: **full delete** — remove the namespaced `.py` **and** `DELETE /api/v2/dags/{id}` to purge run history, the same teardown as the manager's Delete in §6.6). Because the `.afdag` lives in the Jupyter workspace and the `.py` in the (out‑of‑root) dags folder, the deployed→source link is the **`afdag_id` provenance join** (§8.9): every managed `.py` carries `afdag_id=<uuid>`, and an **orphan** is a deployed managed `.py` whose `afdag_id` no longer matches any `.afdag` under the Contents root. Detection is **two‑layered** (decision 2026‑06‑17 — **both**):
1. *In‑session signal* — subscribe to JupyterLab's `serviceManager.contents.fileChanged` (filter `type==='delete'` and `oldValue.path` ending `.afdag`). A `.afdag` deleted **inside** the running JupyterLab re‑runs the orphan sweep **immediately** (`panel.refresh()`) so the manager surfaces the now‑orphaned DAG at once instead of waiting for the next manual refresh (§15.13).
2. *Server reconciliation sweep* — a server endpoint (`dags/orphans` → `find_orphans`) walks the Contents root for live `afdag_id`s and diffs them against `SharedVolumeTarget.list()` (deployed managed files) to return the orphan set. The manager runs it on every refresh and surfaces orphans as a banner (§15.13). This is the **only** layer that catches deletes done outside the session (terminal, `rm`, `git checkout`), which fire no `fileChanged`.

Both paths are **destructive and confirmed, never silent** (§9): a `.afdag` can vanish from a `git` operation or an accidental `rm`, so a purge requires explicit per‑DAG confirmation — the sweep *flags*, the user *confirms*. Reconcile is the mirror of §6.6 Delete (file‑first, then history) and a sibling of the existing **drift** detection (which reconciles *edited‑but‑present* files; this reconciles *deleted‑source* ones). Airflow refuses `DELETE /dags/{id}` while a task instance is running (§8.8), so an orphan with an active run is surfaced but blocked until the run ends or is stopped. Wireframe §15.13.

### 6.6 Resource Manager (sidebar, extended)

Extends the existing `AirflowPanel`. Requirements (endpoints in Appendix D):
- **List** with tag filter + `dag_id` search; flag DAGs with `has_import_errors=true`. *(Fix the existing `only_active` → v2 `exclude_stale`/`paused`; send list params form‑exploded.)*
- **DAG detail / source** (read‑only view for hand‑written DAGs via `dagSources`).
- **Pause/unpause** (existing, correct).
- **Trigger** with a **conf form derived from the DAG's `params`** (`/dags/{id}/details`); allow null `logical_date` for an immediate run (Airflow 3).
- **Runs** → **task instances + states** → **task logs** (paged by continuation token, tail while running).
- **Friendly log viewer ✅ (replaces the raw `<pre>` dump; §15.9).** The log modal is now a structured viewer (`LogViewer.tsx`): each line is **level‑classified client‑side** from its text (INFO/WARNING/ERROR/CRITICAL/DEBUG; a Python traceback with no level token is treated as an error) and rendered with **per‑level colour + an error left‑bar** (non‑color‑only), it **autoscrolls to the first error** on open, and the toolbar adds an **attempt selector** (try 1…N — re‑fetching that try over the existing API), **search**, an **errors‑only** filter, **Copy**, **Download**, and a **Wrap** toggle, with a **loading/error state kept distinct from content**. The loader ignores a stale response if the user switches task/try. **Structured events ✅ (2026‑06‑24, §13 Q16):** the server now **passes Airflow 3's structured log events through** (`client.get_task_logs` → `{content, events?}` where each event is `{event, timestamp?, level?, logger?}` — verified against `apache‑airflow‑core 3.0.2` `StructuredLogMessage`), so the viewer colours by the **server‑provided `level`** when present (falling back to the text `classifyLine` per‑event when a handler omits it, and to whole‑text classification for a plain‑text/parse‑error response). The flattened `content` string is still returned for **Copy/Download** and the plain‑text path, so the change is **back‑compatible**. (Live ndjson tailing remains deferred 🔭 — not worth the polling complexity yet.) `Overlay` also gained **Escape‑to‑close + focus‑on‑open** (benefits every dialog).
- **Import errors** view (`/api/v2/importErrors`) — *the recovery surface*; translate `stack_trace` to plain language.
- **Clear/retry** (`clearTaskInstances`, `dry_run` preview first) and **mark success/failed/skipped** (with dry‑run preview).
- **Stop / terminate a run** (manager **and** editor; decision 2026‑06‑17). Airflow 3 has **no cancel endpoint** for a normal run — stopping an in‑flight run = `PATCH /api/v2/dags/{id}/dagRuns/{run_id}` with `state:"failed"` (the scheduler then terminates its running task instances; only *Backfills* expose a true `cancel`). Surface a **⏹ Stop** on a `running`/`queued` run in the manager's run list (§15.9) and in the editor/deploy banner while a run is in flight (§15.6); confirm (it fails the run). Distinct from **Clear/Retry** (re‑runs tasks) and **Delete** (removes the DAG).
- **Delete** = remove the namespaced `.py` + `.afdag` via `DeployTarget` **first** (so it isn't re‑imported), **then** `DELETE /api/v2/dags/{id}` to purge history; irreversible‑action confirmation. (Airflow refuses the delete while a task instance is running, §8.8 — stop the run first.)
- **Refresh:** tiered visibility‑gated polling keyed off `autoRefreshSeconds` (collapsed list ~15–30s; active run 3–5s; open running‑log tail 2–3s); pause when hidden/offscreen; back off on 429/5xx. (No websockets in Airflow `/api/v2`; the experimental single‑run `wait` ndjson stream may be proxied later.)

### 6.7 Advanced code‑editor task nodes (decision #3)

- A registry entry whose single param is `code` (`widget: code`, CodeMirror 6 reused from JupyterLab). The user's code is emitted **inside** a `@task` function body (TaskFlow) or wrapped as `PythonOperator(python_callable=...)` (Traditional) — **never at module top level**, so a user error can't break the whole file's import.
- **This is an intentional arbitrary‑code‑execution surface** (the code runs on Airflow workers with their privileges). It is governed by the trust boundary in §9: linted via AST/ruff, parse‑checked in an isolated subprocess, gated by who may deploy, and documented. For the non‑technical majority the code editor is hidden unless a Python/Custom‑`@task` node is selected.

### 6.8 Notifications & alerting (callbacks) (DAG‑level + per‑task shipped ✅ 2026‑06‑22)

Airflow's notification channels split in two, and the IR models only one half today:
- **Operators** (graph nodes) — `EmailOperator`, `SlackAPIPostOperator`/`SlackWebhookOperator`, `DiscordWebhookOperator`, `TelegramOperator`, `OpsgenieCreateAlertOperator`. Each is a *task* ("send a Slack message" as a step), fits the existing node model, and ships in §6.2.2.
- **Notifiers** (callbacks) — `SmtpNotifier`, `SlackNotifier`, `AppriseNotifier`, `DiscordNotifier`, `OpsgenieNotifier` (there is **no** `TelegramNotifier` — the telegram provider ships no notifications module). These attach to **`on_success_callback` / `on_failure_callback`** at the DAG or task level and **`on_retry_callback`** at the task level — they are **not** nodes and cannot be dropped on the canvas. (`sla_miss_callback` is gone — SLAs were removed in Airflow 3.0.)

**The gap** (now closed for DAG‑level callbacks ✅ 2026‑06‑22): the `.afdag` IR (§8.3) had no callback model, so "email me when this DAG fails" was unexpressible. As built:
- **IR ✅:** an optional **`callbacks`** block keyed by event, on **`ir.dag.callbacks`** (DAG‑level: `on_success` / `on_failure`) **and on each `ir.nodes[].callbacks`** (per‑task: `on_success` / `on_failure` / **`on_retry`** — the task‑only event the DAG level can't express; all three **fire** in Airflow 3, wheel‑verified against task‑sdk 1.2.2). `sla_miss` is excluded because **SLAs were removed in Airflow 3.0** — that kwarg only emits a `DeprecationWarning` and never fires, with "Deadline Alerts" the 3.1+ replacement (and there is **no** task‑level `sla_miss` callback at all). Each value is a list of `{ notifier_id, params }` referencing a **notifier registry** entry. The DAG block lives on `dag`, the task blocks on their node — both kept out of `edges[]` so cycle‑check is untouched; absent on older `.afdag` files. **Deferred 📝:** the niche `on_execute` / `on_skipped` task events (both fire but are noisy/edge — addable later with no IR change); Deadline Alerts (3.1+) as the SLA replacement.
- **Notifier registry ✅:** a YAML‑per‑notifier mirror of the operator registry (`jupyterlab_airflow/notifiers/*.yaml` + `load_notifiers`/`notifier_client_view`, with an `AIRFLOW_NOTIFIERS_DIR` override): `import`, a Jinja `template` rendering the notifier *instance*, params + `help`, `provider`, `airflow_min_version`. Channels (all wheel‑verified): **`SmtpNotifier`** (email), **`SlackNotifier`** (Slack), **`AppriseNotifier`** (multi‑channel — Teams/WhatsApp/…), **`DiscordNotifier`**, **`OpsgenieNotifier`** (its `payload` is a JSON object). Served by **`GET notifiers`**, provider‑gated like operators.
- **Codegen ✅:** `_build_callbacks` renders the DAG‑level notifier instances and appends `on_<event>_callback=[…]` to the `@dag(…)` (and Traditional `with DAG(…)`) call. `_node_callbacks` renders the **per‑task** instances and **merges** them into the task's trailing `common` kwargs — so every operator template's existing `{{ common | pyargs }}` slot emits them with **no per‑operator edit**: into the `@task(…)` decorator for native ops (the decorator forwards them to the underlying operator) and into the operator call otherwise; this rides the same path in **both** TaskFlow and Traditional. The notifier imports (DAG‑ and task‑level) are collected/sorted. An unknown notifier fails codegen with a plain‑language error. Output‑preserving when no callbacks are set.
- **UI ✅:** a **"Notifications" inspector tab** (DAG · NODE · INFO · **NOTIFY** · CODE · SAVED) edits **DAG‑level** callbacks; a matching **"Notifications" section in the NODE tab** edits the selected task's **per‑task** callbacks (incl. `on_retry`). Both render from one shared **`CallbacksEditor`** — per event, list / add / remove notifiers, each with a registry‑driven RJSF form (the same `help` / `ⓘ`‑bubble machinery as the NODE form); an unavailable notifier shows a "needs `pip install …`" note. **Microsoft Teams** / **WhatsApp** ride via `AppriseNotifier` / `HttpOperator`.
- **Gating & trust ✅:** the `GET notifiers` payload is availability‑annotated (notifiers gate on their `provider` like any op, §6.2.1) — the palette dims an unavailable channel and the **deploy hard‑gate** (`provider_block_errors`) scans **both** `dag.callbacks` and every `node.callbacks` and **blocks pre‑write** on a missing/too‑old notifier provider, mirroring operators. **Required‑field validation**: a notifier missing a required param (e.g. Slack `text`) — at the DAG **or** task level — feeds the editor error badge so Deploy is blocked, just like a NODE form. A notifier runs provider code in the scheduler/worker — the same trust boundary as operators (§9).

Wireframe **§15.14 ✅**. The §6.2.2 **P3 notifier** rows now ship 5 channels — `smtp`, `slack`, `apprise`, `discord`, `opsgenie` (no Telegram notifier — the telegram provider ships none).

### 6.9 Asset / data‑aware scheduling (Airflow 3) — shipped ✅ (2026‑06‑23)

Airflow 3 renamed Datasets → **Assets** and lets a DAG be **scheduled on asset updates** instead of a clock (Studio does a hard override — a combined `AssetOrTimeSchedule` is deferred 🔭). Studio surfaces the two halves of the producer/consumer loop, modelled as **plain data on the IR** (no new graph topology — assets are not nodes/edges, so cycle‑check and validation are untouched):

- **Schedule on assets (consumer side)** — a new DAG‑field `dag.schedule_assets: string[]` (edited as a comma‑separated **“schedule on assets”** text field in the DAG tab, like `tags`). When non‑empty it drives an asset‑based `schedule`. Two more DAG fields control the shape (PRD §6.9, shipped ✅ 2026‑06‑23):
  - **`schedule_asset_mode: 'all' | 'any'`** (default `all`) — `all` runs when **every** listed asset updates (a bare `schedule=[Asset(...)]` list, which Airflow coerces to `AssetAll`); `any` runs when **any one** updates (`schedule=AssetAny(Asset(...))`).
  - **`schedule_with_time: boolean`** (default false) — when set **and** there is a combinable cron `schedule`, the two are **merged** into `schedule=AssetOrTimeSchedule(timetable=CronTriggerTimetable(cron, timezone='UTC'), assets=AssetAll/AssetAny(...))` (run on the time schedule **or** when the assets update). Otherwise the asset condition **overrides** the time schedule. `@once` / `@continuous` / no‑schedule are not cron, so they can't be combined → the assets override (guarded; `@daily`‑style presets DO combine — Airflow normalizes them via `cron_presets`). Empty `schedule_assets` → the existing time `schedule` is unchanged.
- **Produce / consume assets (producer side + lineage)** — per‑task `node.outlets: string[]` and `node.inlets: string[]` (a nested **“Assets (data‑aware scheduling)”** fieldset in the NODE tab, each a comma‑separated text field). On success a task marks its `outlets` updated, triggering any DAG scheduled on them; `inlets` declare consumption (lineage — also read by the §6.2.2 OpenMetadata operator). Codegen merges the rendered `outlets`/`inlets=[Asset(...)]` into the task’s **trailing‑kwargs slot** (the same mechanism per‑task callbacks use), so every operator template emits them with **no per‑operator edit** — into the `@task(…)` decorator for native ops, the operator call otherwise, in **both** TaskFlow & Traditional.

**Asset modelling:** each entry is a single string — an asset **name** or a **URI** — emitted as `Asset('<string>')` (verified against `apache‑airflow‑task‑sdk 1.2.2`: `Asset(name=None, uri=None, …)` requires at least one, and a lone positional sets both `name` and `uri`, the canonical Airflow‑3 form for either a plain name or a URI). `from airflow.sdk import Asset` (and `AssetAny`/`AssetAll`, plus `AssetOrTimeSchedule` from `airflow.timetables.assets` + `CronTriggerTimetable` from `airflow.timetables.trigger` for the combined schedule) is collected **only when** the DAG actually uses it — a DAG with no assets is byte‑unchanged (no stray import). All class/import paths verified against the **exact 3.0.2 target** (airflow‑core 3.0.2 + task‑sdk 1.2.2). Remaining advanced surface — `AssetAlias`, asset **watchers**, `group`/`extra`, the `@asset` decorator, and nested boolean conditions (`AssetAny`/`AssetAll` mixing beyond the flat any/all toggle) — is deferred 🔭 (addable with no IR change). Wireframes **§15.1** (DAG tab schedule‑on‑assets + match‑mode/combine fields) + **§15.3** (NODE‑tab Assets section).

---

## 7. UX / UI specification

- **Layout & theming.** Match the reference UI shape (top bar / palette / canvas / inspector). Style **exclusively with JupyterLab CSS variables** (`--jp-layout-color*`, `--jp-ui-font-color*`, `--jp-border-color*`, `--jp-brand-color1`, `--jp-error-color1`, `--jp-success-color1`); map ReactFlow's CSS vars onto `--jp-*` so dark mode reskins automatically.
- **Reclaimable canvas.** The left palette and right inspector each **collapse to a rail and re‑expand** (§6.1.6) via a header chevron; the canvas grows to fill the freed width and ReactFlow re‑fits. A collapsed panel still exposes its **expand** control (keyboard‑reachable, so the user is never trapped and the palette's add‑node path stays one click away). Transitions are quick (≤150 ms) and the toggle has an ARIA label + state.
- **First‑run onboarding ✅ (§15.2).** Beyond "Drop operators here," a dismissible **3‑step coachmark** (add task → configure → deploy) guides a first‑time user; it advances from graph/deploy state and shows once per browser (`localStorage`). A new `.afdag` already seeds a sensible DAG config (`createEmptyIR`: `@daily`, `catchup=false`, a `studio` tag). *(A scripted template‑DAG seed is a possible follow‑up 🔭.)*
- **Learning & contextual help (the "teach Airflow" goal).** Studio is also a way to *learn* Airflow: every **NODE and DAG** field shows a plain‑language explanation via an `ⓘ` bubble revealing the help on hover or focus (§6.1.3) ✅, and the **INFO** tab explains the selected operator (purpose, when to use it, required inputs, provider/version, docs deep link) and, with nothing selected, core DAG concepts (schedule/`start_date`/`catchup`/retries). Help text avoids jargon, never blocks the form, and is non‑color‑only (an `(i)` glyph + text). All such copy goes through `trans.__()` (raw Airflow errors and generated code are **not** localized).
- **Deploy feedback.** A persistent tri‑state indicator (Writing / Waiting / Registered‑Failed‑Processing) with timeout copy; never a silent success.
- **Failure recovery (make‑or‑break) ✅ (§15.6 / §15.8).** On import error: pull `stack_trace`, translate it to a **plain‑language card** ("A provider package isn't installed…" / "There is a syntax error in your code…" / "Your DAG couldn't be loaded — …") classified by `src/importErrors.ts` (`explainImportError`) — missing‑provider (with the `pip install apache‑airflow‑providers‑…` line derived from the failed module), Airflow‑2 import path, unresolved import, syntax/indentation, undefined‑name, else the raw exception line — with a *Show technical details* expander always kept. In the **editor** it also **maps back to the offending task** (a `task_id` appearing in the traceback → "⚠ Check the **<task>** task"). In the **manager** import‑errors panel each error gets a **one‑click "Open in Studio to fix"** that resolves the deployed `.py` → its source `.afdag` (server `dags/source` via the `afdag_id` provenance ↔ Contents‑root join) and opens it (not the broken `.py`); when the source is gone/pre‑provenance it says so. **Undeploy / rollback to the previous deployed version ✅** — the deploy banner's *Undeploy* (remove `.py` + history) and *↩ Roll back to previous* (restore the `.bak` saved on the last overwrite‑deploy; the prior *deployed* file, which itself re‑imports through the lifecycle and may still need fixing) (§6.5.5 / §15.6).
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
- **CODE‑tab editor & Python syntax highlighting + line numbers (§6.1.3 CODE / §15.4).** Render the generated Python in the shared **`CodeMirrorField`** (`language="python"`, `readOnly`) instead of `<pre><code>` — the field already wires the Python grammar (`@codemirror/lang-python`) and a **line‑number gutter** (`lineNumbers()`), so the gutter is free; the editor is also selectable and scrollable. **Gap to close:** `CodeMirrorField` configures the *language* + gutter but **no CodeMirror highlight *style***, so tokens currently render uncolored — add `syntaxHighlighting(...)` **once, in `CodeMirrorField`**, which colorizes **both** the CODE preview **and** the `code`/`json` node fields in one change. Prefer a **`--jp‑*`‑aware** style so colors track light/dark and match JupyterLab: either `@jupyterlab/codemirror`'s theme/highlight registry (already a dep), or a small `HighlightStyle.define([...])` mapping CodeMirror highlight tags (`tags.keyword/string/comment/number/definition/operator/typeName…`) to `var(--jp-mirror-editor-*-color)`; **fall back** to `@codemirror/language`'s `defaultHighlightStyle` via `syntaxHighlighting(defaultHighlightStyle, { fallback: true })` if the JL registry is too heavy. Keep it strictly read‑only (`EditorState.readOnly` + `EditorView.editable.of(false)`, already supported via the `readOnly` prop), hide the caret, and let the editor scroll inside the tab (CSS on `.jp-afdag-cm`; the old `.jp-afdag-code-pre` rule is then dead). **All deps are already in `package.json`** (`@codemirror/lang-python`, `@codemirror/view`, `@codemirror/state`, `@jupyterlab/codemirror`) — no new install. Test: the CODE tab mounts a `.cm-editor` with a `.cm-gutters` line‑number gutter (not a `<pre>`), and the editor is non‑editable.
- **Layout:** `@dagrejs/dagre` (maintained fork) for one‑click "Tidy layout" ✅ — `src/layout.ts` `tidyLayout(nodes, edges)` builds a dagre graph (`rankdir: TB`) from the **task** nodes (note cards excluded) using each node's measured size, lays it out, and returns id → top‑left positions; the top‑bar `≣ Tidy` button applies them via `setNodes`, the persist effect saves them, and the view re‑fits. elkjs behind a flag for dense graphs (future).
- **New deps:** `@xyflow/react`, `@rjsf/core`, `@rjsf/validator-ajv8`, `@dagrejs/dagre`, plus `@jupyterlab/docregistry`, `@jupyterlab/docmanager`, `@jupyterlab/launcher`, `@jupyterlab/filebrowser`, `@jupyterlab/codemirror`.

### 8.3 The `.afdag` document & IR schema

Versioned IR JSON: `{ schema_version, provenance, syntax_style, dag, nodes[], edges[], notes?[], layout? }` — where `dag` also carries an optional `callbacks` block (§6.8) and an optional `schedule_assets: string[]` for data‑aware scheduling (§6.9), and a `node` carries optional `common`, `callbacks`, and `inlets`/`outlets: string[]` (§6.9). All are absent on older `.afdag` files (back‑compatible). `node.id` is the stable ReactFlow id; `task_id` is the Airflow id (validated identifier, unique). `op` references a registry id (keeps IR decoupled from operator impl). `position` lives in the IR so layout round‑trips. `provenance` (`afdag_id`, `studio_version`, `ir-hash`) is **also embedded in the generated `.py`** so the manager can tell Studio‑created (editable) from hand‑written (read‑only) DAGs and detect drift. See **Appendix B**.

**Annotation notes (§6.1.7)** live in an **optional, separate `notes[]` array** — `{ id, text, position, size? }` — deliberately **outside `nodes[]`/`edges[]`** so the executable task graph that codegen and validation read (`ir["nodes"]`/`ir["edges"]`) is unaffected and note cards can never become tasks, edges, or cycle/required‑field errors. The IR/flow mapping merges `notes[]` into ReactFlow `nodes` as `type:'noteNode'` and splits them back out on persist. `notes[]` is absent on older `.afdag` files (back‑compatible: default to `[]`).

**Notification callbacks (§6.8) — DAG‑level + per‑task ✅:** an optional **`callbacks`** block keyed by event with a list of `{ notifier_id, params }`, on **`dag.callbacks`** (`on_success`/`on_failure`) and on each **`nodes[].callbacks`** (`on_success`/`on_failure`/`on_retry`). `sla_miss` is excluded (SLAs were removed in Airflow 3.0). Kept outside `edges[]` so cycle‑check is untouched — the same isolation as `notes[]`. Codegen renders each notifier from its registry `template`, appending DAG‑level callbacks to the `@dag`/`with DAG(…)` call and merging per‑task callbacks into the task's trailing kwargs; absent on older files.

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

Reuse the existing `_AirflowHandler.respond` + `run_in_executor` pattern and `url_path_join(base_url, 'jupyterlab-airflow', act)`. **Existing:** `health`, `dags`, `dags/pause`, `dags/trigger`, `dagruns`. **Add:** `operators` (registry), `generate` (IR→validated code preview), `validate`, `deploy` (validate→format→atomic write→verify→**unpause+trigger**, §6.5.4), `dags/details`, `dags/source`, `dags/delete`, `dags/orphans` (orphan‑reconciliation sweep, §6.5.6), `dagruns/state` (**stop a run** → `PATCH …/dagRuns/{run_id} state:"failed"`, §6.6), `dagruns/clear`, `taskinstances`, `taskinstances/logs`, `taskinstances/state`, `taskinstances/clear`, `importerrors`, `assets/events`. New `AirflowClient` methods needed: `set_dag_run_state` (stop) and `get_dag_run` (single‑run state for the run‑on‑deploy/stop banners); `trigger_dag`/`set_paused`/`delete_dag` already exist. Extend `AirflowClient` with one method per endpoint group (Appendix D). **Fix** `list_dags` v2 param drift.

### 8.7 `DeployTarget` abstraction

Interface in §6.5.1. `SharedVolumeTarget` reads its dags path from an env var (e.g. `AIRFLOW_DAGS_DIR`, default the mounted `/opt/airflow/dags`). Owns **namespacing** (so Git/S3 reuse it) and the atomic write. **Git target ✅ (`GitDeployTarget`, 2026‑06‑23):** subclasses `SharedVolumeTarget` with `root = <AIRFLOW_GIT_DAGS_REPO>/<AIRFLOW_GIT_DAGS_SUBDIR>` (default subdir `dags`); `write`/`delete`/`restore_backup` do the inherited file op then `git add` the affected `.py` (+ `.airflowignore`) and `commit` (with a fixed `airflow-studio` identity, so it doesn't depend on the server's git config), then `push` to `AIRFLOW_GIT_DAGS_REMOTE`/`AIRFLOW_GIT_DAGS_BRANCH` if a remote is set — mapping to an Airflow `GitDagBundle`. `_ensure_root` validates the repo (`git rev-parse --is-inside-work-tree`); empty commits are skipped (`diff --cached --quiet`); git calls are 60 s‑timeout‑bounded. **S3 target ✅ (`S3DeployTarget`, 2026‑06‑23):** a standalone `DeployTarget` (sibling of `SharedVolumeTarget` under the new thin `DeployTarget` base) that maps the interface onto S3 objects — `write` = `put_object` (collision‑guarded; the prior managed object is copied to a `…​.py.bak` object first), `read`/`exists` = `get_object`/`head_object` (404 → not‑found), `delete` = `delete_object` (+ the backup), `list` = paginated `ListObjectsV2` filtered to managed `.py` objects directly under the prefix, `ensure_airflowignore` = get‑modify‑put on the `.airflowignore` object. Reuses `_FILENAME_RE`/`_parse_header`/`MANAGED_PREFIX`. boto3 is imported lazily + the client is injectable; the S3 API shapes were verified against the botocore service model. The consistency flag drives the verification‑poll timeout (git **and** S3 are `"eventual"`, like the shared volume — Airflow's bundle discovery is delayed regardless of the store's own consistency).

### 8.8 Airflow 3.x integration specifics

- REST `/api/v2` (FastAPI), JWT via `POST /auth/token` → Bearer (already implemented). `execution_date` is gone → `logical_date` (nullable for now‑runs). Pause = `PATCH /dags/{id}?update_mask=is_paused`. Trigger = `POST /dags/{id}/dagRuns {logical_date?, conf}`.
- **Run‑on‑deploy ordering (§6.5.4).** Triggering a run on a **paused** dag returns 200/201 and the run appears, but the scheduler holds it in `queued` (it filters `~DagModel.is_paused`) and **no task starts until the dag is unpaused**. So the deploy auto‑run must **unpause first, then trigger** — there is no single "trigger + unpause" call (the native UI does the two steps too).
- **Stop a run (§6.6).** No `cancel`/`terminate` endpoint exists for a normal DagRun — set the run's state: `PATCH /dags/{id}/dagRuns/{run_id} {state:"failed"}` (allowed states: `queued|success|failed`); running task instances are then terminated by the scheduler. (`POST …/dagRuns/{run_id}/clear` re‑runs tasks; it is **not** a stop.) Only `/api/v2/backfills/{id}/cancel` is a real cancel, and only for backfills.
- **Delete preconditions (§6.5.6 / §6.6).** `DELETE /dags/{id}` **refuses when any task instance is RUNNING** (`delete_dag()` raises `AirflowException("TaskInstances still running")`) and removes **metadata/history only** — if the `.py` is still on disk the dag re‑parses and reappears, so the DeployTarget must remove the file **first** (the existing `purge_dag` order is correct). The dag does **not** need to be paused to delete.
- Default DAG bundle `dags-folder` = `LocalDagBundle` over `[core] dags_folder` — the shared‑volume model needs **no bundle reconfiguration**. `LocalDagBundle` has **no versioning** (always runs latest on disk) → don't edit a deployed file during an active run.
- `.airflowignore` default syntax is **glob** in Airflow 3 (was regexp).
- **Discovery latency is real:** `dag_dir_list_interval` (~300s) for new files, `min_file_process_interval` (~30s) for changed ones; no on‑demand refresh API → §6.5.4 polling is mandatory.

### 8.9 File layout, naming, namespacing, provenance

- **One DAG per file.** Deterministic, sanitized filename. **Namespace per user** in shared deployments: `users/{username}/{slug}.py`, `dag_id = f"{username}__{slug}"`, DAG `owner = username`. Path‑traversal safe (reject `..`, absolute paths, symlinks).
- `.afdag` source of truth lives in the **Jupyter workspace** (Contents‑API reachable for SAVED/reopen); the `.py` is deployed to the shared volume. Re‑associate via the embedded `afdag_id`/`ir-hash`.
- Provenance header in the `.py` (managed flag, `studio_version`, `ir-hash`, **`code` body hash**, `dag_id`, `afdag_id`, syntax mode, **`correlation_id` ✅**) → distinguishes editable vs read‑only and **detects out‑of‑band edits**: the `code=sha256` body hash is stamped at write time and compared to the on‑disk body at the deploy preflight (§6.5.3 / §6.5.5). The **`correlation_id` ✅ (2026‑06‑24)** is a per‑deploy id `deploy_dag` stamps into the header (and returns) — the **same** id the deploy's audit record carries (§9) — so a deployed DAG (and a later import error on its filename → header) traces back to the deploy session (§10). It's stamped at *deploy* time (not in `generate_dag`, which stays deterministic) and only touches the header line, so the body hash / `ir-hash` / drift detection are unaffected.
- **Rename / identity (§6.1.8).** The deploy artifact's filename is `{dag_id}.py` (`deploy.py`), so changing `dag_id` **relocates** it — a `dag_id` rename is *write‑new + remove‑old + reconcile*, never an in‑place edit, and (Airflow having no rename) it starts fresh history under the new id. The durable, **rename‑surviving** identity is **`afdag_id`**, which therefore **must be added to the `.py` provenance header** (today `codegen.py` emits `dag_id`/`ir_hash`/`syntax` only) so both document‑ and `dag_id`‑renames stay re‑associable to their `.afdag`. The `.afdag` filename is itself decoupled from `dag_id` (seeded by `dagIdFromPath` only at creation), so a *document* rename has **no** Airflow effect. When per‑user namespacing (`{username}__{slug}`) lands, the `dag_id`↔filename coupling — and this migration logic — are unchanged.

---

## 9. Security, multi‑user & governance

- **Deploy is privileged.** Writing a `.py` into the dags folder == running code as the Airflow worker (with its connections/secrets/cloud creds). Treat the `deploy` endpoint as a privileged operation, **not** a default‑on capability for every Jupyter user. Document who may deploy.
- **Codegen is a security‑critical compiler.** Safe literal emission only (§8.4); Bash/HTTP/env values escaped, never shell/path‑concatenated; the `.afdag` is **untrusted adversarial JSON** — schema‑validate and re‑run checks server‑side and bound sizes.
- **Code nodes** = arbitrary code; lint + isolated‑subprocess validation; document the blast radius; (later) optional review/approval gate or separate worker queue.
- **Multi‑user reality.** Today the server uses **one shared service account** (process‑wide env creds, one module‑global cached JWT). On JupyterHub each user gets their own server process, so for real per‑user attribution/authorization, inject **per‑user Airflow creds/OIDC** at spawn (`c.Spawner.environment`/`auth_state`); keep env‑var creds as a single‑user/dev fallback. **Document prominently** that, until then, any Jupyter user acts as one Airflow admin and the shared dags folder is a shared trust boundary (Airflow's multi‑team isolation is experimental and does not isolate task execution/secrets).
- **Collision protection** (§6.5.3): pre‑write uniqueness/ownership check; refuse to overwrite non‑Studio files; duplicate‑`dag_id` handling; "modified outside Studio" flow.
- **Destructive lifecycle is confirmed, never automatic‑silent.** Run‑on‑deploy goes live (every deploy unpauses + triggers, §6.5.4) and design‑file‑delete purges the DAG **and its run history** (§6.5.6) — both are irreversible against shared Airflow state. A vanished `.afdag` is a *weak* intent signal (it can disappear via `git`/`rm`), so the reconciliation sweep **flags** orphans and **requires explicit per‑DAG confirmation** before `DELETE /dags/{id}`; it never purges on detection alone. Only the **`afdag_id`‑provenance‑matched, Studio‑managed** `.py` files are eligible — hand‑written DAGs (no provenance header) are never auto‑touched, per §6.5.3.
- **Secrets guidance.** Steer users to **Airflow Connections/Variables** instead of pasting API keys/passwords into env‑var/HTTP/code fields (which would be written in plaintext into the dags folder and `.afdag`). Warn on `AIRFLOW_VERIFY_SSL=false` for any non‑local target (MITM of JWT).
- **Token lifecycle.** The single cached JWT refreshed once on 401 is fragile under rotation/clock skew; make it per‑process and, with Hub‑injected tokens, refresh from the Hub/auth_state rather than re‑POSTing static creds.
- **Audit ✅ (2026‑06‑23).** Every mutating action — **deploy · trigger · pause · unpause · stop‑run · clear · delete · rollback · retire** — emits a structured `{ts, user, action, dag_id, correlation_id, outcome, detail?}` JSON line on the dedicated `jupyterlab_airflow.audit` logger (`audit.py`), stamped with the authenticated Jupyter user (resolved from `self.current_user`; the Hub user under JupyterHub). Wired once into the base handler's `respond(…, audit_action=, audit_dag_id=)` so each mutating handler opts in and read‑only reads stay un‑audited; a dry‑run clear (preview) isn't audited. Records are JSON‑serialized (no log injection) and log the *action only*, never the request body (so a trigger `conf` with secrets isn't recorded); `detail` carries a trimmed error message on failure. Routable to a file/SIEM via normal logging config. *(This landed before full per‑user creds injection, exactly as planned — attribution is correct per server process; see the multi‑user note above.)*

## 10. Testing & QA strategy

- **Golden‑file tests:** IR → expected `.py` for **every operator** and **every escaping edge case** (quotes, newlines, unicode, backslashes, dict/JSON params, reserved/duplicate `task_id`s, identifier sanitization).
- **Round‑trip property test:** IR → `.py` → reopen `.afdag` → identical IR.
- **Toggle equivalence ✅:** Traditional and TaskFlow output for the same IR yield the same task graph (tasks + `>>` dependency edges) — asserted by `test_codegen.py::test_taskflow_and_traditional_yield_the_same_task_graph` (§6.3). *(Graph equivalence; a context‑dependent code‑node body still differs between families — see the §6.3 caveat.)*
- **Real‑Airflow integration:** parse generated DAGs in the pinned `apache/airflow:3.0.2` image; assert **zero import errors** and a **successful run** — not just `compile()`.
- **REST contract tests:** new `/api/v2` endpoints (importErrors, taskInstances, logs, clear/retry, delete) — shapes differ from `/api/v1`.
- **Concurrency:** two simultaneous deploys to the shared folder; collision/overwrite behavior.
- **Security:** injection attempts via params/code nodes; path‑traversal filenames; oversized/adversarial `.afdag`.
- **Frontend:** validation (cycle/required) unit tests; a11y (keyboard path, ARIA) checks; existing jest setup extended.
- **Env fix to verify:** bump `requires-python` to ≥ 3.9 (Airflow 3 needs 3.9+); current `>=3.8` is inconsistent if the validator imports airflow.
- **v1.3 features (§5):** each **new operator** ships with registry tests (ids/providers present, no Airflow‑2 paths, category + sensor `common_params`, `client_view` shape) + codegen tests (renders as the right kind, optional kwargs emit only when set) + a live deploy on `3.0.2`; **log‑viewer** parsing (structured‑event normalization, level classification, attempt fetch, error/loading state) unit‑tested; **`ⓘ` bubble** a11y checked (focus + `Esc`, `aria-describedby`); **notifier codegen** golden‑files for `on_*_callback` wiring (DAG‑ and task‑level), with `callbacks`/`notes[]` round‑trip property tests.

## 11. Observability & telemetry

Structured per‑request server logs `{user, action, dag_id, airflow_status, latency_ms, correlation_id}`; counters `deploy_success` / `deploy_parse_error` / `trigger` / `clear` / `log_fetch` + latency histograms for Airflow round‑trips; a correlation id shared between the `.py` provenance and logs **✅ (2026‑06‑24)** — a per‑deploy `correlation_id` is stamped in the deployed `.py` header **and** the deploy's audit record (§8.9 / §9), so a failed import traces back to its Studio deploy session; a diagnostics view backed by `health`. Optionally forward to OpenTelemetry to correlate with Airflow's own OTel traces.

## 12. Risks, assumptions & mitigations

| # | Risk / assumption | Mitigation |
|---|---|---|
| R1 | **Deploy ≠ appears ≠ runs**; latency + no on‑demand refresh API | Tri‑state polled lifecycle (§6.5.4); honest timeout copy |
| R2 | Server parse‑check is **false‑green** (Jupyter env ≠ Airflow env, missing providers) | Authoritative verdict from `/importErrors`; validate with the worker image/venv; registry records provider deps |
| R3 | **Codegen injection / broken Python** into an executed folder | Safe literal emission, `autoescape=False`, golden + security tests (§8.4, §10) |
| R4 | **Shared‑folder collisions** (duplicate `dag_id`, clobbering) | Namespacing + pre‑write ownership check + provenance refuse‑overwrite (§8.9, §9) |
| R5 | **Round‑trip drift** (`.py` hand‑edited; `.afdag`/`.py` two sources) | `ir-hash` checksum; "modified outside Studio" reopen flow |
| R6 | **Single shared admin** → no attribution/authz; fragile cached JWT | Hub‑injected per‑user creds (v1.2); audit now; per‑process token |
| R7 | **Toggle** = two backends that can silently diverge | **Shipped ✅** with a task‑graph **equivalence test** (same IR → same tasks + `>>` edges in both families, §6.3/§10); residual: a code‑node body's Airflow‑context contract differs between families (caveated, §6.3) |
| R8 | **Code node = RCE** on shared workers | Isolated‑subprocess validation; deploy is privileged; document; (later) sandbox/queue |
| R9 | **Scope creep** (sensors, Git/S3, dual backend) | Phased plan §5; keep only the `DeployTarget` interface in v1 |
| R10 | **Prod may not have a writable shared volume** | `DeployTarget` is load‑bearing from day one, not "later" |
| R11 | **Rename mid‑run / orphaned `dag_id` history** — Airflow has no rename; `{dag_id}.py` relocates and the old DAG is orphaned; removing the old file during an active run strands it | Deploy‑aware rename migration (§6.1.8): block while a run is active; write‑new‑then‑remove‑old; keep‑history default (purge is opt‑in); `afdag_id` in the provenance header for cross‑rename re‑association |
| R12 | **Re‑deploy overwrites a *running* DAG's file** — `LocalDagBundle` runs latest‑on‑disk, so an in‑place update mid‑run can corrupt the active run | Active‑run guard before re‑deploy (§6.5.5): the shared dag‑state preflight blocks with *Cancel* / explicit *Deploy anyway* — the same check as the rename migration |
| R13 | **Auto‑undeploy purges history on a `.afdag` that vanished unintentionally** (git checkout, `rm`, branch switch) | Reconciliation **flags** orphans; purge is **confirmed per‑DAG**, never silent (§6.5.6/§9); only provenance‑matched managed files are eligible; the in‑session signal prompts and the sweep surfaces a banner — both gated |
| R14 | **Run‑on‑deploy goes live every time** — an unfinished/just‑edited DAG could run unintentionally, or backfill on a past `start_date`+`catchup` | The §6.5.5 active‑run guard still blocks a re‑deploy over an in‑flight run; the banner shows *Running* with **Stop run**; a "deploy paused" kill‑switch + catchup‑aware skip are open follow‑ups (§13) |

## 13. Open questions / decisions needed

1. **Where does the parse‑check run?** Jupyter and Airflow are separate containers; the Jupyter ext can't `import airflow` to DagBag‑check. Options: (a) `py_compile` in Jupyter + rely on post‑deploy `/importErrors`; (b) exec/`reserialize` in the Airflow container; (c) ship a thin matching airflow venv in the Jupyter image for validation. **Recommendation:** (a) for MVP + always poll `/importErrors`; pursue (c) for fidelity.
2. **Pin the Airflow + providers versions** for the devcontainer and validator; confirm `airflow.dag_processing.dagbag` path and standard‑provider module names on the pinned `3.0.2` image.
3. **JupyterLab minor target** (repo pins `^4.1.6`); `IContentProvider`/`contentProviderId` need 4.4+ (not required for v1).
4. **JupyterHub credential injection mechanism** (shared OIDC IdP vs per‑user `auth_state`) — sets the token‑refresh path and whether `/auth/token` is used per user.
5. **`/importErrors` server‑side filename filtering** — confirm against the running instance's OpenAPI, else fetch + match client‑side.
6. **Branch/ShortCircuit multi‑output modeling** in the IR/edges (labeled edges vs multiple source handles) and its render to `BranchPythonOperator` follow‑paths.
7. **Code node in Traditional mode** — wrap as `PythonOperator(python_callable=...)` vs force TaskFlow.
8. **Validation subprocess sandbox policy** (CPU/mem/wall‑time, network egress) — concrete since code nodes are arbitrary by design.
9. **Rename of a deployed `dag_id` — old‑history default** (§6.1.8): default to *keep* the old history (pause + remove file → dag goes `stale`) vs *purge* (`DELETE /dags/{old}`)? And should a rename also be triggerable from the **manager**, not only the editor?
10. **Run‑on‑deploy escape hatch (§6.5.4).** Every deploy now unpauses + triggers (decision 2026‑06‑17). Do we also need a "deploy paused / don't run" affordance for a DAG the user wants live but not yet run — and should run‑on‑deploy be **skipped** when the freshly registered DAG would **backfill** (past `start_date` + `catchup=true`) rather than fire a single now‑run?
11. **Orphan‑sweep cadence & scope (§6.5.6).** Run the reconciliation sweep only on manager refresh, or also on a timer / on editor close? Bound the Contents‑root walk (skip huge/irrelevant trees), and in multi‑user act only on **namespaced/owned** DAGs.
12. **Stop‑run semantics (§6.6).** `PATCH state:"failed"` marks the run failed (not a graceful cancel); confirm this is the desired "stop", and whether to also offer `state:"success"` (force‑complete) — Airflow allows both. Should stopping a run be available for `queued` runs too (not just `running`)?
13. ~~**Third‑party operator gating (§6.2.2 ¹).**~~ **Resolved 2026‑06‑23 (option B).** There is no clean REST probe for arbitrary package importability in the *target* (an "importable?" check would also re‑introduce the R2 false‑green if run in the Jupyter env), so third‑party ops are flagged `third_party: true` (+ own `version`) and given a distinct **`third‑party`** availability state: shown in the palette/INFO with a pinned‑install note, **never deploy‑gate‑blocked**, with `/importErrors` (+ the §7 friendly recovery) as the verdict. `provider_block_errors` skips them; `availability(..., third_party=True)` always returns `third‑party` (independent of the index). This generalizes to twilio‑like SDKs and sidesteps OpenMetadata's un‑checkable server‑version match — both shipped P3 packages happen to register as providers, but the design deliberately does not rely on that.
14. **SQL connection‑type picker (§6.2.2).** Surface a connection‑type/`conn_id` picker (Trino/Postgres/MySQL/MSSQL) on the existing SQL node — documenting that the matching provider package must be installed — rather than adding per‑DB operator YAMLs (deprecated in Airflow 3)?
15. ~~**Notifier callback modeling (§6.8).**~~ **Resolved 2026‑06‑22:** DAG‑level callbacks ship on **`ir.dag.callbacks`** and per‑task callbacks on **`ir.nodes[].callbacks`** (the `notes[]`‑style isolation) via a **separate notifier registry** (`notifiers/*.yaml`, not shared with operators); the task‑only **`on_retry`** event is included, and the deploy‑time provider hard‑gate scans both scopes. Remaining 📝: the niche `on_execute`/`on_skipped` task events.
16. ~~**Log viewer & structured events (§6.6).**~~ **Resolved 2026‑06‑24 (structured pass‑through shipped).** `client.get_task_logs` passes Airflow 3's structured events through as `events?: {event, timestamp?, level?, logger?}[]` (widening `ITaskLogsRes` minimally — verified against `apache‑airflow‑core 3.0.2` `StructuredLogMessage`), the viewer colours by the server `level` (text `classifyLine` is the per‑event + plain‑text fallback), and the flattened `content` stays for Copy/Download (back‑compatible). **Live ndjson tailing: not worth the polling complexity — deferred 🔭.**

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
| **M6 — Recovery UX + a11y ✅** | Friendly import‑error → node mapping (`explainImportError`) + "Open in Studio to fix" (manager → `dags/source` → open `.afdag`) + undeploy/rollback; keyboard path + non‑color‑only indicators |
| **M7 — Lifecycle automation** | **Run on deploy:** after register, the server unpauses + triggers a run and the banner reaches *Running* (integration test on `3.0.2` asserts a green run, no manual step); **Stop run** (manager + editor) `PATCH`es a run to `failed` and its tasks terminate; **orphan reconciliation:** deleting a `.afdag` — via the in‑session `fileChanged` signal **and** the server sweep (terminal/`git`/`rm` deletes) — flags, confirms, then removes the `.py` and `DELETE`s the DAG; delete is blocked while a task runs; all three are audited (`{user, action, dag_id, correlation_id}`) |
| **v1.1** | Traditional backend + working toggle (task‑graph equivalence test) ✅; Tidy layout (dagre) ✅; more operators ✅ (catalogue → 18) |
| **v1.2** | **Advanced KubernetesPodOperator surface ✅** (node_selector/labels/annotations/service_account_name/priority_class_name/security_context + pod_template_file/pod_template_dict declarative escape hatch for volumes/secrets/affinity/resources); **Asset / data‑aware scheduling ✅** (`dag.schedule_assets` → `schedule=[Asset(...)]` + per‑task `outlets`/`inlets`, §6.9); **Git + S3 `DeployTarget` ✅** (`GitDeployTarget` commits/pushes to a `GitDagBundle` repo; `S3DeployTarget` puts objects for an S3 bundle — factory‑selected by `AIRFLOW_DEPLOY_TARGET`); **Studio action audit trail ✅** (`audit.py`: every mutating action → `{ts, user, action, dag_id, correlation_id, outcome}` on the `jupyterlab_airflow.audit` logger). Per‑user Airflow creds = Hub‑spawn deployment config + documented trust model (§9), not in‑extension RBAC (NG4) |
| **v1.3** | **P0 + P1 + P2 + third‑party P3 lakehouse ops ✅** (26 ops, catalogue → 44, wheel‑verified; GX + OpenMetadata ride the un‑gated `third-party` state, §13 Q13; **live deploy on `3.0.2` is the remaining gate**); **friendly log viewer ✅** (level colour, attempt selector, search, Copy/Download/Wrap, autoscroll‑to‑first‑error); **`ⓘ` field bubbles ✅** on DAG + NODE (hover/focus/click, `Esc`/blur to dismiss); **Notifications (DAG‑level + per‑task) ✅** — NOTIFY tab + NODE‑tab Notifications section + `dag.callbacks`/`node.callbacks` IR + notifier registry (Smtp/Slack/Apprise/Discord/Opsgenie) + `on_*_callback` codegen incl. task‑only `on_retry` (§6.8) |

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
│        [ Traditional │▣TaskFlow ]  ≣ Tidy  ↶ ↷  Reset  Save  ⚙ Generate DAG  ▶ Deploy │
├──── OPERATORS ───«─┬─────────────── CANVAS ───────────────┬─»── INSPECTOR ───────┤
│ 🔍 Search…         │                                      │ [DAG] NODE INFO CODE SAVED │
│ ▾ PYTHON / BASH    │        ┌────────────────────┐        │ ─────────────────────│
│   Bash operator    │        │ PYTHON_BASH        │        │ DAG CONFIGURATION    │
│   Branch operator  │        │ ▷ Bash operator  ✕ │ ● green│ DAG ID    [ my_dag ] │
│   Python operator  │        │ task_id: print1    │        │ DESCRIPTION [      ] │
│   Custom @task     │        └─────────┬──────────┘        │ SCHEDULE ⓘ[ @daily ▾]│
│ ▾ FLOW CONTROL     │                  ▼  (rounded         │ ON ASSETS ⓘ[ orders ]│
│   Empty operator   │                                      │ START DATE[01/01/2024]│
│   Short-circuit op │        ┌────────────────────┐  arrow)│ OWNER     [ data-team]│
│   Trigger DAG run  │        │ ▷ Bash operator    │ ● green│ RETRIES[1] RTY-DLY[5]│
│ ▾ HTTP·SQL (P1 ✅)  │        │ task_id: print2    │        │ TAGS  [ etl, prod  ] │
│   HTTP req · SQL    │        └─────────┬──────────┘        │ PARAMS  { }          │
│ ▾ SENSORS (P0 ✅)   │                  ▼                   │ CATCHUP ⓘ ◯ off      │
│   File·Ext·SQL sens │        ┌────────────────────┐        │ ASSET MATCH[ all ▾]  │
│ ＋ Add note        │        │  … print3 / print4 │ ● green│ ALSO ON TIME ◯ off   │
│                    │        └────────────────────┘ ┌────┐ │                      │
│                    │     ⊕ ⊖ ⤢ (zoom/fit)         │mmap│ │                      │
└────────────────────┴──────────────────────────────┴────┴─┴──────────────────────┘
```
Built: palette (search/categories/drag) · rounded‑corner arrow edges · minimap/zoom · DAG form (id/description/schedule/start_date/owner/retries/retry_delay/tags/params/catchup) · live `✓ no errors` badge · Reset/Save/Generate/Deploy · **`≣ Tidy` ✅** — one‑click auto‑layout (dagre top‑to‑bottom layered layout) that re‑positions the task nodes, persists the new positions, and re‑fits the view; disabled when the canvas is empty, and leaves free‑floating note cards where they are (§8.2). **TaskFlow + Traditional ✅:** the top‑bar toggle flips the IR's `syntax_style`; codegen renders `@dag`/`@task` (TaskFlow) or `with DAG(…)` + operator instances + `>>` wiring (Traditional) accordingly (§6.3). Palette **catalogue** grows per §6.2.1 / §6.2.2: **P0** shipped — **Flow Control** gains `ShortCircuit` + `LatestOnly`, and a new **Sensors** category lands (`File` · `ExternalTask` · `DateTime` · `TimeDelta`); **P1** shipped — the first gated ops `HTTP` · `SQL query` · `SqlSensor`; **P2** shipped — `KubernetesPodOperator` (Kubernetes), `S3KeySensor`/`GCSObjectExistenceSensor` (Sensors), `BigQueryInsertJobOperator` (Cloud) — all dimmed when their provider is absent (§15.7) ✅. Catalogue → 18. **Lakehouse expansion (§6.2.2): P0 + P1 + P2 + third‑party P3 shipped ✅ (catalogue → 44)** — **Storage** (`S3CreateObject`/`Copy`/`List`/`Delete`), **Ingestion** (`SFTP`/`FTP` transmit, `SFTP→S3`, `IMAP→S3`), **Compute** (`SparkSubmit`/`SparkSql`/`SparkJDBC`/`SparkKubernetes`, `Papermill`), **Data Quality** (`SQLColumnCheck`/`SQLTableCheck`, + the third‑party `GX checkpoint`), **Sensors** (`SFTP`/`FTP`/`IMAP` sensors), **Notifications** (`Email`, `Slack`/`SlackWebhook`, `Discord`, `Telegram`, `Opsgenie`), **Governance** (the third‑party `OpenMetadata lineage`); the two third‑party ops (2026‑06‑23) are code‑first and ride a distinct un‑dimmed `third-party` palette state (§15.7), and the §6.8 notifier callbacks shipped — all gated like the existing tiers. **DAG‑field help ✅ (§6.1.3):** every DAG CONFIGURATION field now shows an `ⓘ` bubble (hover/focus) with its explanation, not just `dag_id`/`tags`. **Data‑aware scheduling ✅ (§6.9):** a **"schedule on assets"** field (comma‑separated asset names/URIs) sits below `schedule`, with a **match mode** select (`all`/`any` → `AssetAll`/`AssetAny`) and an **"also run on time"** checkbox (combine asset + cron → `AssetOrTimeSchedule`); filling assets drives an asset‑based `schedule=`, overriding the time schedule unless combine is on.

### 15.2 Studio editor — empty‑state / onboarding ✅

0 nodes → drop‑zone. *(src: 01-small-demo f0000; the clip also demos the syntax toggle.)*

```
│ … palette …  │   ┌ Getting started · Step 1 of 3   Skip tour ┐   │ DAG CONFIG … │
│              │   │ ● ○ ○                                      │   │ DAG ID [my_dag]│
│              │   │ Add your first task — pick an operator     │   │  …           │
│              │   │ from the Operators palette on the left.    │   │              │
│              │   └────────────────────────────────────────────┘   │              │
│              │            ╭───────────────────────╮            │              │
│              │            │   Drop operators here   │            │              │
│              │            ╰───────────────────────╯            │              │
                 top bar shows “0 nodes”; [Traditional│▣TaskFlow] toggle is the clip’s subject
```
Built ✅: beyond the drop hint, a dismissible **3‑step coachmark** (`Coachmark`) pinned top‑centre of the canvas walks a first‑time user **add task → configure → deploy**. It advances from state (step 1→2 when the first task lands; finishes when a Deploy starts) with *Next*/*Done* + *Skip tour*, and is shown **once per browser** (a `localStorage` flag), so it never nags a returning user (§7).

### 15.3 Studio editor — NODE tab + live validation ✅

Select a node → operator form; required‑field gaps drive the badge + the node's red `●`. *(src: 02-demo-a f0150/f0600)*

```
top bar:  … my_dag · 2 nodes · ✕ 2 errors      ← red while required fields empty

  canvas node (invalid)              INSPECTOR — NODE tab
  ┌────────────────────┐            ┌ DAG [NODE] INFO CODE SAVED ─────────────┐
  │ PYTHON_BASH       ✕│            │ ⚠ 2 errors on this node                 │
  │ ▷ Bash operator    │ ● red      │ BASH OPERATOR        node_173…_6        │
  │ task_id: bash_7    │            │ TASK ID *         [ bash_7            ]  │
  └────────────────────┘            │ BASH COMMAND * ⓘ  [                  ]⛔│ ← red outline; ⓘ = help bubble
                                    │ ENVIRONMENT VARS  [ { }              ]  │
                                    │ ─ COMMON SETTINGS ───────────────────   │ ← per-task; overrides DAG defaults
                                    │ RETRIES           [   ]                 │   (blank = inherit; only set
                                    │ RETRY DELAY (SEC) [   ]                 │    values are emitted; retry_delay
                                    │ DEPENDS ON PAST   ◯ off                 │    → timedelta(seconds=…))
                                    │ (sensors add MODE · POKE INTERVAL · TIMEOUT) │
                                    │ ─ NOTIFICATIONS ─────────────────────   │ ← per-task callbacks (§6.8)
                                    │ On failure — when this task fails       │   on_failure / on_retry / on_success
                                    │   [ Slack ✕ ]  text* [ :red: failed  ]  │   notifiers run as callbacks, not
                                    │   ＋ Add  Email · Slack · …              │   graph tasks → node.callbacks
                                    │ On retry — when about to retry          │   on_retry is the task-only event
                                    │   No notifications.   ＋ Add            │
                                    │ On success — when this task succeeds    │
                                    │   No notifications.   ＋ Add            │
                                    │ ─ ASSETS (data-aware scheduling) ────   │ ← §6.9; comma-sep names/URIs
                                    │ INLETS (consumes) [ s3://lake/raw.csv ] │   → inlets=[Asset(…)] (lineage)
                                    │ OUTLETS (produces)[ curated_orders    ] │   → outlets=[Asset(…)]; updating it
                                    │                                         │   triggers DAGs scheduled on it
                                    │ ─────────────────────────────────────  │
                                    │                       [ 🗑 Delete task ] │
                                    └─────────────────────────────────────────┘
```
Built: registry‑generated form, `validateNodeParams` required‑field check (red outline), top‑bar `✕ N errors` decrementing live, per‑node dots, in‑card ✕ + “Delete task”. New operators just add param YAML — no form code. **Field ⓘ bubbles ✅ (§6.1.3):** each NODE field (and the Common‑settings fields) shows an `ⓘ` revealing its help on hover / focus / click, sourced from the operator YAML `help` — wired once as a custom RJSF `DescriptionFieldTemplate` (the `InfoBubble` primitive) in `AfdagForm`, so the DAG form (§15.1) gets the same treatment. **Common settings ✅:** a nested "Common settings" fieldset (the op's registry `common_params`) edits per‑task `retries`/`retry_delay`/`depends_on_past` (+ sensor `mode`/`poke_interval`/`timeout`) into `node.common`; codegen emits them (overriding the DAG defaults, `retry_delay` → `timedelta`), writing only the values the user set (§6.1.3). **Per‑task notifications ✅ (§6.8, 2026‑06‑22):** a "Notifications" section below Common settings attaches notifiers to this task's `on_failure` / `on_retry` / `on_success` events (`on_retry` is the task‑only event the DAG level can't express) into `node.callbacks`; it reuses the shared `CallbacksEditor` (the same component the DAG‑level NOTIFY tab uses) and codegen merges the rendered `on_*_callback=[…]` into the task's trailing kwargs — into the `@task(…)` decorator for native ops, the operator call otherwise — so every operator template picks it up with no per‑operator edit. **Assets ✅ (§6.9, 2026‑06‑23):** an "Assets (data‑aware scheduling)" fieldset (after Common settings) edits `outlets` (produces) / `inlets` (consumes) as comma‑separated `Asset` names/URIs into `node.outlets`/`node.inlets`; codegen merges them into the same trailing‑kwargs slot (`outlets=[Asset(...)]`), so producing an asset triggers any DAG scheduled on it.

### 15.4 Studio editor — CODE tab (+ cycle‑error variant) ✅ · syntax highlight + line numbers ✅

Live generated‑Python preview + Copy; the cycle path replaces the code until the graph is acyclic. The preview is a **read‑only CodeMirror editor with Python syntax highlighting + a left line‑number gutter** (✅ built — `CodePanel` renders the generated code through `CodeMirrorField language="python" readOnly`; §6.1.3 / §8.2). *(src: 03-demo-b f0500)*

```
 INSPECTOR — CODE tab (valid)                  cycle‑detection variant
 ┌ DAG NODE INFO [CODE] SAVED ─────────────┐   ┌ … [CODE] … ──────────────────────┐
 │ GENERATED CODE              [ ⧉ Copy ]  │   │ ✕ Validation                     │
 │ 1│ # airflow-studio: managed … taskflow │   │ DAG contains a cycle — Airflow   │
 │ 2│ from airflow.sdk import dag, task    │   │ does not support cyclic deps.    │
 │ 3│ @dag(schedule="@daily", …)           │   │ Remove an edge on the path:      │
 │ 4│ def my_dag():                        │   │     print3 → print1              │
 │ 5│     @task.bash(task_id="print1")     │   │ (code preview hidden until the   │
 │ 6│     def print1(): return "echo Hi"   │   │  graph is acyclic)               │
 │ 7│     # --- Dependencies ---           │   │                                  │
 │ 8│     print1 >> print2 >> print3       │   │ [ ⚙ Generate DAG ]               │
 │ [ ⚙ Generate DAG ]          ✓ Valid     │   └──────────────────────────────────┘
 └─────────────────────────────────────────┘    ↑ keyword/str/comment colored;
   └─ line-number gutter; tokens syntax-colored    gutter on the left
```
Built ✅: server codegen (**TaskFlow + Traditional**, selected by the top‑bar toggle / IR `syntax_style`; the CODE‑tab header shows the active family), Copy, validation panel showing client errors **and** post‑deploy Airflow import status. **Syntax highlighting ✅:** the preview is a read‑only `CodeMirrorField` (`language="python"`) with **Python syntax highlighting + a line‑number gutter** — `--jp‑*`‑themed (light/dark), selectable, scrollable (§6.1.3 / §8.2).

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
Built: registry‑driven (`description`/`docs_url`/`example`/per‑param `help`/`provider`/`airflow_min_version`), all rendered as escaped plain text (registry is user‑extensible → no raw HTML). For an op the target Airflow can't run (§6.2.1) the tab now adds an `ⓘ` line — "Not installed in your Airflow. `pip install …` — then refresh. Deploy is blocked until it's installed." (or "Needs Airflow X+") ✅. A non‑checkable prerequisite (e.g. a K8s cluster) still rides in the operator `description` 🔭.

### 15.6 Deploy — top‑bar action + tri‑state banner ✅ (incl. run‑on‑deploy + stop ✅)

Deploy is an *observable* lifecycle (§6.5.4), not a silent success — and (📝) **runs on deploy**: after the dag registers, the server unpauses + triggers a run, so the banner continues *Registered → Running → finished*. *(src: 04-main-demo build→deploy→native‑Airflow run)*

```
 top-bar:   ▶ Deploy  ─►  ⏳ Deploying…  ─►  ✓ Deployed     /     ✕ Failed

 DeployBanner (under the top bar):
 ① ┌ ⏳ Writing my_dag.py to the dags folder…                          ┐
 ② ┌ ⏳ Waiting for Airflow to pick it up (up to a few minutes)…       ┐
   │                                       [ Keep waiting ]    [ × ]   │
 ③a┌ ✓ Registered — unpausing + triggering my_dag…  [ Undeploy ]      ┐   📝 run-on-deploy
   │   ▶ Running (run_id 2026-06-17T…)            [ ⏹ Stop run ] [ × ] │   ← unpause→trigger
 ③a'┌ ✓ Run finished — my_dag         [ ▶ Run again ] [ Undeploy ] [ × ]┐
 ③b┌ ✕ Couldn’t load my_dag                            [ × ]          ┐
   │ ┌ A provider package isn’t installed                           │  ✅ explainImportError
   │ │ This DAG uses an operator from “airflow.providers.http”, but  │     (plain-language card)
   │ │ that provider isn’t installed in your Airflow.                │
   │ │ ⚠ Check the **call_api** task.                               │  ← task mapped from the trace
   │ │ pip install apache-airflow-providers-http …, then re-deploy.  │
   │ └ [ Show technical details ▾ ]                                  │
   │   [ ↩ Roll back to previous ]   [ Undeploy ]               [ × ] │ ✅ (Roll back when a backup exists)
```
Built ✅: atomic `SharedVolumeTarget` write + post‑deploy polling of `/dags` + `/importErrors`; banner renders writing/waiting/registered/running/finished/failed/processing. **Run on deploy ✅ (§6.5.4):** on `registered`, `StudioApp` unpauses (`setDagPaused false`) **then** triggers (`triggerDag`), captures the `dag_run_id`, and polls `getDagRun` so the banner advances ③a→③a' (`running`→`finished`); **⏹ Stop run** PATCHes the run to `failed` via `setDagRunState` (§6.6) and the same poll moves it to *finished*. *Run again* re‑invokes the unpause→trigger→poll flow. The §6.5.5 active‑run guard still gates the write; if the auto‑trigger fails the banner falls back to the manual *Unpause & trigger* button. **Undeploy / rollback ✅ (§7):** an **Undeploy** button rides every *deployed* phase (registered · finished · processing · failed) → a destructive‑confirm → `deleteDag`/`purge_dag` (remove the `.py` + purge history; the `.afdag` stays), then the banner goes idle. On a **failed** import where `backedUp` is true, **↩ Roll back to previous** calls `rollbackDag` (restore the `.bak`) and re‑enters the deploy lifecycle. **Friendly recovery ✅ (§7):** the failed banner no longer dumps a bare traceback — `explainImportError(stack_trace, currentIR)` (`src/importErrors.ts`) renders a plain‑language card (missing‑provider + `pip install …` line · Airflow‑2 path · unresolved import · syntax/indentation · undefined‑name · else the raw exception line) and, when a `task_id` appears in the trace, a **⚠ Check the <task> task** line; the raw traceback stays under *Show technical details*.

### 15.7 Palette — provider‑availability states ✅

How gated (Tier‑2/3) operators appear (§6.2.1). Unavailable ops stay **visible but dimmed** — never hidden, never blocked from the canvas. Third‑party (off‑constraints) ops are **not dimmed** (they deploy) but carry a neutral info glyph.

```
 ▾ HTTP                       ⟳  ← refresh: re-read the target's installed providers
   HTTP            dimmed  ⓘ │ title: Requires apache-airflow-providers-http in your
                              │        Airflow — pip install …-http. Deploy will block.
 ▾ SENSORS
   File sensor                 (available — standard provider is never gated)
   S3 key sensor   dimmed  ⓘ │ title: Requires …-amazon in your Airflow — pip install …
   Kubernetes pod  dimmed  ⓘ │ title: Requires …-cncf-kubernetes — pip install … (cluster
                              │        prerequisites are in the INFO tab, not checkable)
 ▾ DATA QUALITY
   GX checkpoint   normal  ⓘ │ title: Third-party package, off the constraints file —
                              │        pip install airflow-provider-great-expectations==1.0.0.
                              │        Deploy isn't blocked; missing → a clear import error.
 ▾ GOVERNANCE
   OpenMetadata    normal  ⓘ │ title: Third-party — pip install openmetadata-ingestion==…
                              │        (must match your OpenMetadata SERVER version).
```

Built ✅: the server reads the **target** Airflow's `/api/v2/providers` (+ `/api/v2/version`) via `AirflowClient.list_providers`/`version`, cached in `providers.py` with a **60 s TTL + a `?refresh=1` force** (the palette's **⟳** button → `loadOperators(true)`). `annotated_operators` tags each `GET operators` entry **`available | missing-provider | version-too-old | unknown`** (`unknown` = target unreachable → shown, never blocked). `Palette` dims an unavailable op (opacity) **and** appends an `ⓘ` glyph (non‑color‑only) with a `title` carrying the "Requires `…-X`" + `pip install` hint; the op is **still addable**. The **INFO** tab repeats the note (§15.5). **Deploy hard‑fails** before writing: `deploy_dag` runs `provider_block_errors(ir, target_index)` and returns a plain‑language "provider not installed in your target Airflow" in `errors` (the DeployBanner shows it) — a no‑op when the target is unreachable, so `/importErrors` stays the authoritative post‑deploy verdict. All P0 ops are standard‑provider → never gated. The **gated** ops now ship (✅) and dim/hard‑fail when their provider is absent from the target: `HTTP` (`providers‑http`), `SQL query` + `SqlSensor` (`providers‑common‑sql`), and the P2 cloud/K8s ops `KubernetesPodOperator` (`cncf‑kubernetes`), `S3KeySensor` (`amazon`), `GCSObjectExistenceSensor` + `BigQueryInsertJobOperator` (`google`). Studio gates on the **provider**; non‑checkable prereqs (a K8s cluster, a Connection) ride in the operator `description`/INFO tab. **Third‑party (off‑constraints) ops ✅ (§6.2.2 ¹ / §13 Q13):** GE + OpenMetadata are flagged `third_party: true` + a `version` pin → a distinct **`third-party`** availability state. They render **un‑dimmed** (deployable) with a neutral brand‑tinted `ⓘ` glyph + a pinned‑install `title` (`jp-afdag-palette-item-info`); the INFO tab shows the same note (`jp-afdag-info-thirdparty`). `provider_block_errors` **skips** them, so deploy is never hard‑blocked — `/importErrors` (+ the §7 friendly recovery) is the verdict.

### 15.8 Manager — DAG list (left sidebar) ✅ (incl. stop‑run + orphan banner ✅)

The operations surface. *(Mirrors what the demo shows running in the **native** Airflow UI — src: 04-main-demo f0400 — but rendered inside JupyterLab.)*

```
 ┌ Airflow — DAGs ──────────────────────────────── ⟳ ┐
 │ 🔍 Search dag_id…                      [ Tags ▾ ]  │
 │ ⚠ Import errors (1) ▾                              │
 │ ┌ load_dag.py                                      │   ✅ per-error friendly card
 │ │ A provider package isn’t installed               │      (explainImportError)
 │ │ pip install apache-airflow-providers-http …       │
 │ │ [ Open in Studio to fix ]  [ Show technical … ▾ ] │   ✅ dags/source → open .afdag
 │ └──────────────────────────────────────────────────│
 │ ⚠ 2 orphaned DAGs — .afdag source deleted ▾        │   📝 reconciliation sweep → §15.13(B)
 │ ──────────────────────────────────────────────────│
 │ ◐ my_dag         @daily   etl,prod      ⏸  ▶     🗑 │   ◐ run-status donut
 │ ● ingestion_dag  15m  ⏵running          ⏸  ▶  ⏹  🗑 │   ⏸ pause/unpause · ▶ trigger
 │ ⚠ load_dag       (import error)         ⏸  ▶     🗑 │   ⏹ stop run (running only, 📝)
 │ ──────────────────────────────────────────────────│   🗑 delete (purge file+history)
 └────────────────────────────────────────────────────┘
```
Built ✅: list (search/tag filter, `exclude_stale`), pause, trigger, run‑status, `has_import_errors` badge + import‑errors panel, delete (file‑then‑history). **Friendly import errors + "Open in Studio to fix" ✅ (§7):** each import error renders a plain‑language card (`explainImportError`) — filename, a friendly title/summary, a `pip install …`/fix hint, the raw trace tucked under *Show technical details* — with a one‑click **Open in Studio to fix** that calls `dags/source` (resolve the deployed `.py` → its source `.afdag` via the `afdag_id` provenance ↔ Contents‑root join) and opens it in the Studio factory; when the source is gone/pre‑provenance it explains why instead. **Trigger ✅:** a no‑params DAG triggers instantly; a DAG with `params` opens the conf dialog (§15.10). **Stop‑run ✅:** a **stop** link on a `running`/`queued` run in the drill‑down (§15.9) `PATCH`es it to `failed` (§6.6). **Orphan banner ✅:** the manager calls `dags/orphans` on every refresh and renders a warn‑coloured banner of deployed DAGs whose source `.afdag` was deleted (§6.5.6 / §15.13), each with *Undeploy & purge* / *Keep*.

### 15.9 Manager — run / task drill‑down + logs ✅ (incl. stop‑run ✅) · friendly log viewer ✅

Expand a DAG → runs → task instances → logs. *(Mirrors native grid/logs — src: 04-main-demo f0600/f0850.)*

```
 ┌ my_dag ─────────────────────────────────┐   Log modal
 │ RUNS                                     │   ┌ print2 ▾  try [1│2]  🔎 ⧉ ↧ ⤢wrap ┐
 │ ▾ 2026-03-14 17:25  ⏵ running [ ⏹ Stop ]─┼─  │ 17:25:01 INFO  Running BashOp…    │   📝 friendly viewer:
 │    • print1  ✓ success  try 1            │   │ 17:25:01 INFO  echo Hello         │   level colour+glyph,
 │    • print2  ✕ failed   try 2  [ logs ]──┼──▶│ 17:25:02 ERROR Command exited 1 ◀─┤   autoscroll-to-error,
 │    • print3  ◷ queued                    │   │ 17:25:02 ERROR Traceback (most…   │   search·copy·download,
 │ ▸ 2026-03-13 …      ✓                     │   │ [☐ errors only]    attempt 2 of 2 │   wrap · try selector
 │ ──────────────────────────────────────── │   └──────────────────────────────────┘
 │ [ Clear/Retry ▸ dry-run preview ]  [ Mark state… ]   [ ⏹ Stop run ] ← running only │
 └──────────────────────────────────────────┘
```
Built ✅: task instances + states, task logs (a single fetch of the task's **current try**, rendered as one raw `<pre>`), clear/retry (dry‑run preview → confirm), mark success/failed/skipped, and (✅) a **stop** link on a `running`/`queued` run that `PATCH`es it to `failed` (→ scheduler terminates its tasks, §6.6 / §8.8) behind a confirm — distinct from Clear/Retry (re‑run) and Mark‑state. **Friendly log viewer ✅ (§6.6):** the raw `<pre>` is replaced by a structured viewer (`LogViewer.tsx`) — **per‑level colour + an error left‑bar** (non‑color‑only), **traceback‑as‑error + autoscroll‑to‑first‑error**, an **attempt selector** (try 1…N, re‑fetched over the existing API), **search** + **errors‑only** filter, **Copy/Download**, a **Wrap** toggle, and a load/error state distinct from content. Levels come from Airflow 3's **structured events** when present (`client.get_task_logs` passes `events?: {event, timestamp?, level?, logger?}[]` through — §13 Q16 ✅, 2026‑06‑24), with **client‑side text `classifyLine` as the per‑event + plain‑text fallback**; the flattened `content` stays for Copy/Download (back‑compatible). `Overlay` gained Escape‑close + focus‑on‑open. ndjson live‑tailing stays deferred 🔭. *(Native grid/Gantt/XCom stay in Airflow’s own UI — NG3; optional deep‑link.)*

### 15.10 Manager — trigger‑with‑conf dialog ✅

The last missing piece of “triggers”: a conf form derived from the DAG’s `params`.

```
 ┌ Trigger my_dag ──────────────────────────────┐
 │ This DAG accepts parameters:                 │   ← fields from GET /dags/{id}/details
 │   start_date  [ 2026-06-15              ]     │   ← string+format:date → date input
 │   region      [ eu-west-1   ▾           ]     │   ← enum → dropdown
 │   dry_run     [ ☐ ]                           │   ← boolean → checkbox
 │   threshold   [ 0.5                     ]     │   ← number (min/max) → number input
 │   extra       [ {"k": "v"}              ]     │   ← object/array → JSON textarea
 │ ─────────────────────────────────────────── │
 │   logical_date  ◉ now    ○ [ pick…      ]     │   ← null logical_date = run now (AF3)
 │              [ Cancel ]       [ ▶ Trigger ]   │
 └───────────────────────────────────────────────┘
   DAGs with no params skip the dialog → instant bare trigger (today’s behavior).
```

Built ✅: `ManagerApp.trigger` calls **`getDagDetails`** (`GET /dags/{id}/details`); a DAG with a non‑empty `params` opens **`TriggerDialog`**, a no‑params DAG (or an unreadable details response) keeps the instant bare trigger. Airflow serializes each param as `{value, description, schema}` (a JSON‑Schema fragment); pure **`triggerForm.ts`** (`classifyParam`/`initialDraft`/`buildConf`, unit‑tested) projects that onto typed controls — **enum→dropdown, boolean→checkbox, integer/number→number input (min/max), string+`format:date|date-time`→date/datetime picker, object/array→JSON textarea, else text** — inferring the control from the default’s runtime type when the schema has no `type`. Each param’s `description` renders as inline help. On submit `buildConf` rebuilds the run `conf`: a cleared field falls back to the param default (key omitted, so Airflow uses its default) or sends explicit `null` when the schema is nullable; an `integer` field rejects a non‑whole value (no silent `parseInt` truncation); a `date-time` value is normalized to an **offset‑bearing UTC ISO string** (the offset‑less `datetime-local` value Airflow’s own `date-time` format validation would reject), and a `date-time` default is reshaped into the local‑input format so it actually populates; bad JSON/number blocks submit with an inline error that clears as the field is edited. **`logical_date`** defaults to **now** (null); *pick* converts the chosen local datetime to a UTC ISO string (and an empty *pick* is blocked, not silently run‑now). A **server‑side rejection keeps the dialog open** with the user’s conf intact and shows the Airflow error inline (rather than closing and discarding the form). `triggerDag(id, conf, logicalDate)` POSTs `/dags/trigger`. Verified end‑to‑end against `apache/airflow:3.0.2` (conf echoed back validated; pinned `logical_date` accepted; offset‑bearing `date-time` accepted while the naked local value is rejected). The form is plain themed React (no RJSF) to keep the manager bundle light.

> **Triggers — fully covered now:** the **TriggerDagRunOperator** ships as a palette operator (`operators/trigger_dagrun.yaml`) for composing multi‑DAG pipelines; the Manager's **one‑click trigger** runs a no‑params DAG instantly and routes a params DAG through the conf dialog above.

### 15.11 Rename a Studio DAG — document vs `dag_id` ✅

Rename splits by *what* is renamed and the deploy/run state (§6.1.8). The safe path (A) reuses JupyterLab's file rename; (B)/(B′) are a guided migration.

```
 (A) Rename the document (.afdag), not deployed → just a file rename, no Airflow impact
 ┌ Rename ──────────────────────────────────┐
 │ Name  [ my_dag.afdag              ]       │   reuses docmanager:rename;
 │              [ Cancel ]   [ Rename ]      │   dag_id + any deployed DAG unaffected
 └───────────────────────────────────────────┘

 (B) Change dag_id, DEPLOYED + idle → migration (new DAG, fresh history)
 ┌ Rename & redeploy ────────────────────────────────────────┐
 │ New dag_id   [ sales_etl_v2               ]   ✓ valid       │
 │ ⚠ Airflow has no rename — this creates a NEW DAG           │
 │   “sales_etl_v2” (paused, empty history). The old          │
 │   “sales_etl” history does NOT carry over.                 │
 │ Old DAG:   ◉ Keep history  (pause + remove file)           │
 │            ○ Purge old DAG (deletes its run history)        │
 │               [ Cancel ]        [ Rename & redeploy ]      │
 └────────────────────────────────────────────────────────────┘

 (B′) Change dag_id, DEPLOYED + run ACTIVE → blocked
 ┌ Rename & redeploy ────────────────────────────────────────┐
 │ ⛔ “sales_etl” has a run in progress. Renaming now would    │
 │    strand it (Airflow runs the latest file on disk).       │
 │    [ Watch run & continue when done ]                      │
 │    [ Override (lose the in-flight run) ]      [ Cancel ]    │
 └────────────────────────────────────────────────────────────┘
```
✅ built (parts A + B, 2026‑06‑16). (A) **Rename file…** reuses the JupyterLab document `context.rename` (filesystem‑only, no Airflow impact). (B) **Rename DAG id…** — the DAG‑form `dag_id` is read‑only; the toolbar action validates the new id, runs `renamePreflight`, then branches by state (draft = set id; deployed‑idle = confirm keep‑history vs purge → `runDeploy(newIR)` then `retireOldDag` once registered; deployed + active run = blocked). `afdag_id` (in the provenance header, §8.9) keeps the `.afdag` ↔ deployed‑DAG link across the rename.

### 15.12 Re‑deploy an updated DAG — active‑run + drift guards ✅

Editing + Deploy overwrites the same `{dag_id}.py` and re‑runs the lifecycle (§15.6). One shared dag‑state preflight gates the Deploy button on two conditions: a **run in flight** (§6.5.5 / §8.8) and **out‑of‑band drift** (the deployed file was hand‑edited, §6.5.3). Distinct from a `dag_id` rename (§15.11) — same file, same history.

```
 (deployed + idle, unchanged)  Deploy → overwrites {dag_id}.py → tri-state. No prompt.

 (deployed + run in progress)  Deploy →
 ┌ A run is in progress ──────────────────────────────────────┐
 │ ⛔ “sales_etl” has 1 run(s) in progress. Re-deploying       │
 │    overwrites the DAG file while it runs — Airflow runs     │
 │    the latest file on disk, so the in-flight run can break. │
 │        [ Cancel ]              [ Deploy anyway ]            │
 └────────────────────────────────────────────────────────────┘

 (deployed file hand-edited outside Studio)  Deploy →
 ┌ Modified outside Studio ───────────────────────────────────┐
 │ ⚠ “sales_etl” was edited directly in the dags folder since │
 │   Studio last deployed it. Deploying overwrites those      │
 │   manual edits with the current graph.                     │
 │        [ Cancel ]              [ Overwrite ]               │
 └────────────────────────────────────────────────────────────┘
```
✅ the preflight gates Deploy on **both** an active run (*Deploy anyway*) and **drift** (a hand‑edited deployed file → *Overwrite* / *Cancel*); drift uses a `code=sha256` body hash stamped in the provenance header vs. the on‑disk body. **Undeploy / rollback ✅ (§7):** the deploy banner offers **Undeploy** (deployed states → confirm → `deleteDag`/`purge_dag`: remove the `.py` + purge history; the `.afdag` stays) and, on a **failed** import when a backup exists, **↩ Roll back to previous** (`rollbackDag` → restore the `.bak`). Every overwrite‑deploy first copies the prior managed `.py` to `{dag_id}.py.bak` (ignored by the dag‑processor; cleared on delete), so a bad re‑deploy can return to the last deployed version; `deploy_dag` reports `backed_up` so the banner knows a rollback target exists. 🔭 still to come: **delete‑on‑source‑delete** is itself ✅ (§15.13).

### 15.13 Delete a Studio DAG document — undeploy reconciliation ✅

Deleting a `.afdag` should delete its deployed DAG (full purge, §6.5.6). Both detection layers feed **one** surface — the manager's orphan banner — so the per‑DAG *Undeploy & purge* confirm is the single consent point (§9). The in‑session `fileChanged` delete signal just makes the banner appear **instantly** (it re‑runs the sweep) rather than waiting for the next manual refresh; the sweep is also what catches terminal/`git`/`rm` deletes. *(✅ built — banner + signal; no reference frame, new surface.)*

```
 (A) In-session: deleting my_dag.afdag in the file browser fires
     contents.fileChanged(delete) → index.ts calls panel.refresh()
     → the orphan sweep re-runs → the banner (B) appears at once.

 (B) Orphan banner (manager sidebar, §15.8) — from `GET dags/orphans`:
 ┌ Airflow — DAGs ─────────────────────────────────── ⟳ ┐
 │ ⚠ 2 orphaned DAGs — their .afdag source was deleted ▾ │   warn-coloured banner
 │   • my_dag      [ Undeploy & purge ]   [ Keep ]       │   Undeploy → confirm modal:
 │   • sales_etl   [ Undeploy & purge ]   [ Keep ]       │   "removes .py + purges history"
 └────────────────────────────────────────────────────────┘
   Undeploy & purge → confirm → deleteDag (purge_dag: file-first, then DELETE /dags/{id}).
   Keep → hidden for the session (remembered, so refresh/in-session re-sweeps don't re-nag).
   (Airflow refuses delete while a task runs, §8.8 → stop the run first via §15.9.)
```
✅ built. Server: `find_orphans` diffs `afdag_id` provenance on deployed managed `.py` files (`SharedVolumeTarget.list()`) against the `afdag_id`s of live `.afdag` files walked from the Contents root (`dags/orphans` handler passes `contents_manager.root_dir`); remediation reuses **`purge_dag`** (file‑first, then `DELETE /dags/{id}`). Only provenance‑matched, Studio‑managed files with an `afdag_id` are eligible (hand‑written / pre‑provenance DAGs untouched, §9). The mirror of §15.12 drift (edited‑but‑present) for the **deleted‑source** case.

### 15.14 Studio editor — Notifications tab (callbacks) ✅ (DAG‑level + per‑task)

The inspector tab to attach **notifiers** to DAG callbacks (§6.8) — the half of "notifications" that isn't a graph node. The **same editor** also appears as a "Notifications" section in the NODE tab for per‑task callbacks (§15.1). *(Studio surface — no reference frame.)*

```
 ┌ DAG NODE INFO [NOTIFY] CODE SAVED ─────────────────────────┐
 │ Alert a channel when this DAG reaches an event. Notifiers   │
 │ run as Airflow callbacks, not graph tasks.                  │
 │ On failure — when the DAG run fails ──────────────────────  │
 │ ┌ Email (SMTP)                                        [✕] ┐ │
 │ │ TO * ⓘ      [ data-eng@example.com ]                    │ │
 │ │ SUBJECT ⓘ   [ {{ dag.dag_id }} failed ]                 │ │
 │ └─────────────────────────────────────────────────────────┘ │
 │ ＋ Add   [ Email (SMTP) ] [ Slack message ]                  │
 │ On success — when the DAG run succeeds ───────────────────  │
 │ No notifications.   ＋ Add  [ Email (SMTP) ] [ Slack… ]      │
 └─────────────────────────────────────────────────────────────┘
```
✅ built (DAG‑level **and** per‑task, §6.8). The **NOTIFY** inspector tab edits `ir.dag.callbacks` (`on_failure`/`on_success` — `sla_miss` is omitted, SLAs were removed in Airflow 3.0); each event lists add/remove notifiers with a registry‑driven RJSF form (per‑field `help`/`ⓘ`). A **notifier registry** (`notifiers/*.yaml` → `GET notifiers`, provider‑gated) ships 5 channels — `Smtp`, `Slack`, `Apprise` (multi‑channel → Teams/WhatsApp), `Discord`, `Opsgenie`; codegen wires `on_*_callback=[…]` into the `@dag`/`with DAG(…)` call with the notifier imports. **Per‑task callbacks ✅ (2026‑06‑22):** the identical editor is extracted into a shared `CallbacksEditor` and reused as a "Notifications" section in the **NODE** tab (§15.1), editing `node.callbacks` over `on_failure`/`on_retry`/`on_success` (`on_retry` is the task‑only event); codegen merges the rendered `on_*_callback=[…]` into the task's trailing kwargs (the `@task(…)` decorator for native ops, the operator call otherwise) and the deploy provider hard‑gate + the error badge scan node callbacks too. The **operator** channels (`EmailOperator`, `SlackAPIPostOperator`, …) ship as palette nodes (§6.2.2).

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
