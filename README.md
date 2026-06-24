# jupyterlab-airflow

[![Build](https://github.com/groupnotes/jupyterlab-airflow/actions/workflows/build.yml/badge.svg)](https://github.com/groupnotes/jupyterlab-airflow/actions/workflows/build.yml)

A JupyterLab 4.x prebuilt extension for **Apache Airflow 3.x** — "Airflow Studio":
a **no-code visual DAG editor** plus an **operations manager**, without leaving
JupyterLab.

The extension is two cooperating halves:

- a **prebuilt labextension** (TypeScript/React) that renders the UI, and
- a **Jupyter server extension** (Python) that proxies the Airflow REST API
  (`/api/v2`) so Airflow credentials stay on the server and are never exposed to
  the browser. It fetches a JWT from `/auth/token`, caches it, and refreshes
  once on a `401`.

## Features

### Airflow Studio — visual DAG editor

Open or create an `.afdag` document (a JSON intermediate representation of a DAG
graph) and build a pipeline visually:

- **Drag-and-drop canvas** — add operators, connect them with rounded-arrow
  edges, and configure each in a registry-generated form; the `.afdag` IR is the
  single source of truth. Add free-text note cards; one-click **Tidy** auto-layout.
- **Operator palette — 44 operators** across Storage, Ingestion, Compute, Data
  Quality, Sensors, Flow Control, SQL/HTTP, Cloud, Kubernetes, Governance, and
  Notifications. The catalogue is **data-only** (one YAML per operator), and each
  op is **gated** on what is installed in your _target_ Airflow — unavailable ops
  are dimmed with a `pip install …` hint, and off-constraints third-party ops
  (Great Expectations, OpenMetadata) carry a distinct, never-blocked state.
- **Live validation** (required fields + cycle detection) and a **generated
  Python preview** with syntax highlighting — **TaskFlow or Traditional**, toggled
  per document.
- **Deploy** with an observable tri-state lifecycle banner (writing → waiting →
  registered / failed), **run-on-deploy**, **stop-run**, **undeploy**, **rollback**
  to the previous version, out-of-band **drift** detection, and a guided
  `dag_id`-rename migration.
- **Data-aware scheduling** (Airflow 3 Assets) — schedule a DAG on asset updates
  (all-of / any-of, or combined with a cron schedule via `AssetOrTimeSchedule`)
  and declare per-task `inlets`/`outlets`.
- **Notifications** — attach notifier callbacks (Smtp / Slack / Apprise / Discord
  / Opsgenie) to DAG-level and per-task lifecycle events.
- **Friendly recovery** — a failed import shows a plain-language card with a fix
  hint and an "Open in Studio to fix" button; every field has a hoverable help
  bubble.

### Resource manager (left sidebar)

Lists your DAGs (search / tag filter) and lets you, without leaving JupyterLab:
pause/unpause, **trigger** (with a parameter-conf form), drill into runs and task
instances, view logs in a **friendly log viewer** (per-level colour from Airflow's
structured events, an attempt selector, search, errors-only, Copy / Download /
Wrap, autoscroll-to-first-error), **clear/retry**, **delete**, and resolve
**import errors** and orphaned (source-deleted) DAGs.

### Deploy targets

A pluggable `DeployTarget`, selected by `AIRFLOW_DEPLOY_TARGET`:

- **`shared_volume`** (default) — atomic write into the Airflow dags folder.
- **`git`** — commit (and optionally push) generated DAGs to a git working tree
  an Airflow `GitDagBundle` tracks.
- **`s3`** — write DAGs as objects under a key prefix an S3 DAG bundle reads
  (AWS S3 or any S3-compatible store such as MinIO).

All three share namespacing, the Studio-provenance collision guard, backup /
rollback, and `.airflowignore` handling.

### Audit

Every mutating action (deploy / trigger / pause / stop-run / clear / delete /
rollback / retire) emits a structured `{ts, user, action, dag_id,
correlation_id, outcome}` JSON line on the `jupyterlab_airflow.audit` logger
(routable to a file / SIEM via standard logging config). The deploy
`correlation_id` is also stamped into the deployed `.py` provenance header, so a
failed import traces back to its Studio deploy session.

## Requirements

- JupyterLab >= 4.0.0, < 5
- Python >= 3.9
- An Apache Airflow 3.x instance reachable from the Jupyter server, with the
  REST API (`/api/v2`) and JWT token endpoint (`/auth/token`) enabled.

## Configuration

The server extension is configured entirely through **environment variables** on
the Jupyter server process (credentials never live in the frontend or in tracked
files):

### Airflow connection

| Variable             | Default                 | Description                                              |
| -------------------- | ----------------------- | -------------------------------------------------------- |
| `AIRFLOW_API_URL`    | `http://localhost:8080` | Base URL of the Airflow API server.                      |
| `AIRFLOW_USERNAME`   | `admin`                 | Username used to obtain a JWT from `/auth/token`.        |
| `AIRFLOW_PASSWORD`   | `admin`                 | Password used to obtain a JWT.                           |
| `AIRFLOW_API_TOKEN`  | _(unset)_               | A pre-minted JWT. If set, username/password are ignored. |
| `AIRFLOW_VERIFY_SSL` | `true`                  | Set to `false` to skip TLS verification (self-signed).   |

### Operator registry

| Variable                | Default   | Description                                                         |
| ----------------------- | --------- | ------------------------------------------------------------------- |
| `AIRFLOW_OPERATORS_DIR` | _(unset)_ | Extra directory of operator YAML that overrides/extends the bundle. |

### Deploy target

| Variable                  | Default             | Description                                                    |
| ------------------------- | ------------------- | -------------------------------------------------------------- |
| `AIRFLOW_DEPLOY_TARGET`   | `shared_volume`     | `shared_volume`, `git`, or `s3` — which `DeployTarget` to use. |
| `AIRFLOW_DAGS_DIR`        | `/opt/airflow/dags` | Shared-volume dags folder (the `shared_volume` target).        |
| `AIRFLOW_GIT_DAGS_REPO`   | _(unset)_           | git target: local git working tree the `GitDagBundle` tracks.  |
| `AIRFLOW_GIT_DAGS_SUBDIR` | `dags`              | git target: DAG subdirectory within the repo.                  |
| `AIRFLOW_GIT_DAGS_BRANCH` | `main`              | git target: branch to commit/push.                             |
| `AIRFLOW_GIT_DAGS_REMOTE` | _(unset)_           | git target: remote to push to; unset → commit-only.            |
| `AIRFLOW_S3_DAGS_BUCKET`  | _(unset)_           | s3 target: bucket the S3 DAG bundle reads (requires `boto3`).  |
| `AIRFLOW_S3_DAGS_PREFIX`  | `dags`              | s3 target: key prefix for DAG objects.                         |
| `AIRFLOW_S3_ENDPOINT_URL` | _(unset)_           | s3 target: endpoint for an S3-compatible store (e.g. MinIO).   |

> **Multi-user trust model.** The server uses **one Airflow service account per
> Jupyter server process**. On JupyterHub each user gets their own server, so
> inject **per-user** Airflow credentials at spawn for real per-user authorization;
> until then, any user of a given server acts as that one Airflow account, and the
> shared dags folder/bundle is a shared trust boundary (deploying a DAG runs code
> as the Airflow worker). All mutating actions are audited regardless. See
> [`docs/PRD.md`](docs/PRD.md) §9.

## Install

```bash
pip install jupyterlab-airflow
```

## Development install

> See [`.devcontainer/README.md`](.devcontainer/README.md) for a ready-to-use
> dev container that also spins up a local Airflow 3.x.

```bash
# Clone, then from the repo root:
pip install -e ".[test]"
jupyter labextension develop . --overwrite      # symlink the prebuilt extension
jupyter server extension enable jupyterlab_airflow
jlpm install

# Iterate: rebuild TS + labextension on change (one terminal)
jlpm watch
# Run JupyterLab (another terminal)
jupyter lab
```

## Testing

```bash
# Frontend (jest)
jlpm test
# Server extension (pytest)
python -m pytest jupyterlab_airflow/tests/
# Lint / format
jlpm lint:check
```

## Uninstall

```bash
pip uninstall jupyterlab-airflow
```

## API endpoints (server extension)

All under the JupyterLab base URL, namespace `jupyterlab-airflow`. Every response
is wrapped as `{data}` (success) or `{error, detail}` (failure); the frontend
normalizes both. The server proxies Airflow `/api/v2` and never exposes the JWT.

| Method | Endpoint                 | Purpose                                                |
| ------ | ------------------------ | ------------------------------------------------------ |
| GET    | `/health`                | Obtain a token; report the connection.                 |
| GET    | `/operators`             | Operator registry, annotated with target availability. |
| GET    | `/notifiers`             | Notifier registry (notification callbacks).            |
| POST   | `/generate`              | Render an `.afdag` IR to Airflow-3 Python.             |
| POST   | `/validate`              | Full validation pipeline (incl. DagBag import).        |
| POST   | `/deploy`                | Validate, then write the DAG via the deploy target.    |
| GET    | `/deploy/status`         | One observation of the deploy tri-state.               |
| GET    | `/importerrors`          | DAG import errors.                                     |
| GET    | `/dags`                  | List DAGs (search/paginate).                           |
| GET    | `/dags/details`          | DAG detail incl. `params` (trigger conf form).         |
| POST   | `/dags/pause`            | Pause / unpause a DAG.                                 |
| POST   | `/dags/trigger`          | Trigger a run (optional `conf`, `logical_date`).       |
| POST   | `/dags/delete`           | Remove the `.py` + purge history (undeploy).           |
| POST   | `/dags/rollback`         | Restore the previous deployed version.                 |
| GET    | `/dags/orphans`          | Deployed DAGs whose source `.afdag` was deleted.       |
| GET    | `/dags/source`           | Resolve a deployed DAG to its source `.afdag`.         |
| GET    | `/dags/rename/preflight` | Deploy state for the `dag_id`-rename migration.        |
| POST   | `/dags/retire`           | Retire the old `dag_id` after a rename.                |
| GET    | `/dagruns`               | List a DAG's runs.                                     |
| GET    | `/dagruns/get`           | One DagRun's state.                                    |
| POST   | `/dagruns/state`         | Set a run's state (stop a run → `failed`).             |
| GET    | `/taskinstances`         | Task instances of a run.                               |
| GET    | `/taskinstances/logs`    | Task logs (structured events + flattened text).        |
| POST   | `/taskinstances/clear`   | Clear/retry task instances (dry-run preview).          |

## Design & documentation

[`docs/PRD.md`](docs/PRD.md) is the canonical product spec — the architecture, the
`.afdag` IR, codegen, deploy/lifecycle, the operator-registry contract, security
model, and §15 ASCII wireframes of every screen.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
