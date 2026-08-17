# Docker dev environment

Development environment for the **jupyterlab-airflow** extension: a JupyterLab 4
dev container plus a local Apache Airflow 3.x instance to develop against, run
via plain `docker compose` (no VS Code / Dev Containers required) — **and no
image to build**, locally or in CI.

The `docker-compose.yml` lives at the repo root; run all commands below from there.

## Quick start

```bash
cp .env.sample .env      # first time only; adjust COMPOSE_PROFILES / HOST_SSH_DIR as needed
docker compose up -d
docker compose logs -f jupyter
```

JupyterLab is served automatically at http://localhost:8889/lab (no token — offset from JupyterLab's usual 8888 so this stack can run alongside another docker-compose project's `jupyter` service, e.g. `jupyterlab-db-explorer`, on the same host). Open a shell in the running container with:

```bash
docker compose exec jupyter bash
```

Stop everything with `docker compose down` (add `-v` to also delete the `airflow-home`/`airflow-pgdata`/`node_modules`/`usr-local` volumes for a clean slate — note `airflow-pgdata` holds Airflow's metadata, so `-v` discards DAG run history).

## How it works — no build, live reload both ways

The `jupyter` service runs [`nikolaik/python-nodejs:python3.12-nodejs22`](https://github.com/nikolaik/docker-python-nodejs) — a pre-built, off-the-shelf Python 3.12 + Node 22 image pulled straight from Docker Hub, unmodified. There is no `docker/Dockerfile` and `docker compose up` never builds anything; the first run just pulls the image, same as any other public image.

The repo is bind-mounted at `/workspace`. On every `docker compose up` / restart, `docker/entrypoint.sh` runs automatically and:

1. `pip install jupyterlab hatchling hatch-jupyter-builder hatch-nodejs-version` — the CLI tooling the project needs (not part of the base image)
2. `jlpm install` — installs/updates JS dependencies
3. `pip install -e ".[test]"` — installs the Python package in editable mode (this also runs the TS/webpack build once via the `hatch-jupyter-builder` hook)
4. `jupyter labextension develop . --overwrite` — symlinks the extension into JupyterLab
5. `jupyter server extension enable jupyterlab_airflow`
6. `jlpm watch` in the background — recompiles TypeScript and rebuilds the labextension bundle on every source change
7. `jupyter lab --autoreload` in the foreground on `:8888` (published on the host as `:8889` — see Services below)

So:

- **Frontend (`src/`, `style/`)** changes are picked up live by `jlpm watch`; refresh the browser tab to see them.
- **Backend (`jupyterlab_airflow/*.py`)** changes are picked up **automatically** — `--autoreload` makes Jupyter Server watch every imported `.py` file and restart the whole process when one changes. No `docker compose restart` needed. (If autoreload ever misbehaves, `docker compose restart jupyter` is the manual fallback — still just a process restart, no rebuild.)
- **Dependency changes** (`package.json`, `pyproject.toml`) are picked up automatically on the next `docker compose restart jupyter` / `up`, since steps 1–3 above re-run every start.
- There is genuinely **nothing to rebuild, ever** — not even after changing this dev environment's own tooling, since it isn't baked into an image.

`node_modules` and `/usr/local` (Python packages, their `bin/` launcher scripts, and Node/yarn) live in named Docker volumes (`node_modules`, `usr-local`) rather than the bind-mounted workspace or the base image's writable layer, so installs stay native to the container's Linux filesystem (avoiding host/container binary mismatches) and persist across `docker compose down` + `up` — a full recreate stays fast because pip/jlpm find everything already installed and only fetch what changed.

Persisting `/usr/local` needed one specific workaround: `jupyter labextension develop --overwrite` (step 4 above) replaces `/usr/local/share/jupyter/labextensions/jupyterlab-airflow` — a real directory pip's editable install populated — with a **symlink** into the bind-mounted repo. With `/usr/local` persisted, that symlink persists too, so the *next* `pip install -e .` finds an "existing installation" and tries its usual uninstall-then-reinstall dance; its uninstall step expects the real files its RECORD listed and instead finds a symlink to a directory missing one of them (`build_log.json`), crashing mid-rollback. `docker/entrypoint.sh` now wipes `jupyterlab_airflow`'s own install footprint (dist-info, the `.pth` file, that labextensions path, its two `jupyter_*_config.d` JSON files) before every install, so it's always a clean install for that one package — its dependencies are untouched and stay cached. Verified clean across three consecutive full `down`+`up` cycles.

The container runs as **root** (`user: root`): the base image's default non-root user (`pn`) can't write the root-owned named volumes above without `sudo`, which isn't installed in this image. Root is fine for a disposable local dev container; `jupyter lab` is passed `--allow-root` accordingly.

## Services

| Service | Image | Ports | Profile | Purpose |
| ------- | ----- | ----- | ------- | ------- |
| jupyter | `nikolaik/python-nodejs:python3.12-nodejs22` | 8889→8888, 9998→9999 | (always on) | JupyterLab dev container |
| airflow | `apache/airflow:3.0.2` | 8081→8080 | airflow | Local Airflow 3.x (standalone) |
| airflow-db | `postgres:16` | — | airflow | Airflow's metadata database |

> **Why Postgres and not the default SQLite.** SQLite allows a single writer, and with `LocalExecutor` the scheduler, dag-processor and api-server all contend for it. That contention killed the scheduler here with `sqlite3.OperationalError: database is locked`, and `standalone` did not restart it — so for hours DAGs deployed and registered but **silently never ran**. Worse, the container still reported `healthy`, because the healthcheck only pinged the API server. The healthcheck now also runs `airflow jobs check --job-type SchedulerJob`, so a dead scheduler shows up as `Up (unhealthy)` instead of hiding. If you ever do see runs stuck in `queued`, that is the symptom to check first.
>
> Switching the metadata DB **resets Airflow's own state** (DAG run history). Your `.afdag` files and the deployed `.py` are untouched, and DAGs re-register from the dags folder on the next scan.

## Airflow provider packages (why every palette node is enabled)

Studio dims an operator in the palette — and hard-blocks a deploy that uses it — when its provider package isn't installed in the **target** Airflow (PRD §6.2.1; the check reads `GET /api/v2/providers` from the `airflow` container, not from Jupyter).

`apache/airflow:3.0.2` already ships ten of the providers the registry needs (`amazon`, `cncf-kubernetes`, `common-sql`, `ftp`, `google`, `http`, `sftp`, `slack`, `smtp`, `standard`). The remaining seven are installed at container start via `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yml` — no image build:

`apache-spark` · `apprise` · `discord` · `imap` · `opsgenie` · `papermill` · `telegram`

Versions are pinned to what the official 3.0.2 constraints resolve to, so a rebuild-free `up` stays reproducible instead of drifting onto a future incompatible release. With these in place all **47** gated operators/notifiers report `available` and none are dimmed.

The install also passes `--constraint` pointing at Airflow's own 3.0.2 constraints file, and that flag is **load-bearing**. The two third-party ops sit *outside* that file (PRD §13 Q13), and without the pin pip will happily satisfy them by resolving a different Airflow — a first attempt at adding them selected `apache-airflow-core==3.3.1`, i.e. it would have silently upgraded Airflow out from under this pinned 3.0.2 environment. A constraints file only pins packages listed in it, so a third-party package still installs; it simply can no longer drag Airflow along. Anything genuinely unsatisfiable now fails loudly instead of quietly breaking the scheduler.

### The two third-party ops

| Op | Package | Installed here? |
| -- | ------- | --------------- |
| Great Expectations | `airflow-provider-great-expectations==1.0.0` | **Yes** — resolves cleanly under the 3.0.2 constraints |
| OpenMetadata lineage | `openmetadata-ingestion` | **No — it cannot be** |

`openmetadata-ingestion` is not installable alongside Airflow 3.0.2 at any version. Every published release (0.8.0 → 1.13.3.2, roughly 350 of them) conflicts with the constraint set on a different pin — `idna>=3.15` vs `idna==3.10`, `PyJWT>=2.13.0`, `httpx~=0.28.0`, `boto3~=1.41.5`, `chardet==4.0.0`, `jaraco.context>=6.1.0`. Unconstrained it appears to work only because pip upgrades Airflow itself.

This costs the product nothing: Studio *never* deploy-blocks a third-party op — it shows a pinned-install hint and lets Airflow's own import error be the verdict (PRD §13 Q13). The palette entry stays usable and the generated DAG is correct; it just won't run in *this* dev container. If you need it working, run it against an Airflow whose constraints admit it (OpenMetadata ships its own `openmetadata-ingestion`-based Airflow image for exactly this reason) rather than forcing it into this one.

To add another provider, append a pinned entry to `_PIP_ADDITIONAL_REQUIREMENTS` and `docker compose up -d airflow`.

> **Why a bind-mounted pip cache.** These packages reinstall on every container **recreate** (a plain `restart` keeps the writable layer, so pip just reports "already satisfied"). One of them pulls `pyspark`, which ships no matching wheel and is built from source — measured at ~1630s to recreate on an empty cache versus ~236s once the built wheel is cached. `docker/airflow-pip-cache/` is bind-mounted to `/tmp/.cache` for that reason. Two details worth knowing if you touch it: the image sets `PIP_CACHE_DIR=/tmp/.cache/pip`, so mounting the usual `~/.cache` caches nothing; and it must be a **bind** mount, because a fresh *named* volume is created root-owned while the container runs as uid 50000 and cannot write into it. The folder's contents are gitignored.

## Enable / disable Airflow

The Airflow service is controlled via a Docker Compose **profile**. Edit `.env` at the repo root:

```env
# Remove 'airflow' to run JupyterLab on its own (e.g. to target a remote Airflow).
COMPOSE_PROFILES=airflow
```

After changing, run `docker compose up -d` again.

## Airflow credentials

Airflow 3 runs with the **SimpleAuthManager**. Credentials are pre-seeded from
`docker/airflow-config/passwords.json` (mounted into the container) so they are
deterministic:

| Field    | Value                  |
| -------- | ---------------------- |
| URL      | http://localhost:8081  |
| Username | `admin`                |
| Password | `admin`                |

> The UI is published on host port **8081** (mapped to the container's 8080) to
> avoid clashing with other services commonly bound to 8080. Inside the compose
> network Airflow is still reached at `http://airflow:8080`.

The JupyterLab server extension reaches Airflow over the compose network via the
environment variables set on the `jupyter` service in `docker-compose.yml`:

```yaml
AIRFLOW_API_URL: http://airflow:8080
AIRFLOW_USERNAME: admin
AIRFLOW_PASSWORD: admin
```

To point the extension at a **remote** Airflow instead, disable the `airflow`
profile and override these variables (e.g. set `AIRFLOW_API_TOKEN` to a
pre-minted JWT).

## Audit trail

Every mutating action — deploy, trigger, pause, stop-run, clear, delete, rollback, retire, and Variable/Connection writes — is written as one JSON object per line to:

```
<jupyter data dir>/airflow-studio/audit.log      # in this container: /root/.local/share/jupyter/...
```

The server prints the resolved path at startup (`audit trail → …`). Tail it with:

```bash
docker compose exec jupyter tail -f /root/.local/share/jupyter/airflow-studio/audit.log
```

Each record carries `{ts, user, action, dag_id, correlation_id, outcome, via}`. `via` is `request` (a human clicked something) or `reconciler` (the server finished a deploy's lifecycle in the background) — the `user` is the human either way. `outcome` is `ok`, `rejected`, `denied` (a viewer was refused), `error`, or `needs_repair`. The `correlation_id` is also stamped into the deployed `.py` header, so one grep ties a file on disk back to the deploy, retire, unpause and trigger that produced it.

The file is created `0600` (it names users and what they did) and rotates at 5 MB × 5. `JUPYTERLAB_AIRFLOW_AUDIT_LOG` overrides the path, or set it to `off` if you route the `jupyterlab_airflow.audit` logger yourself — if that logger already has handlers, the extension attaches none of its own.

> This was previously **inert**: the records were emitted but the logger sat under a handler-less root at `WARNING`, so they were silently discarded. If you are upgrading and wondered why the trail was empty, that is why.

## Read-only (viewer) mode

`JUPYTERLAB_AIRFLOW_ROLE` controls whether this server may change Airflow:

```env
JUPYTERLAB_AIRFLOW_ROLE=viewer   # unset or "editor" = full access (the default)
```

A **viewer** can read everything — the DAG list, runs, task logs, import errors, the generated CODE tab, live validation — but every privileged action is refused with a `403`: deploy/undeploy/retire/rollback, trigger/pause/stop-run/clear/delete, and writing Variables or Connections. The refusal happens **server-side, before anything runs**, so a denied deploy writes no file; the UI's read-only mode is a courtesy on top, not the control. Each refusal is written to the audit log with `outcome="denied"`.

To try it: set the variable in `.env` and `docker compose restart jupyter`.

> Unset defaults to `editor`, so nothing changes unless you opt in. An unrecognised value falls back to `viewer` and logs a warning — a typo in a permission setting should never hand out the rights it was meant to withhold.
>
> On **JupyterHub** each user gets their own server process, so set this per user at spawn (`c.Spawner.environment`) — the same hook you would use to inject per-user Airflow credentials. Note that a `.afdag` is a normal file in the user's own workspace: a viewer can still edit it in a text editor. What is enforced is that nothing they change can reach Airflow.

## DAGs

Airflow's bundled example DAGs are **off** (`AIRFLOW__CORE__LOAD_EXAMPLES=false`):
several of them in 3.0.2 import a test-only `tests_common` module that isn't in the
published wheel, so they show up as a spurious import error. Deploying a DAG from
the Studio editor writes it to `docker/airflow-dags/`; add more `.py` files there
directly and the dag-processor picks them up within a minute. This is also the
`AIRFLOW_DAGS_DIR` deploy target the `jupyter` service is configured with.

## SSH keys inside the container

`HOST_SSH_DIR` (in `.env`) is bind-mounted read-only to `/root/.ssh` (the container runs as root — see above). It defaults to the empty, gitignored `docker/ssh-keys/` folder (a no-op). Point it at your real SSH directory to use your own keys for `git` inside the container, e.g. `HOST_SSH_DIR=${HOME}/.ssh` (macOS/Linux/Git Bash) or `HOST_SSH_DIR=C:/Users/you/.ssh` (Windows).

## Verify Airflow

From a terminal inside the `jupyter` container (`docker compose exec jupyter bash`):

```bash
# Get a JWT and list DAGs through the REST API the extension uses.
TOKEN=$(curl -s -X POST http://airflow:8080/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | python -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

curl -s http://airflow:8080/api/v2/dags -H "Authorization: Bearer $TOKEN" | head -c 400
```

## Updating the base image

There's nothing to rebuild, but you can bump the pinned tag in `docker-compose.yml` (e.g. a newer Node major) and pull it:

```bash
docker compose pull jupyter
docker compose up -d
```
