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

JupyterLab is served automatically at http://localhost:8888/lab (no token). Open a shell in the running container with:

```bash
docker compose exec jupyter bash
```

Stop everything with `docker compose down` (add `-v` to also delete the `airflow-db`/`node_modules`/`pip-site-packages` volumes for a clean slate).

## How it works — no build, live reload both ways

The `jupyter` service runs [`nikolaik/python-nodejs:python3.12-nodejs22`](https://github.com/nikolaik/docker-python-nodejs) — a pre-built, off-the-shelf Python 3.12 + Node 22 image pulled straight from Docker Hub, unmodified. There is no `docker/Dockerfile` and `docker compose up` never builds anything; the first run just pulls the image, same as any other public image.

The repo is bind-mounted at `/workspace`. On every `docker compose up` / restart, `docker/entrypoint.sh` runs automatically and:

1. `pip install jupyterlab hatchling hatch-jupyter-builder hatch-nodejs-version` — the CLI tooling the project needs (not part of the base image)
2. `jlpm install` — installs/updates JS dependencies
3. `pip install -e ".[test]"` — installs the Python package in editable mode (this also runs the TS/webpack build once via the `hatch-jupyter-builder` hook)
4. `jupyter labextension develop . --overwrite` — symlinks the extension into JupyterLab
5. `jupyter server extension enable jupyterlab_airflow`
6. `jlpm watch` in the background — recompiles TypeScript and rebuilds the labextension bundle on every source change
7. `jupyter lab --autoreload` in the foreground on `:8888`

So:

- **Frontend (`src/`, `style/`)** changes are picked up live by `jlpm watch`; refresh the browser tab to see them.
- **Backend (`jupyterlab_airflow/*.py`)** changes are picked up **automatically** — `--autoreload` makes Jupyter Server watch every imported `.py` file and restart the whole process when one changes. No `docker compose restart` needed. (If autoreload ever misbehaves, `docker compose restart jupyter` is the manual fallback — still just a process restart, no rebuild.)
- **Dependency changes** (`package.json`, `pyproject.toml`) are picked up automatically on the next `docker compose restart jupyter` / `up`, since steps 1–3 above re-run every start.
- There is genuinely **nothing to rebuild, ever** — not even after changing this dev environment's own tooling, since it isn't baked into an image.

`node_modules` and the installed Python packages (JupyterLab, this project, etc.) live in named Docker volumes (`node_modules`, `pip-site-packages`) rather than the bind-mounted workspace or the base image's writable layer, so installs stay native to the container's Linux filesystem (avoiding host/container binary mismatches) and persist across `docker compose down` + `up` — a fresh container still starts fast because pip/jlpm find everything already installed and only fetch what changed.

The container runs as **root** (`user: root`): the base image's default non-root user (`pn`) can't write the root-owned named volumes above without `sudo`, which isn't installed in this image. Root is fine for a disposable local dev container; `jupyter lab` is passed `--allow-root` accordingly.

## Services

| Service | Image                                       | Ports      | Profile     | Purpose                        |
| ------- | -------------------------------------------- | ---------- | ----------- | ------------------------------- |
| jupyter | `nikolaik/python-nodejs:python3.12-nodejs22` | 8888, 9999 | (always on) | JupyterLab dev container        |
| airflow | `apache/airflow:3.0.2`                       | 8081→8080  | airflow     | Local Airflow 3.x (standalone)  |

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
