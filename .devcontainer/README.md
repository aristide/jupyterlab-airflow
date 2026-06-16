# Devcontainer

Development environment for the **jupyterlab-airflow** extension: a JupyterLab 4
dev container plus a local Apache Airflow 3.x instance to develop against.

## Services

| Service | Image                       | Ports      | Profile     | Purpose                          |
| ------- | --------------------------- | ---------- | ----------- | -------------------------------- |
| jupyter | aristidetm/labextension-dev | 8888, 9999 | (always on) | JupyterLab dev container         |
| airflow | apache/airflow:3.0.2        | 8081→8080  | airflow     | Local Airflow 3.x (standalone)   |

## Enable / Disable Airflow

The Airflow service is controlled via a Docker Compose **profile**. Edit
`.devcontainer/.env`:

```env
# Remove 'airflow' to run JupyterLab on its own (e.g. to target a remote Airflow).
COMPOSE_PROFILES=airflow
```

## Airflow credentials

Airflow 3 runs with the **SimpleAuthManager**. Credentials are pre-seeded from
`.devcontainer/airflow-config/passwords.json` (mounted into the container) so
they are deterministic:

| Field    | Value                  |
| -------- | ---------------------- |
| URL      | http://localhost:8081  |
| Username | `admin`                |
| Password | `admin`                |

> The UI is published on host port **8081** (mapped to the container's 8080) to
> avoid clashing with other services commonly bound to 8080. Inside the compose
> network Airflow is still reached at `http://airflow:8080`.

The JupyterLab server extension reaches Airflow over the compose network via the
environment variables set on the `jupyter` service:

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
published wheel, so they show up as a spurious import error. A demo DAG lives in
`.devcontainer/airflow-dags/`; add more `.py` files there and the dag-processor
picks them up within a minute. (Re-enable examples by flipping the flag in
`docker-compose.yaml` if you want them as reference content.)

## Quick Start

1. Open the project in VS Code with the Dev Containers extension.
2. Select **Reopen in Container**.
3. Wait for the image to build and Airflow to become healthy (first start takes
   a couple of minutes while it migrates its metadata DB).
4. Build & install the extension (see the project root `README.md`):
   ```bash
   pip install -e ".[test]"
   jupyter labextension develop . --overwrite
   jupyter server extension enable jupyterlab_airflow
   jlpm watch    # in one terminal
   jupyter lab --ip=0.0.0.0 --port=8888 --no-browser   # in another
   ```
5. Open the **Airflow** panel from the JupyterLab left sidebar.

## Verify Airflow

From a terminal inside the dev container:

```bash
# Get a JWT and list DAGs through the REST API the extension uses.
TOKEN=$(curl -s -X POST http://airflow:8080/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | python -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

curl -s http://airflow:8080/api/v2/dags -H "Authorization: Bearer $TOKEN" | head -c 400
```

## Build the dev image

```bash
docker build -t aristidetm/labextension-dev .
```
