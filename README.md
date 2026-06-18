# jupyterlab-airflow

[![Build](https://github.com/groupnotes/jupyterlab-airflow/actions/workflows/build.yml/badge.svg)](https://github.com/groupnotes/jupyterlab-airflow/actions/workflows/build.yml)

A JupyterLab 4.x extension for **Apache Airflow 3.x**. It adds a left-sidebar
panel that lists your DAGs and lets you pause/unpause, trigger runs, and inspect
recent run states — without leaving JupyterLab.

The extension is made of two parts:

- a **prebuilt labextension** (TypeScript/React) that renders the sidebar, and
- a **Jupyter server extension** (Python) that proxies the Airflow REST API
  (`/api/v2`) so Airflow credentials stay on the server and are never exposed to
  the browser.

## Requirements

- JupyterLab >= 4.0.0, < 5
- An Apache Airflow 3.x instance reachable from the Jupyter server, with the
  REST API (`/api/v2`) and JWT token endpoint (`/auth/token`) enabled.

## Configuration

The server extension is configured entirely through environment variables on
the Jupyter server process:

| Variable            | Default                 | Description                                              |
| ------------------- | ----------------------- | -------------------------------------------------------- |
| `AIRFLOW_API_URL`   | `http://localhost:8080` | Base URL of the Airflow API server.                      |
| `AIRFLOW_USERNAME`  | `admin`                 | Username used to obtain a JWT from `/auth/token`.        |
| `AIRFLOW_PASSWORD`  | `admin`                 | Password used to obtain a JWT.                           |
| `AIRFLOW_API_TOKEN` | _(unset)_               | A pre-minted JWT. If set, username/password are ignored. |
| `AIRFLOW_VERIFY_SSL`| `true`                  | Set to `false` to skip TLS verification (self-signed).   |

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
# Link your dev source with JupyterLab
jupyter labextension develop . --overwrite
# Enable the server extension
jupyter server extension enable jupyterlab_airflow

# Rebuild the TypeScript on change (one terminal)
jlpm build
# Run JupyterLab (another terminal)
jupyter lab
```

## Testing

```bash
# Frontend
jlpm test
# Server extension
python -m pytest jupyterlab_airflow/tests/
```

## Uninstall

```bash
pip uninstall jupyterlab-airflow
```

## API endpoints (server extension)

All under the JupyterLab base URL, namespace `jupyterlab-airflow`:

| Method | Endpoint         | Proxies to                                  |
| ------ | ---------------- | ------------------------------------------- |
| GET    | `/health`        | obtains a token; reports the connection     |
| GET    | `/dags`          | `GET /api/v2/dags`                          |
| POST   | `/dags/pause`    | `PATCH /api/v2/dags/{id}` (`is_paused`)     |
| POST   | `/dags/trigger`  | `POST /api/v2/dags/{id}/dagRuns`            |
| GET    | `/dagruns`       | `GET /api/v2/dags/{id}/dagRuns`             |

## License

BSD-3-Clause. See [LICENSE](LICENSE).
