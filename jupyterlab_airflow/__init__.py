from ._version import __version__
from .handlers import setup_handlers


def _jupyter_labextension_paths():
    return [{
        "src": "labextension",
        "dest": "jupyterlab-airflow"
    }]


def _jupyter_server_extension_points():
    return [{
        "module": "jupyterlab_airflow"
    }]


def _load_jupyter_server_extension(server_app):
    """Registers the API handler to receive HTTP requests from the frontend extension.

    Parameters
    ----------
    server_app: jupyterlab.labapp.LabApp
        JupyterLab application instance
    """
    setup_handlers(server_app.web_app)
    # The deploy reconciler (PRD §6.5.4): the server finishes a deploy's
    # lifecycle — wait for registration, retire the renamed-away dag_id, unpause,
    # trigger — even if the browser tab closes. It is lazily scheduled, so a
    # server that never deploys creates no timer and makes no Airflow calls.
    from . import journal, reconciler

    journal.set_root(getattr(server_app, "data_dir", None))
    rec = reconciler.configure(server_app)  # None when disabled / no journal
    if rec is not None:
        rec.start()
    server_app.web_app.settings["jupyterlab_airflow_reconciler"] = rec
    name = "jupyterlab_airflow"
    server_app.log.info(f"Registered {name} server extension")


# For backward compatibility with notebook server - useful for Binder/JupyterHub
load_jupyter_server_extension = _load_jupyter_server_extension
