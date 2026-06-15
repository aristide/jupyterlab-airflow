import { ITranslator } from '@jupyterlab/translation';
import { refreshIcon, runIcon } from '@jupyterlab/ui-components';
import { ISignal } from '@lumino/signaling';
import * as React from 'react';

import {
  clearTasks,
  deleteDag,
  getTaskLogs,
  listDagRuns,
  listDags,
  listImportErrors,
  listTaskInstances,
  setDagPaused,
  triggerDag
} from '../handler';
import { IDag, IDagRun, IImportError, ITaskInstance } from '../interfaces';

type Trans = ReturnType<ITranslator['load']>;

export interface IManagerAppProps {
  trans: Trans;
  refreshSignal: ISignal<unknown, void>;
}

type RunMap = Record<string, IDagRun[] | 'loading'>;
type TaskMap = Record<string, ITaskInstance[] | 'loading'>;

interface ILogsModal {
  title: string;
  text: string;
}

interface IConfirm {
  title: string;
  message: string;
  confirmLabel: string;
  danger?: boolean;
  onConfirm: () => void;
}

const runKey = (dagId: string, runId: string): string => `${dagId}::${runId}`;

export function ManagerApp(props: IManagerAppProps): JSX.Element {
  const { trans } = props;
  const [dags, setDags] = React.useState<IDag[]>([]);
  const [importErrors, setImportErrors] = React.useState<IImportError[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [query, setQuery] = React.useState('');
  const [showErrors, setShowErrors] = React.useState(true);

  const [runs, setRuns] = React.useState<RunMap>({});
  const [tasks, setTasks] = React.useState<TaskMap>({});
  const [logs, setLogs] = React.useState<ILogsModal | null>(null);
  const [confirm, setConfirm] = React.useState<IConfirm | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);

  // Latest query, read by the stable `refresh` so effects don't churn.
  const queryRef = React.useRef(query);
  queryRef.current = query;

  const refresh = React.useCallback(
    async (pattern: string = queryRef.current): Promise<void> => {
      setLoading(true);
      setError(null);
      const [dagRes, errRes] = await Promise.all([
        listDags(100, pattern),
        listImportErrors()
      ]);
      setLoading(false);
      if (dagRes.status === 'ERR') {
        setError(dagRes.error ?? 'Unknown error');
        return;
      }
      setDags(dagRes.data?.dags ?? []);
      setImportErrors(
        errRes.status === 'OK' ? (errRes.data?.import_errors ?? []) : []
      );
      setRuns({});
      setTasks({});
    },
    []
  );

  // Initial load + external refresh command.
  React.useEffect(() => {
    void refresh();
    const handler = (): void => void refresh();
    props.refreshSignal.connect(handler);
    return () => {
      props.refreshSignal.disconnect(handler);
    };
  }, [props.refreshSignal, refresh]);

  // Debounced search.
  React.useEffect(() => {
    const id = window.setTimeout(() => void refresh(query), 300);
    return () => window.clearTimeout(id);
  }, [query, refresh]);

  const togglePause = async (dag: IDag): Promise<void> => {
    const res = await setDagPaused(dag.dag_id, !dag.is_paused);
    if (res.status === 'OK') {
      setDags(ds =>
        ds.map(d =>
          d.dag_id === dag.dag_id ? { ...d, is_paused: !d.is_paused } : d
        )
      );
    } else {
      setError(res.error ?? 'Failed to update DAG');
    }
  };

  const loadRuns = async (dagId: string): Promise<void> => {
    const res = await listDagRuns(dagId);
    setRuns(r => ({
      ...r,
      [dagId]: res.status === 'OK' ? (res.data?.dag_runs ?? []) : []
    }));
  };

  const trigger = async (dag: IDag): Promise<void> => {
    const res = await triggerDag(dag.dag_id);
    if (res.status === 'ERR') {
      setError(res.error ?? 'Failed to trigger DAG');
      return;
    }
    setBusy(trans.__('Triggered %1', dag.dag_id));
    window.setTimeout(() => setBusy(null), 2500);
    if (dag.dag_id in runs) {
      await loadRuns(dag.dag_id);
    }
  };

  const toggleDag = async (dagId: string): Promise<void> => {
    if (dagId in runs) {
      setRuns(r => {
        const next = { ...r };
        delete next[dagId];
        return next;
      });
      return;
    }
    setRuns(r => ({ ...r, [dagId]: 'loading' }));
    await loadRuns(dagId);
  };

  const toggleRun = async (dagId: string, runId: string): Promise<void> => {
    const key = runKey(dagId, runId);
    if (key in tasks) {
      setTasks(t => {
        const next = { ...t };
        delete next[key];
        return next;
      });
      return;
    }
    setTasks(t => ({ ...t, [key]: 'loading' }));
    const res = await listTaskInstances(dagId, runId);
    setTasks(t => ({
      ...t,
      [key]: res.status === 'OK' ? (res.data?.task_instances ?? []) : []
    }));
  };

  const viewLogs = async (
    dagId: string,
    runId: string,
    ti: ITaskInstance
  ): Promise<void> => {
    setLogs({ title: `${ti.task_id} — loading…`, text: trans.__('Loading…') });
    const res = await getTaskLogs(dagId, runId, ti.task_id, ti.try_number || 1);
    setLogs({
      title: `${ti.task_id} (try ${ti.try_number || 1})`,
      text:
        res.status === 'OK'
          ? res.data?.content || trans.__('(empty)')
          : (res.error ?? trans.__('Failed to load logs'))
    });
  };

  const clearTask = async (
    dagId: string,
    runId: string,
    ti: ITaskInstance
  ): Promise<void> => {
    const preview = await clearTasks(dagId, runId, [ti.task_id], true);
    const count =
      preview.status === 'OK' ? (preview.data?.total_entries ?? 1) : 1;
    setConfirm({
      title: trans.__('Clear & retry'),
      message: trans.__(
        'This will clear %1 task instance(s) so Airflow re-runs them. Continue?',
        count
      ),
      confirmLabel: trans.__('Clear & retry'),
      onConfirm: async () => {
        setConfirm(null);
        const res = await clearTasks(dagId, runId, [ti.task_id], false);
        if (res.status === 'ERR') {
          setError(res.error ?? 'Failed to clear');
          return;
        }
        await toggleRun(dagId, runId);
        await toggleRun(dagId, runId);
      }
    });
  };

  const removeDag = (dag: IDag): void => {
    setConfirm({
      title: trans.__('Delete DAG'),
      message: trans.__(
        'Delete "%1"? This removes the deployed .py file and purges its run history. This cannot be undone.',
        dag.dag_id
      ),
      confirmLabel: trans.__('Delete'),
      danger: true,
      onConfirm: async () => {
        setConfirm(null);
        const res = await deleteDag(dag.dag_id);
        if (res.status === 'ERR') {
          setError(res.error ?? 'Failed to delete DAG');
          return;
        }
        await refresh();
      }
    });
  };

  return (
    <div className="jp-airflow-root">
      <div className="jp-airflow-header">
        <span className="jp-airflow-title">{trans.__('Airflow DAGs')}</span>
        <button
          className="jp-airflow-iconbtn"
          title={trans.__('Refresh')}
          onClick={() => void refresh()}
        >
          <refreshIcon.react tag="span" width="16px" height="16px" />
        </button>
      </div>

      <input
        className="jp-airflow-search"
        placeholder={trans.__('Filter by dag_id…')}
        value={query}
        onChange={e => setQuery(e.target.value)}
      />

      {busy && <div className="jp-airflow-toast">{busy}</div>}

      {importErrors.length > 0 && (
        <div className="jp-airflow-importerrors">
          <button
            className="jp-airflow-importerrors-head"
            onClick={() => setShowErrors(s => !s)}
          >
            {showErrors ? '▾' : '▸'} {trans.__('Import errors')} (
            {importErrors.length})
          </button>
          {showErrors &&
            importErrors.map((err, i) => (
              <details key={err.import_error_id ?? i} className="jp-airflow-ie">
                <summary>{basename(err.filename)}</summary>
                <pre>{err.stack_trace ?? trans.__('(no details)')}</pre>
              </details>
            ))}
        </div>
      )}

      {loading && (
        <div className="jp-airflow-status">{trans.__('Loading…')}</div>
      )}
      {error && (
        <div className="jp-airflow-error">
          {error}
          <div className="jp-airflow-hint">
            {trans.__(
              'Check the AIRFLOW_API_URL / AIRFLOW_USERNAME / AIRFLOW_PASSWORD environment variables on the Jupyter server.'
            )}
          </div>
        </div>
      )}
      {!loading && !error && dags.length === 0 && (
        <div className="jp-airflow-status">{trans.__('No DAGs found.')}</div>
      )}

      <ul className="jp-airflow-list">
        {dags.map(dag => (
          <DagRow
            key={dag.dag_id}
            dag={dag}
            trans={trans}
            runs={runs}
            tasks={tasks}
            onToggleDag={toggleDag}
            onToggleRun={toggleRun}
            onPause={togglePause}
            onTrigger={trigger}
            onDelete={removeDag}
            onViewLogs={viewLogs}
            onClear={clearTask}
          />
        ))}
      </ul>

      {logs && (
        <Overlay onClose={() => setLogs(null)}>
          <div className="jp-airflow-modal jp-airflow-logs">
            <div className="jp-airflow-modal-head">
              <span>{logs.title}</span>
              <button
                className="jp-airflow-iconbtn"
                onClick={() => setLogs(null)}
              >
                ✕
              </button>
            </div>
            <pre className="jp-airflow-logtext">{logs.text}</pre>
          </div>
        </Overlay>
      )}

      {confirm && (
        <Overlay onClose={() => setConfirm(null)}>
          <div className="jp-airflow-modal">
            <div className="jp-airflow-modal-head">{confirm.title}</div>
            <div className="jp-airflow-modal-body">{confirm.message}</div>
            <div className="jp-airflow-modal-actions">
              <button
                className="jp-airflow-btn"
                onClick={() => setConfirm(null)}
              >
                {trans.__('Cancel')}
              </button>
              <button
                className={
                  confirm.danger
                    ? 'jp-airflow-btn jp-mod-danger'
                    : 'jp-airflow-btn jp-mod-accent'
                }
                onClick={() => void confirm.onConfirm()}
              >
                {confirm.confirmLabel}
              </button>
            </div>
          </div>
        </Overlay>
      )}
    </div>
  );
}

interface IDagRowProps {
  dag: IDag;
  trans: Trans;
  runs: RunMap;
  tasks: TaskMap;
  onToggleDag: (dagId: string) => void;
  onToggleRun: (dagId: string, runId: string) => void;
  onPause: (dag: IDag) => void;
  onTrigger: (dag: IDag) => void;
  onDelete: (dag: IDag) => void;
  onViewLogs: (dagId: string, runId: string, ti: ITaskInstance) => void;
  onClear: (dagId: string, runId: string, ti: ITaskInstance) => void;
}

function DagRow(props: IDagRowProps): JSX.Element {
  const { dag, trans, runs, tasks } = props;
  const dagRuns = runs[dag.dag_id];
  const schedule =
    dag.timetable_summary ||
    (typeof dag.schedule_interval === 'string'
      ? dag.schedule_interval
      : dag.schedule_interval?.value) ||
    '—';

  return (
    <li className="jp-airflow-dag">
      <div className="jp-airflow-dagrow">
        <button
          className="jp-airflow-expand"
          onClick={() => props.onToggleDag(dag.dag_id)}
          title={trans.__('Show recent runs')}
        >
          {dag.dag_id in runs ? '▾' : '▸'}
        </button>
        <span className="jp-airflow-dagname" title={dag.description ?? ''}>
          {dag.dag_display_name || dag.dag_id}
        </span>
        {dag.has_import_errors && (
          <span
            className="jp-airflow-badge jp-mod-error"
            title={trans.__('This DAG has an import error')}
          >
            !
          </span>
        )}
        <span className="jp-airflow-schedule">{schedule}</span>
        <label
          className="jp-airflow-pause"
          title={dag.is_paused ? trans.__('Paused') : trans.__('Active')}
        >
          <input
            type="checkbox"
            checked={!dag.is_paused}
            onChange={() => props.onPause(dag)}
          />
        </label>
        <button
          className="jp-airflow-iconbtn"
          title={trans.__('Trigger DAG')}
          onClick={() => props.onTrigger(dag)}
        >
          <runIcon.react tag="span" width="16px" height="16px" />
        </button>
        <button
          className="jp-airflow-iconbtn jp-mod-danger"
          title={trans.__('Delete DAG')}
          onClick={() => props.onDelete(dag)}
        >
          ✕
        </button>
      </div>

      {dag.dag_id in runs && (
        <ul className="jp-airflow-runs">
          {dagRuns === 'loading' ? (
            <li className="jp-airflow-status">{trans.__('Loading runs…')}</li>
          ) : (dagRuns ?? []).length === 0 ? (
            <li className="jp-airflow-status">{trans.__('No runs yet.')}</li>
          ) : (
            (dagRuns as IDagRun[]).map(run => {
              const key = runKey(dag.dag_id, run.dag_run_id);
              const tis = tasks[key];
              return (
                <li key={run.dag_run_id} className="jp-airflow-run">
                  <div className="jp-airflow-runrow">
                    <button
                      className="jp-airflow-expand"
                      onClick={() =>
                        props.onToggleRun(dag.dag_id, run.dag_run_id)
                      }
                      title={trans.__('Show task instances')}
                    >
                      {key in tasks ? '▾' : '▸'}
                    </button>
                    <span
                      className={`jp-airflow-state jp-airflow-state-${run.state}`}
                    >
                      {run.state}
                    </span>
                    <span className="jp-airflow-runid">{run.dag_run_id}</span>
                  </div>
                  {key in tasks && (
                    <ul className="jp-airflow-tasks">
                      {tis === 'loading' ? (
                        <li className="jp-airflow-status">
                          {trans.__('Loading tasks…')}
                        </li>
                      ) : (tis ?? []).length === 0 ? (
                        <li className="jp-airflow-status">
                          {trans.__('No task instances.')}
                        </li>
                      ) : (
                        (tis as ITaskInstance[]).map(ti => (
                          <li key={ti.task_id} className="jp-airflow-task">
                            <span
                              className={`jp-airflow-state jp-airflow-state-${ti.state}`}
                            >
                              {ti.state ?? '—'}
                            </span>
                            <span className="jp-airflow-taskid">
                              {ti.task_id}
                            </span>
                            <button
                              className="jp-airflow-linkbtn"
                              onClick={() =>
                                props.onViewLogs(dag.dag_id, run.dag_run_id, ti)
                              }
                            >
                              {trans.__('logs')}
                            </button>
                            <button
                              className="jp-airflow-linkbtn"
                              onClick={() =>
                                props.onClear(dag.dag_id, run.dag_run_id, ti)
                              }
                            >
                              {trans.__('clear')}
                            </button>
                          </li>
                        ))
                      )}
                    </ul>
                  )}
                </li>
              );
            })
          )}
        </ul>
      )}
    </li>
  );
}

function Overlay(props: {
  children: React.ReactNode;
  onClose: () => void;
}): JSX.Element {
  return (
    <div className="jp-airflow-overlay" onClick={props.onClose}>
      <div onClick={e => e.stopPropagation()}>{props.children}</div>
    </div>
  );
}

function basename(path?: string): string {
  if (!path) {
    return '(unknown file)';
  }
  return path.replace(/\\/g, '/').split('/').pop() || path;
}
