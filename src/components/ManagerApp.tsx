import { ITranslator } from '@jupyterlab/translation';
import { refreshIcon, runIcon } from '@jupyterlab/ui-components';
import { ISignal } from '@lumino/signaling';
import * as React from 'react';
import { createPortal } from 'react-dom';

import {
  clearTasks,
  deleteDag,
  findOrphans,
  getTaskLogs,
  listDagRuns,
  listDags,
  listImportErrors,
  listTaskInstances,
  setDagPaused,
  setDagRunState,
  triggerDag
} from '../handler';
import {
  IDag,
  IDagRun,
  IImportError,
  IOrphan,
  ITaskInstance
} from '../interfaces';

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
  // Deployed DAGs whose source .afdag was deleted (PRD §6.5.6).
  const [orphans, setOrphans] = React.useState<IOrphan[]>([]);
  const [showOrphans, setShowOrphans] = React.useState(true);
  // dag_ids the user chose to "Keep" this session — don't re-nag on refresh.
  const keptOrphans = React.useRef<Set<string>>(new Set());

  const [runs, setRuns] = React.useState<RunMap>({});
  const [tasks, setTasks] = React.useState<TaskMap>({});
  const [logs, setLogs] = React.useState<ILogsModal | null>(null);
  const [confirm, setConfirm] = React.useState<IConfirm | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);

  // Latest query, read by the stable `refresh` so effects don't churn.
  const queryRef = React.useRef(query);
  queryRef.current = query;

  const refresh = React.useCallback(
    async (pattern: string = queryRef.current, sweep = true): Promise<void> => {
      setLoading(true);
      setError(null);
      const [dagRes, errRes, orphanRes] = await Promise.all([
        listDags(100, pattern),
        listImportErrors(),
        // The orphan sweep walks the whole Contents tree (§6.5.6), so skip it on
        // the per-keystroke search refresh — only run it on real refreshes.
        sweep ? findOrphans() : Promise.resolve(null)
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
      // Suppress orphans on a degraded sweep (a .afdag couldn't be read) — never
      // surface a destructive "source deleted" prompt on incomplete data.
      if (orphanRes && orphanRes.status === 'OK' && !orphanRes.data?.degraded) {
        setOrphans(
          (orphanRes.data?.orphans ?? []).filter(
            o => !keptOrphans.current.has(o.dag_id)
          )
        );
      }
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

  // Debounced search — skip the orphan sweep (it walks the Contents tree).
  React.useEffect(() => {
    const id = window.setTimeout(() => void refresh(query, false), 300);
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

  // Stop an in-flight run (§6.6): Airflow has no cancel, so this marks the run
  // failed (the scheduler then terminates its running tasks).
  const stopRun = (dagId: string, run: IDagRun): void => {
    setConfirm({
      title: trans.__('Stop run'),
      message: trans.__(
        'Stop run "%1" of "%2"? Airflow has no cancel — this marks the run failed and terminates its running tasks.',
        run.dag_run_id,
        dagId
      ),
      confirmLabel: trans.__('Stop run'),
      danger: true,
      onConfirm: async () => {
        setConfirm(null);
        const res = await setDagRunState(dagId, run.dag_run_id, 'failed');
        if (res.status === 'ERR') {
          setError(res.error ?? 'Failed to stop run');
          return;
        }
        await loadRuns(dagId);
      }
    });
  };

  // Orphan reconciliation (§6.5.6): the deployed DAG whose source .afdag was
  // deleted. "Undeploy & purge" reuses the same teardown as Delete.
  const undeployOrphan = (orphan: IOrphan): void => {
    setConfirm({
      title: trans.__('Undeploy orphaned DAG'),
      message: trans.__(
        'The source .afdag for "%1" was deleted. Undeploy it? This removes the deployed .py and purges its run history. This cannot be undone.',
        orphan.dag_id
      ),
      confirmLabel: trans.__('Undeploy & purge'),
      danger: true,
      onConfirm: async () => {
        setConfirm(null);
        const res = await deleteDag(orphan.dag_id);
        if (res.status === 'ERR') {
          setError(res.error ?? 'Failed to undeploy DAG');
          return;
        }
        keptOrphans.current.delete(orphan.dag_id);
        await refresh();
      }
    });
  };

  const keepOrphan = (orphan: IOrphan): void => {
    keptOrphans.current.add(orphan.dag_id);
    setOrphans(os => os.filter(o => o.dag_id !== orphan.dag_id));
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

      {orphans.length > 0 && (
        <div className="jp-airflow-importerrors jp-mod-warn">
          <button
            className="jp-airflow-importerrors-head"
            onClick={() => setShowOrphans(s => !s)}
          >
            {showOrphans ? '▾' : '▸'}{' '}
            {trans.__('Orphaned DAGs — source .afdag deleted')} (
            {orphans.length})
          </button>
          {showOrphans &&
            orphans.map(o => (
              <div key={o.dag_id} className="jp-airflow-orphan">
                <span className="jp-airflow-orphan-name" title={o.filename}>
                  {o.dag_id}
                </span>
                <button
                  className="jp-airflow-linkbtn jp-mod-danger"
                  onClick={() => undeployOrphan(o)}
                >
                  {trans.__('Undeploy & purge')}
                </button>
                <button
                  className="jp-airflow-linkbtn"
                  onClick={() => keepOrphan(o)}
                >
                  {trans.__('Keep')}
                </button>
              </div>
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
            onStopRun={stopRun}
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
  onStopRun: (dagId: string, run: IDagRun) => void;
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
                    {(run.state === 'running' || run.state === 'queued') && (
                      <button
                        className="jp-airflow-linkbtn jp-mod-danger"
                        title={trans.__('Stop this run')}
                        onClick={() => props.onStopRun(dag.dag_id, run)}
                      >
                        {trans.__('stop')}
                      </button>
                    )}
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
  // Portal to <body> so the fixed-position backdrop covers the whole window.
  // Rendered inside the left sidebar it gets trapped by the panel's containing
  // block (lumino widgets establish one via transform/contain), which clips the
  // modal to the narrow rail instead of centring it over the app.
  return createPortal(
    <div className="jp-airflow-overlay" onClick={props.onClose}>
      <div
        className="jp-airflow-overlay-inner"
        onClick={e => e.stopPropagation()}
      >
        {props.children}
      </div>
    </div>,
    document.body
  );
}

function basename(path?: string): string {
  if (!path) {
    return '(unknown file)';
  }
  return path.replace(/\\/g, '/').split('/').pop() || path;
}
