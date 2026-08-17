import { ITranslator } from '@jupyterlab/translation';
import { refreshIcon, runIcon } from '@jupyterlab/ui-components';
import { ISignal } from '@lumino/signaling';
import * as React from 'react';
import { createPortal } from 'react-dom';

import {
  apiError,
  clearTasks,
  deleteDag,
  findDagSource,
  findOrphans,
  getDagDetails,
  getTaskLogs,
  listDagRuns,
  listDags,
  listImportErrors,
  listTaskInstances,
  setDagPaused,
  retireOldDag,
  setDagRunState,
  triggerDag
} from '../handler';
import {
  IDag,
  IDagParam,
  IDagRun,
  IImportError,
  IOrphan,
  ISupersededDag,
  ITaskInstance
} from '../interfaces';
import { explainImportError } from '../importErrors';
import {
  CanEditContext,
  useCanEdit,
  useFetchCanEdit
} from './capabilitiesContext';
import { ILogViewerData, LogViewer } from './LogViewer';
import { TriggerDialog } from './TriggerDialog';

type Trans = ReturnType<ITranslator['load']>;

export interface IManagerAppProps {
  trans: Trans;
  refreshSignal: ISignal<unknown, void>;
  /** Open a `.afdag` source in the Studio editor ("Open in Studio to fix"). */
  openPath?: (path: string) => void;
}

type RunMap = Record<string, IDagRun[] | 'loading'>;
type TaskMap = Record<string, ITaskInstance[] | 'loading'>;

interface IConfirm {
  title: string;
  message: string;
  confirmLabel: string;
  danger?: boolean;
  onConfirm: () => void;
}

const runKey = (dagId: string, runId: string): string => `${dagId}::${runId}`;

/** How often the panel re-reads Airflow on its own. Matched to the design's
 *  footer copy; short enough that a triggered run visibly changes state, long
 *  enough that an idle panel is not a load source. */
const AUTO_REFRESH_MS = 15_000;

/** Ticks between orphan sweeps. The sweep walks the entire Contents tree, so it
 *  cannot ride the same cadence as a plain list read. */
const ORPHAN_SWEEP_EVERY = 4;

export function ManagerApp(props: IManagerAppProps): JSX.Element {
  const { trans, openPath } = props;
  // Advisory (PRD §9) — the server enforces it on every mutating endpoint.
  const canEdit = useFetchCanEdit();
  const [dags, setDags] = React.useState<IDag[]>([]);
  const [importErrors, setImportErrors] = React.useState<IImportError[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [query, setQuery] = React.useState('');
  const [showErrors, setShowErrors] = React.useState(true);
  // Deployed DAGs whose source .afdag was deleted (PRD §6.5.6).
  const [orphans, setOrphans] = React.useState<IOrphan[]>([]);
  // Renamed-but-not-retired leftovers (PRD §15.11). Tracked separately from
  // orphans because the remedy differs: retire, not purge.
  const [superseded, setSuperseded] = React.useState<ISupersededDag[]>([]);
  const [showSuperseded, setShowSuperseded] = React.useState(true);
  const keptSuperseded = React.useRef<Set<string>>(new Set());
  const [showOrphans, setShowOrphans] = React.useState(true);
  // dag_ids the user chose to "Keep" this session — don't re-nag on refresh.
  const keptOrphans = React.useRef<Set<string>>(new Set());

  const [runs, setRuns] = React.useState<RunMap>({});
  const [tasks, setTasks] = React.useState<TaskMap>({});
  const [logs, setLogs] = React.useState<ILogViewerData | null>(null);
  const [confirm, setConfirm] = React.useState<IConfirm | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);
  // Open trigger-with-conf dialog for a DAG that declares params (PRD §15.10).
  const [triggerTarget, setTriggerTarget] = React.useState<{
    dagId: string;
    params: Record<string, IDagParam>;
  } | null>(null);

  // Latest query, read by the stable `refresh` so effects don't churn.
  const queryRef = React.useRef(query);
  queryRef.current = query;

  // Which DAGs are expanded, read by a background refresh so it can re-read
  // them without taking `runs` as a dependency (which would rebuild `refresh`
  // on every drill-down and restart the poll timer with it).
  const runsRef = React.useRef<RunMap>(runs);
  runsRef.current = runs;

  // Auto-refresh stands down while anything modal is up. A list re-ordering
  // underneath a confirm dialog is how someone confirms the wrong DAG, and a
  // log view jumping mid-read is just hostile.
  const suspendPollRef = React.useRef(false);
  suspendPollRef.current = Boolean(confirm || triggerTarget || logs);

  const refresh = React.useCallback(
    async (
      pattern: string = queryRef.current,
      sweep = true,
      background = false
    ): Promise<void> => {
      // A background tick is deliberately quiet: no spinner, because flashing
      // "Loading…" over a stable list every 15 seconds reads as instability
      // rather than freshness.
      if (!background) {
        setLoading(true);
        setError(null);
      }
      const [dagRes, errRes, orphanRes] = await Promise.all([
        listDags(100, pattern),
        listImportErrors(),
        // The orphan sweep walks the whole Contents tree (§6.5.6), so skip it on
        // the per-keystroke search refresh — only run it on real refreshes.
        sweep ? findOrphans() : Promise.resolve(null)
      ]);
      if (!background) {
        setLoading(false);
      }
      if (dagRes.status === 'ERR') {
        // A failed background poll leaves the last good list on screen. One
        // transient blip should not replace a working view with an error
        // banner, and the next tick will either recover or the user will
        // refresh by hand and see the real message.
        if (!background) {
          setError(dagRes.error ?? 'Unknown error');
        }
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
        setSuperseded(
          (orphanRes.data?.superseded ?? []).filter(
            s => !keptSuperseded.current.has(s.dag_id)
          )
        );
      }
      if (!background) {
        setRuns({});
        setTasks({});
        return;
      }
      // Background: keep every open drill-down open and re-read it in place.
      // Collapsing them on a timer would make the one thing auto-refresh is
      // FOR — watching a run progress — the one thing you cannot do.
      const open = Object.keys(runsRef.current);
      if (!open.length) {
        return;
      }
      const fetched = await Promise.all(
        open.map(async dagId => {
          const res = await listDagRuns(dagId);
          return [
            dagId,
            res.status === 'OK' ? (res.data?.dag_runs ?? []) : []
          ] as const;
        })
      );
      setRuns(current => {
        const next = { ...current };
        for (const [dagId, dagRuns] of fetched) {
          // Only refresh rows the user still has open: they may have collapsed
          // one while the request was in flight, and re-adding it would fight
          // them.
          if (dagId in current) {
            next[dagId] = dagRuns;
          }
        }
        return next;
      });
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

  // Auto-refresh. Airflow state changes without us — a scheduled run starts, a
  // task fails, the reconciler finishes a deploy — and until now the panel only
  // learned about any of it when the user pressed refresh.
  React.useEffect(() => {
    let ticks = 0;
    const id = window.setInterval(() => {
      // A hidden tab polls nothing: on JupyterHub this is one server process
      // per user, and a forgotten tab in a background window has no business
      // holding a 15-second heartbeat against Airflow indefinitely.
      if (document.hidden || suspendPollRef.current) {
        return;
      }
      ticks += 1;
      // The orphan sweep walks the whole Contents tree, so it rides a slower
      // cadence than the list itself — once a minute, not four times.
      void refresh(queryRef.current, ticks % ORPHAN_SWEEP_EVERY === 0, true);
    }, AUTO_REFRESH_MS);

    // Coming back to the tab reads immediately rather than waiting out the rest
    // of an interval. Skipping ticks while hidden is what makes that necessary:
    // without this, the first thing you see after switching back is data as old
    // as however long you were away.
    const onVisible = (): void => {
      if (!document.hidden && !suspendPollRef.current) {
        void refresh(queryRef.current, true, true);
      }
    };
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      window.clearInterval(id);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [refresh]);

  const togglePause = async (dag: IDag): Promise<void> => {
    const res = await setDagPaused(dag.dag_id, !dag.is_paused);
    if (res.status === 'OK') {
      setDags(ds =>
        ds.map(d =>
          d.dag_id === dag.dag_id ? { ...d, is_paused: !d.is_paused } : d
        )
      );
    } else {
      setError(apiError(res, 'Failed to update DAG'));
    }
  };

  const loadRuns = async (dagId: string): Promise<void> => {
    const res = await listDagRuns(dagId);
    setRuns(r => ({
      ...r,
      [dagId]: res.status === 'OK' ? (res.data?.dag_runs ?? []) : []
    }));
  };

  // Fire a run. On success: toast + refresh the run list. Returns an error
  // string (or null) so callers decide where to show it — the no-params path
  // uses the top-level banner; the conf dialog keeps it inline so the user's
  // conf isn't lost (PRD §15.10).
  const runTrigger = async (
    dagId: string,
    conf: Record<string, unknown> = {},
    logicalDate: string | null = null
  ): Promise<string | null> => {
    const res = await triggerDag(dagId, conf, logicalDate);
    if (res.status === 'ERR') {
      return apiError(res, trans.__('Failed to trigger DAG'));
    }
    setBusy(trans.__('Triggered %1', dagId));
    window.setTimeout(() => setBusy(null), 2500);
    if (dagId in runs) {
      await loadRuns(dagId);
    }
    return null;
  };

  const trigger = async (dag: IDag): Promise<void> => {
    // Read the DAG's params: a params DAG opens the conf dialog; a no-params DAG
    // — or an unreadable details response — keeps the instant bare trigger.
    const res = await getDagDetails(dag.dag_id);
    const params = res.status === 'OK' ? (res.data?.params ?? {}) : {};
    if (Object.keys(params).length === 0) {
      const err = await runTrigger(dag.dag_id);
      if (err) {
        setError(err);
      }
      return;
    }
    setTriggerTarget({ dagId: dag.dag_id, params });
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

  const loadLogs = async (
    dagId: string,
    runId: string,
    taskId: string,
    tryNumber: number,
    maxTry: number
  ): Promise<void> => {
    setLogs({
      dagId,
      runId,
      taskId,
      tryNumber,
      maxTry,
      text: null,
      events: undefined,
      truncated: false,
      error: null
    });
    const res = await getTaskLogs(dagId, runId, taskId, tryNumber);
    setLogs(prev => {
      // Ignore a stale response if the user switched DAG / task / try meanwhile.
      if (
        !prev ||
        prev.dagId !== dagId ||
        prev.taskId !== taskId ||
        prev.runId !== runId ||
        prev.tryNumber !== tryNumber
      ) {
        return prev;
      }
      if (res.status === 'OK') {
        return {
          ...prev,
          text: res.data?.content ?? '',
          events: res.data?.events,
          truncated: res.data?.truncated ?? false,
          error: null
        };
      }
      return {
        ...prev,
        text: null,
        error: res.error ?? trans.__('Failed to load logs')
      };
    });
  };

  const viewLogs = (dagId: string, runId: string, ti: ITaskInstance): void => {
    const maxTry = ti.try_number || 1;
    void loadLogs(dagId, runId, ti.task_id, maxTry, maxTry);
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
          setError(apiError(res, 'Failed to clear'));
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
          setError(apiError(res, 'Failed to delete DAG'));
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
          setError(apiError(res, 'Failed to stop run'));
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
          setError(apiError(res, 'Failed to undeploy DAG'));
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

  // Finish the migration the editor started. `purge: false` deletes the old
  // generated file and pauses the old DAG but KEEPS its run history — the
  // non-destructive half of the §15.11 dialog, so it needs no second
  // confirmation. Purging remains available per-row via Delete.
  const retireSuperseded = (item: ISupersededDag): void => {
    void (async () => {
      setBusy(trans.__('Retiring %1…', item.dag_id));
      const res = await retireOldDag(item.dag_id, false);
      setBusy(null);
      if (res.status === 'ERR') {
        setError(apiError(res, trans.__('Failed to retire %1', item.dag_id)));
        return;
      }
      setSuperseded(list => list.filter(s => s.dag_id !== item.dag_id));
      await refresh();
    })();
  };

  const keepSuperseded = (item: ISupersededDag): void => {
    keptSuperseded.current.add(item.dag_id);
    setSuperseded(list => list.filter(s => s.dag_id !== item.dag_id));
  };

  // "Open in Studio to fix" (PRD §7): resolve the failed deployed file back to
  // its source `.afdag` and open it in the editor. The source may be gone (a
  // pre-provenance deploy, or the design file was deleted) — say so plainly.
  const openInStudio = async (err: IImportError): Promise<void> => {
    if (!openPath) {
      return;
    }
    setBusy(trans.__('Locating source…'));
    const res = await findDagSource({ filename: err.filename });
    setBusy(null);
    if (res.status === 'ERR') {
      setError(res.error ?? 'Failed to locate the DAG source');
      return;
    }
    if (res.data?.path) {
      openPath(res.data.path);
    } else {
      setError(
        trans.__(
          "Couldn't find the .afdag source for %1 — it may have been deleted or deployed before source tracking.",
          basename(err.filename)
        )
      );
    }
  };

  return (
    <CanEditContext.Provider value={canEdit}>
      <div className="jp-airflow-root">
        <div className="jp-airflow-header">
          <div className="jp-airflow-eyebrow">{trans.__('Apache Airflow')}</div>
          <span className="jp-airflow-title">{trans.__('Airflow DAGs')}</span>
          {!canEdit && (
            <span
              className="jp-airflow-viewonly"
              title={trans.__(
                'You can browse DAGs, runs and logs, but not trigger, pause or delete them.'
              )}
            >
              {trans.__('View only')}
            </span>
          )}
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
              importErrors.map((err, i) => {
                const explained = explainImportError(err.stack_trace);
                return (
                  <div
                    key={err.import_error_id ?? i}
                    className="jp-airflow-ie jp-airflow-ie-card"
                  >
                    <div className="jp-airflow-ie-file">
                      {basename(err.filename)}
                    </div>
                    <div className="jp-airflow-ie-title">{explained.title}</div>
                    <div className="jp-airflow-ie-summary">
                      {explained.summary}
                    </div>
                    {explained.hint && (
                      <div className="jp-airflow-ie-hint">{explained.hint}</div>
                    )}
                    {openPath && (
                      <button
                        className="jp-airflow-linkbtn"
                        onClick={() => void openInStudio(err)}
                      >
                        {trans.__('Open in Studio to fix')}
                      </button>
                    )}
                    <details className="jp-airflow-ie-trace">
                      <summary>{trans.__('Show technical details')}</summary>
                      <pre>{err.stack_trace ?? trans.__('(no details)')}</pre>
                    </details>
                  </div>
                );
              })}
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
                  {/* "Keep" only dismisses the notice locally, so it stays
                    available to a viewer; the purge does not. */}
                  {canEdit && (
                    <button
                      className="jp-airflow-linkbtn jp-mod-danger"
                      onClick={() => undeployOrphan(o)}
                    >
                      {trans.__('Undeploy & purge')}
                    </button>
                  )}
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

        {/* A rename that never finished: the flow now deploys a new dag_id but
          the old one is still live in Airflow. Deliberately a SEPARATE banner
          from orphans — an orphan's source is gone and purging is the answer,
          whereas here the source is alive and well and the remedy is a
          keep-history retire. Sharing the orphan banner would offer a
          destructive action for a non-destructive problem. */}
        {superseded.length > 0 && (
          <div className="jp-airflow-importerrors jp-mod-warn">
            <button
              className="jp-airflow-importerrors-head"
              onClick={() => setShowSuperseded(s => !s)}
            >
              {showSuperseded ? '▾' : '▸'}{' '}
              {trans.__('Unfinished rename — old dag_id still deployed')} (
              {superseded.length})
            </button>
            {showSuperseded &&
              superseded.map(s => (
                <div key={s.dag_id} className="jp-airflow-orphan">
                  <span className="jp-airflow-orphan-name" title={s.filename}>
                    {s.dag_id} → {s.current_dag_id}
                  </span>
                  {canEdit && (
                    <button
                      className="jp-airflow-linkbtn"
                      onClick={() => retireSuperseded(s)}
                      title={trans.__(
                        'Remove the old DAG and its file. Run history is kept.'
                      )}
                    >
                      {trans.__('Retire (keep history)')}
                    </button>
                  )}
                  <button
                    className="jp-airflow-linkbtn"
                    onClick={() => keepSuperseded(s)}
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

        {dags.length > 0 && (
          <div className="jp-airflow-count">
            {dags.length}{' '}
            {dags.length === 1 ? trans.__('dag') : trans.__('dags')}
          </div>
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
            <LogViewer
              data={logs}
              trans={trans}
              onSelectTry={t =>
                void loadLogs(
                  logs.dagId,
                  logs.runId,
                  logs.taskId,
                  t,
                  logs.maxTry
                )
              }
              onClose={() => setLogs(null)}
            />
          </Overlay>
        )}

        {triggerTarget && (
          <Overlay onClose={() => setTriggerTarget(null)}>
            <TriggerDialog
              dagId={triggerTarget.dagId}
              params={triggerTarget.params}
              trans={trans}
              onClose={() => setTriggerTarget(null)}
              onSubmit={(conf, logicalDate) =>
                runTrigger(triggerTarget.dagId, conf, logicalDate)
              }
            />
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

        {/* The footer states how the panel actually behaves, and now it does
          poll, so it can finally say so. The claim is kept honest: the interval
          is read from the same constant that drives the timer, and the timer
          stands down for a hidden tab or an open dialog — so "Auto-refresh 15s"
          is an upper bound on staleness while you are looking at it, which is
          the only time it matters.
          Refresh sits beside that label rather than in the header: it is the
          manual version of the sentence next to it, so the control and the
          statement read as one thing. */}
        <div className="jp-airflow-footer">
          <button
            className="jp-airflow-iconbtn jp-airflow-footer-refresh"
            title={trans.__('Refresh')}
            aria-label={trans.__('Refresh')}
            onClick={() => void refresh()}
          >
            <refreshIcon.react tag="span" width="14px" height="14px" />
          </button>
          <span>
            {trans.__('Auto-refresh %1s', String(AUTO_REFRESH_MS / 1000))}
          </span>
          <span className="jp-airflow-footer-api">/api/v2</span>
        </div>
      </div>
    </CanEditContext.Provider>
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
  const canEdit = useCanEdit();
  const dagRuns = runs[dag.dag_id];
  const schedule =
    dag.timetable_summary ||
    (typeof dag.schedule_interval === 'string'
      ? dag.schedule_interval
      : dag.schedule_interval?.value) ||
    '—';
  // Derived only from what /dags already returns — no extra request just to
  // colour a dot. An import error outranks paused, because a DAG that cannot
  // be parsed is the more urgent fact about it.
  const dotState = dag.has_import_errors
    ? 'error'
    : dag.is_paused
      ? 'paused'
      : 'active';

  return (
    <li className="jp-airflow-dag">
      <div
        className={
          'jp-airflow-dagrow' + (dag.is_paused ? ' jp-mod-paused' : '')
        }
      >
        {/* Name, state dot and meta are one click target: the whole left side
            expands the row, which is a far bigger hit area than the caret was
            and matches how the list reads — a card you open. */}
        <button
          className="jp-airflow-dagmain"
          onClick={() => props.onToggleDag(dag.dag_id)}
          title={trans.__('Show recent runs')}
          aria-expanded={dag.dag_id in runs}
        >
          <span className="jp-airflow-expand" aria-hidden="true">
            {dag.dag_id in runs ? '▾' : '▸'}
          </span>
          {/* The dot is decorative — `aria-hidden` — because the same state is
              spelled out in the meta line below, so nothing here is conveyed
              by colour alone. */}
          <span
            className={'jp-airflow-dot jp-mod-' + dotState}
            aria-hidden="true"
          />
          <span className="jp-airflow-dagtext">
            <span className="jp-airflow-dagname" title={dag.description ?? ''}>
              {dag.dag_display_name || dag.dag_id}
            </span>
            <span className="jp-airflow-dagmeta">
              {schedule}
              {' · '}
              {dag.is_paused ? trans.__('Paused') : trans.__('Active')}
            </span>
          </span>
        </button>
        {dag.has_import_errors && (
          <span
            className="jp-airflow-badge jp-mod-error"
            title={trans.__('This DAG has an import error')}
          >
            !
          </span>
        )}
        {/* Every row action mutates shared Airflow state, so a viewer gets
            none of them — the row stays fully readable and still expands to
            its runs, tasks and logs. */}
        <div className="jp-airflow-dagactions">
          {!canEdit ? null : (
            <>
              {/* Outline triangle for Resume vs the filled one for Trigger: two
              adjacent play-ish glyphs need a shape difference, not just a
              tooltip, or the destructive-by-surprise click is one slip away. */}
              <button
                className="jp-airflow-iconbtn jp-airflow-rowbtn"
                title={
                  dag.is_paused ? trans.__('Resume DAG') : trans.__('Pause DAG')
                }
                aria-label={
                  dag.is_paused ? trans.__('Resume DAG') : trans.__('Pause DAG')
                }
                aria-pressed={dag.is_paused}
                onClick={() => props.onPause(dag)}
              >
                {dag.is_paused ? '▷' : '❙❙'}
              </button>
              <button
                className="jp-airflow-iconbtn jp-airflow-rowbtn jp-mod-accent"
                title={trans.__('Trigger DAG')}
                aria-label={trans.__('Trigger DAG')}
                onClick={() => props.onTrigger(dag)}
              >
                <runIcon.react tag="span" width="14px" height="14px" />
              </button>
              <button
                className="jp-airflow-iconbtn jp-airflow-rowbtn jp-mod-danger"
                title={trans.__('Delete DAG')}
                aria-label={trans.__('Delete DAG')}
                onClick={() => props.onDelete(dag)}
              >
                🗑
              </button>
            </>
          )}
        </div>
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
                    {canEdit &&
                      (run.state === 'running' || run.state === 'queued') && (
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
                            {/* `logs` above stays — reading a log is a read.
                                `clear` re-runs the task, so it does not. */}
                            {canEdit && (
                              <button
                                className="jp-airflow-linkbtn"
                                onClick={() =>
                                  props.onClear(dag.dag_id, run.dag_run_id, ti)
                                }
                              >
                                {trans.__('clear')}
                              </button>
                            )}
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
  const innerRef = React.useRef<HTMLDivElement>(null);
  const onCloseRef = React.useRef(props.onClose);
  onCloseRef.current = props.onClose;
  // Escape closes the overlay (logs / trigger / confirm), and focus moves into
  // the dialog on open so keyboard users land inside it.
  React.useEffect(() => {
    const onKey = (event: KeyboardEvent): void => {
      if (event.key !== 'Escape' || event.defaultPrevented) {
        return;
      }
      // Don't hijack Escape while the user is editing a field — its native
      // gesture (e.g. clear the log search box) should win, not close the modal.
      const el = document.activeElement;
      const tag = el?.tagName;
      if (
        tag === 'INPUT' ||
        tag === 'TEXTAREA' ||
        tag === 'SELECT' ||
        (el instanceof HTMLElement && el.isContentEditable)
      ) {
        return;
      }
      onCloseRef.current();
    };
    document.addEventListener('keydown', onKey);
    // The inner wrapper is `display:contents` (no box → not focusable), so focus
    // the actual modal element instead. tabindex=-1 makes it programmatically
    // focusable without a :focus-visible ring; Escape then closes it (focus is
    // on the box, not an input).
    const modal =
      innerRef.current?.querySelector<HTMLElement>('.jp-airflow-modal');
    if (modal) {
      if (!modal.hasAttribute('tabindex')) {
        modal.setAttribute('tabindex', '-1');
      }
      modal.focus();
    } else {
      innerRef.current
        ?.querySelector<HTMLElement>('button, input, select, textarea')
        ?.focus();
    }
    return () => document.removeEventListener('keydown', onKey);
  }, []);
  // Portal to <body> so the fixed-position backdrop covers the whole window.
  // Rendered inside the left sidebar it gets trapped by the panel's containing
  // block (lumino widgets establish one via transform/contain), which clips the
  // modal to the narrow rail instead of centring it over the app.
  return createPortal(
    <div className="jp-airflow-overlay" onClick={props.onClose}>
      <div
        ref={innerRef}
        tabIndex={-1}
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
