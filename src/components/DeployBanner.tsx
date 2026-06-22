import * as React from 'react';

import { IImportError } from '../interfaces';
import { IExplainedError } from '../importErrors';

export type DeployPhase =
  | 'idle'
  | 'writing'
  | 'waiting'
  | 'registered'
  | 'running'
  | 'finished'
  | 'failed'
  | 'processing'
  | 'error';

export interface IDeployState {
  phase: DeployPhase;
  message: string;
  filename?: string;
  dagId?: string;
  isPaused?: boolean;
  triggered?: boolean;
  /** The run-on-deploy run id (PRD §6.5.4) — drives the run poll + Stop. */
  runId?: string;
  /** Latest observed state of that run (`running`/`success`/`failed`/…). */
  runState?: string;
  /** Secondary line kept across the run (e.g. a rename "old DAG retired"). */
  note?: string;
  /** A prior deployed version was backed up, so a rollback is available (§7). */
  backedUp?: boolean;
  importError?: IImportError;
}

export interface IDeployBannerProps {
  state: IDeployState;
  /** Plain-language translation of a failed import + the offending task (§7). */
  explanation?: IExplainedError;
  onDismiss: () => void;
  onUnpauseTrigger: () => void;
  onStopRun: () => void;
  onKeepWaiting: () => void;
  /** Remove the deployed DAG (file + history) — PRD §7. */
  onUndeploy: () => void;
  /** Restore the previous deployed version — PRD §6.5.5 / §7. */
  onRollback: () => void;
}

const MOD: Record<DeployPhase, string> = {
  idle: '',
  writing: 'jp-mod-busy',
  waiting: 'jp-mod-busy',
  running: 'jp-mod-busy',
  finished: 'jp-mod-ok',
  processing: 'jp-mod-warn',
  registered: 'jp-mod-ok',
  failed: 'jp-mod-error',
  error: 'jp-mod-error'
};

/**
 * The deploy lifecycle banner (PRD §6.5.4): Writing → Waiting → Registered →
 * (run-on-deploy) Running → Finished, plus Failed-to-import / Still-processing.
 * Purely presentational; StudioApp drives the state machine and polling.
 */
export function DeployBanner(props: IDeployBannerProps): JSX.Element | null {
  const {
    state,
    explanation,
    onDismiss,
    onUnpauseTrigger,
    onStopRun,
    onKeepWaiting,
    onUndeploy,
    onRollback
  } = props;
  if (state.phase === 'idle') {
    return null;
  }

  // The DAG is on disk in Airflow in these phases, so Undeploy is offered.
  const undeployBtn = (
    <button className="jp-afdag-btn" onClick={onUndeploy}>
      Undeploy
    </button>
  );

  const busy =
    state.phase === 'writing' ||
    state.phase === 'waiting' ||
    state.phase === 'running';
  // A finished run that didn't succeed colours the banner as an error.
  const mod =
    state.phase === 'finished' && state.runState && state.runState !== 'success'
      ? 'jp-mod-error'
      : MOD[state.phase];

  return (
    <div className={`jp-afdag-deploybanner ${mod}`} role="status">
      <span className="jp-afdag-deploybanner-msg">
        {busy && <span className="jp-afdag-spinner" aria-hidden="true" />}
        {state.message}
        {state.note && (
          <span className="jp-afdag-deploybanner-note">{state.note}</span>
        )}
      </span>

      {state.phase === 'registered' && (
        <span className="jp-afdag-deploybanner-actions">
          {!state.triggered && (
            <button className="jp-afdag-btn" onClick={onUnpauseTrigger}>
              Unpause &amp; trigger
            </button>
          )}
          {undeployBtn}
          <button
            className="jp-afdag-btn jp-afdag-btn-ghost"
            onClick={onDismiss}
          >
            Dismiss
          </button>
        </span>
      )}

      {state.phase === 'running' && (
        <span className="jp-afdag-deploybanner-actions">
          <button
            className="jp-afdag-btn jp-afdag-btn-danger"
            onClick={onStopRun}
          >
            ⏹ Stop run
          </button>
          <button
            className="jp-afdag-btn jp-afdag-btn-ghost"
            onClick={onDismiss}
          >
            Dismiss
          </button>
        </span>
      )}

      {state.phase === 'finished' && (
        <span className="jp-afdag-deploybanner-actions">
          <button className="jp-afdag-btn" onClick={onUnpauseTrigger}>
            ▶ Run again
          </button>
          {undeployBtn}
          <button
            className="jp-afdag-btn jp-afdag-btn-ghost"
            onClick={onDismiss}
          >
            Dismiss
          </button>
        </span>
      )}

      {state.phase === 'processing' && (
        <span className="jp-afdag-deploybanner-actions">
          <button className="jp-afdag-btn" onClick={onKeepWaiting}>
            Keep waiting
          </button>
          {undeployBtn}
          <button
            className="jp-afdag-btn jp-afdag-btn-ghost"
            onClick={onDismiss}
          >
            Dismiss
          </button>
        </span>
      )}

      {state.phase === 'failed' && (
        <span className="jp-afdag-deploybanner-actions">
          {state.backedUp && (
            <button className="jp-afdag-btn" onClick={onRollback}>
              ↩ Roll back to previous
            </button>
          )}
          {undeployBtn}
          <button
            className="jp-afdag-btn jp-afdag-btn-ghost"
            onClick={onDismiss}
          >
            Dismiss
          </button>
        </span>
      )}

      {(state.phase === 'waiting' || state.phase === 'error') && (
        <span className="jp-afdag-deploybanner-actions">
          <button
            className="jp-afdag-btn jp-afdag-btn-ghost"
            onClick={onDismiss}
          >
            Dismiss
          </button>
        </span>
      )}

      {state.phase === 'failed' && explanation && (
        <div className="jp-afdag-deploybanner-explain">
          <div className="jp-afdag-deploybanner-explain-title">
            {explanation.title}
          </div>
          <div>{explanation.summary}</div>
          {explanation.nodeTaskId && (
            <div className="jp-afdag-deploybanner-explain-node">
              ⚠ Check the <strong>{explanation.nodeTaskId}</strong> task.
            </div>
          )}
          {explanation.hint && (
            <div className="jp-afdag-deploybanner-explain-hint">
              {explanation.hint}
            </div>
          )}
        </div>
      )}

      {state.phase === 'failed' && state.importError?.stack_trace && (
        <details className="jp-afdag-deploybanner-trace">
          <summary>Show technical details</summary>
          <pre>{state.importError.stack_trace}</pre>
        </details>
      )}
    </div>
  );
}
