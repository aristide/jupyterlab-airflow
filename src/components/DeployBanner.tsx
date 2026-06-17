import * as React from 'react';

import { IImportError } from '../interfaces';

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
  importError?: IImportError;
}

export interface IDeployBannerProps {
  state: IDeployState;
  onDismiss: () => void;
  onUnpauseTrigger: () => void;
  onStopRun: () => void;
  onKeepWaiting: () => void;
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
  const { state, onDismiss, onUnpauseTrigger, onStopRun, onKeepWaiting } =
    props;
  if (state.phase === 'idle') {
    return null;
  }

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
          <button
            className="jp-afdag-btn jp-afdag-btn-ghost"
            onClick={onDismiss}
          >
            Dismiss
          </button>
        </span>
      )}

      {(state.phase === 'waiting' ||
        state.phase === 'failed' ||
        state.phase === 'error') && (
        <span className="jp-afdag-deploybanner-actions">
          <button
            className="jp-afdag-btn jp-afdag-btn-ghost"
            onClick={onDismiss}
          >
            Dismiss
          </button>
        </span>
      )}

      {state.phase === 'failed' && state.importError?.stack_trace && (
        <details className="jp-afdag-deploybanner-trace">
          <summary>Traceback</summary>
          <pre>{state.importError.stack_trace}</pre>
        </details>
      )}
    </div>
  );
}
