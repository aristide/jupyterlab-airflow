import * as React from 'react';

import { IImportError } from '../interfaces';

export type DeployPhase =
  | 'idle'
  | 'writing'
  | 'waiting'
  | 'registered'
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
  importError?: IImportError;
}

export interface IDeployBannerProps {
  state: IDeployState;
  onDismiss: () => void;
  onUnpauseTrigger: () => void;
  onKeepWaiting: () => void;
}

const MOD: Record<DeployPhase, string> = {
  idle: '',
  writing: 'jp-mod-busy',
  waiting: 'jp-mod-busy',
  processing: 'jp-mod-warn',
  registered: 'jp-mod-ok',
  failed: 'jp-mod-error',
  error: 'jp-mod-error'
};

/**
 * The deploy lifecycle banner (PRD §6.5.4): Writing → Waiting → Registered /
 * Failed-to-import / Still-processing. Purely presentational; StudioApp drives
 * the state machine and polling.
 */
export function DeployBanner(props: IDeployBannerProps): JSX.Element | null {
  const { state, onDismiss, onUnpauseTrigger, onKeepWaiting } = props;
  if (state.phase === 'idle') {
    return null;
  }

  const busy = state.phase === 'writing' || state.phase === 'waiting';

  return (
    <div className={`jp-afdag-deploybanner ${MOD[state.phase]}`} role="status">
      <span className="jp-afdag-deploybanner-msg">
        {busy && <span className="jp-afdag-spinner" aria-hidden="true" />}
        {state.message}
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
