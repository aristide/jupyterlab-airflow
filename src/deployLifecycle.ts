/**
 * "Keep waiting" routing for the deploy banner (PRD §6.5.4).
 *
 * Extracted from `StudioApp` so the one decision that can leave two live DAGs
 * behind is a pure function with a test, rather than a chain of ref checks
 * buried in a callback.
 *
 * There are two completely different ways a deploy can be finished:
 *
 * - **journaled** — the server owns the remaining steps, and the rename's retire
 *   intent lives *only* in the journal (a React ref dies with the tab, which is
 *   the bug the journal exists to fix);
 * - **legacy** — the server did not journal the deploy (the reconciler is off,
 *   or the journal could not be written), so the widget performs the tail itself
 *   from `fallbackRetireRef`.
 *
 * The dangerous move is falling from the first into the second. On the journaled
 * path the widget deliberately holds no retire intent, so re-running the tail
 * here unpauses and triggers the *new* DAG while the renamed-away one is still
 * on disk, still registered and still unpaused — both scheduled, both processing
 * the same data. Hence: as long as we know a journaled deploy id, the plan is
 * always to ask the *server* (observe it, or resume it when its budget expired)
 * and never to poll.
 */
export type KeepWaitingPlan =
  | { kind: 'observe'; deployId: string }
  | { kind: 'resume'; deployId: string }
  | { kind: 'poll'; dagId: string; filename: string }
  | { kind: 'none' };

export interface IKeepWaitingState {
  /** The journaled deploy this tab is still observing (non-terminal). */
  observedDeployId: string | null;
  /** A journaled deploy that went terminal with the `expired` outcome. */
  resumableDeployId: string | null;
  dagId?: string;
  filename?: string;
}

export function keepWaitingPlan(state: IKeepWaitingState): KeepWaitingPlan {
  if (state.observedDeployId) {
    return { kind: 'observe', deployId: state.observedDeployId };
  }
  if (state.resumableDeployId) {
    return { kind: 'resume', deployId: state.resumableDeployId };
  }
  if (state.dagId && state.filename) {
    return { kind: 'poll', dagId: state.dagId, filename: state.filename };
  }
  return { kind: 'none' };
}
