import * as React from 'react';

import { AfdagCallbacksValue, IAfdagCallbacks, IAfdagIR } from '../ir';
import { CallbacksEditor, ICallbackEvent } from './CallbacksEditor';

export interface INotificationsTabProps {
  dag: IAfdagIR['dag'];
  onDagChange: (patch: Partial<IAfdagIR['dag']>) => void;
}

// DAG-level lifecycle events that still fire in Airflow 3 (SLAs were removed in
// 3.0); `on_retry` is task-level (see the NODE-tab per-task callbacks section).
const DAG_EVENTS: ICallbackEvent[] = [
  { id: 'on_failure', label: 'On failure', hint: 'the DAG run fails' },
  { id: 'on_success', label: 'On success', hint: 'the DAG run succeeds' }
];

/**
 * Notifications tab (PRD §6.8 / §15.14): attach notifiers (email / Slack / …) to
 * DAG lifecycle events. Notifiers run as Airflow callbacks — not graph tasks — so
 * they live on `dag.callbacks`, not in `nodes[]`. Thin wrapper over the shared
 * {@link CallbacksEditor} (the per-task NODE-tab section reuses the same editor).
 */
export function NotificationsTab(props: INotificationsTabProps): JSX.Element {
  const { dag, onDagChange } = props;
  return (
    <div className="jp-afdag-tabpanel">
      <CallbacksEditor
        events={DAG_EVENTS}
        value={dag.callbacks as AfdagCallbacksValue | undefined}
        onChange={next =>
          onDagChange({ callbacks: next as IAfdagCallbacks | undefined })
        }
        intro="Alert a channel when this DAG reaches an event — e.g. email or Slack on failure. Notifiers run as Airflow callbacks, not graph tasks."
      />
    </div>
  );
}
