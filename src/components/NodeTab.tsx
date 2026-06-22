import * as React from 'react';

import { formDataToNode, nodeForm, nodeToFormData } from '../forms';
import { AfdagFlowNode, IAfdagNodeData } from '../graph';
import { IOperatorDef } from '../interfaces';
import { AfdagCallbacksValue, IAfdagTaskCallbacks } from '../ir';
import { getOperator } from '../operators';
import { AfdagForm } from './AfdagForm';
import { CallbacksEditor, ICallbackEvent } from './CallbacksEditor';
import { useEditorActions } from './editorContext';

// Per-task lifecycle events (PRD §6.8). `on_retry` is the task-only event the
// DAG level can't express; all three fire in Airflow 3 and the `@task` decorator
// forwards them to the underlying operator.
const TASK_EVENTS: ICallbackEvent[] = [
  { id: 'on_failure', label: 'On failure', hint: 'this task fails' },
  { id: 'on_retry', label: 'On retry', hint: 'this task is about to retry' },
  { id: 'on_success', label: 'On success', hint: 'this task succeeds' }
];

export interface INodeTabProps {
  node: AfdagFlowNode | null;
  /** Bumped on an external IR reload so the keyed children reseed local state. */
  reloadKey: number;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
}

/**
 * NODE tab: the operator-specific form generated from the registry (PRD §6.2),
 * rendered with RJSF. Required-field validation also feeds the canvas error
 * badge via the shared registry (see operators.ts). A nested keyed form holds
 * local state so code/JSON edits aren't reformatted mid-edit; the keys fold in
 * `reloadKey` so an external `.afdag` reload reseeds them (parity with DAG/NOTIFY).
 */
export function NodeTab(props: INodeTabProps): JSX.Element {
  const { node, reloadKey, onNodeChange } = props;
  const { deleteNode } = useEditorActions();
  if (!node) {
    return (
      <div className="jp-afdag-tabpanel">
        <div className="jp-afdag-hint">Select a node to edit it.</div>
      </div>
    );
  }
  const def = getOperator(node.data.op);
  if (!def) {
    return (
      <div className="jp-afdag-tabpanel">
        <div className="jp-afdag-hint">Unknown operator: {node.data.op}</div>
        <button
          className="jp-afdag-btn jp-afdag-btn-danger jp-afdag-node-delete-btn"
          onClick={() => deleteNode(node.id)}
        >
          Delete task
        </button>
      </div>
    );
  }
  return (
    <div className="jp-afdag-tabpanel">
      <NodeForm
        key={`${reloadKey}:${node.id}`}
        node={node}
        def={def}
        onNodeChange={onNodeChange}
      />
      <NodeCallbacksSection
        key={`${reloadKey}:cb:${node.id}`}
        node={node}
        onNodeChange={onNodeChange}
      />
      <button
        className="jp-afdag-btn jp-afdag-btn-danger jp-afdag-node-delete-btn"
        title="Remove this task and its connections"
        onClick={() => deleteNode(node.id)}
      >
        Delete task
      </button>
    </div>
  );
}

/**
 * Per-task notification callbacks (PRD §6.8): a "Notifications" section inside the
 * NODE tab (parallel to "Common settings") that attaches notifiers to this task's
 * lifecycle events — including the task-only `on_retry`. Reuses the shared
 * {@link CallbacksEditor}; commits to `node.data.callbacks` via `onNodeChange`.
 * Keyed by node id so its local state reseeds when a different node is selected.
 */
function NodeCallbacksSection(props: {
  node: AfdagFlowNode;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
}): JSX.Element {
  const { node, onNodeChange } = props;
  return (
    <div className="jp-afdag-node-callbacks">
      <h3 className="jp-afdag-node-section-title">Notifications</h3>
      <CallbacksEditor
        events={TASK_EVENTS}
        value={node.data.callbacks as AfdagCallbacksValue | undefined}
        onChange={next =>
          onNodeChange(node.id, {
            callbacks: next as IAfdagTaskCallbacks | undefined
          })
        }
        intro="Alert a channel when this task reaches an event — e.g. Slack on failure or retry. Runs as a per-task Airflow callback, not a graph task."
      />
    </div>
  );
}

interface INodeFormProps {
  node: AfdagFlowNode;
  def: IOperatorDef;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
}

function NodeForm(props: INodeFormProps): JSX.Element {
  const { node, def, onNodeChange } = props;
  const { schema, uiSchema } = React.useMemo(() => nodeForm(def), [def]);
  const [formData, setFormData] = React.useState(() =>
    nodeToFormData(def, node.data.task_id, node.data.params, node.data.common)
  );

  const handleChange = React.useCallback(
    (next: Record<string, unknown>) => {
      setFormData(next);
      const { task_id, params, common } = formDataToNode(def, next);
      onNodeChange(node.id, { task_id, params, common });
    },
    [def, node.id, onNodeChange]
  );

  return (
    <AfdagForm
      schema={schema}
      uiSchema={uiSchema}
      formData={formData}
      onChange={handleChange}
    />
  );
}
