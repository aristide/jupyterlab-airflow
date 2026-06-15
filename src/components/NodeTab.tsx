import * as React from 'react';

import { formDataToNode, nodeForm, nodeToFormData } from '../forms';
import { AfdagFlowNode, IAfdagNodeData } from '../graph';
import { IOperatorDef } from '../interfaces';
import { getOperator } from '../operators';
import { AfdagForm } from './AfdagForm';
import { useEditorActions } from './editorContext';

export interface INodeTabProps {
  node: AfdagFlowNode | null;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
}

/**
 * NODE tab: the operator-specific form generated from the registry (PRD §6.2),
 * rendered with RJSF. Required-field validation also feeds the canvas error
 * badge via the shared registry (see operators.ts). A nested keyed form holds
 * local state so code/JSON edits aren't reformatted mid-edit.
 */
export function NodeTab(props: INodeTabProps): JSX.Element {
  const { node, onNodeChange } = props;
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
        key={node.id}
        node={node}
        def={def}
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

interface INodeFormProps {
  node: AfdagFlowNode;
  def: IOperatorDef;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
}

function NodeForm(props: INodeFormProps): JSX.Element {
  const { node, def, onNodeChange } = props;
  const { schema, uiSchema } = React.useMemo(() => nodeForm(def), [def]);
  const [formData, setFormData] = React.useState(() =>
    nodeToFormData(def, node.data.task_id, node.data.params)
  );

  const handleChange = React.useCallback(
    (next: Record<string, unknown>) => {
      setFormData(next);
      const { task_id, params } = formDataToNode(def, next);
      onNodeChange(node.id, { task_id, params });
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
