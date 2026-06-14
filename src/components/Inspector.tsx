import * as React from 'react';

import { AfdagFlowNode, IAfdagNodeData } from '../graph';
import { IAfdagIR } from '../ir';
import { getOperator } from '../operators';

export interface IInspectorProps {
  dag: IAfdagIR['dag'];
  node: AfdagFlowNode | null;
  onDagChange: (patch: Partial<IAfdagIR['dag']>) => void;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
}

/**
 * A minimal scaffold inspector. Later milestones replace the per-node form
 * with a registry-driven JSON-Schema form (RJSF) and add CODE / SAVED tabs.
 */
export function Inspector(props: IInspectorProps): JSX.Element {
  const { dag, node, onDagChange, onNodeChange } = props;
  return (
    <div className="jp-afdag-inspector">
      <div className="jp-afdag-inspector-section">
        <div className="jp-afdag-inspector-title">DAG</div>
        <label className="jp-afdag-field">
          <span>dag_id</span>
          <input
            value={dag.dag_id}
            onChange={event => onDagChange({ dag_id: event.target.value })}
          />
        </label>
        <label className="jp-afdag-field">
          <span>schedule</span>
          <input
            value={dag.schedule ?? ''}
            onChange={event => onDagChange({ schedule: event.target.value })}
          />
        </label>
      </div>
      <div className="jp-afdag-inspector-section">
        <div className="jp-afdag-inspector-title">Node</div>
        {!node && (
          <div className="jp-afdag-hint">Select a node to edit it.</div>
        )}
        {node && <NodeForm node={node} onNodeChange={onNodeChange} />}
      </div>
    </div>
  );
}

interface INodeFormProps {
  node: AfdagFlowNode;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
}

function NodeForm(props: INodeFormProps): JSX.Element {
  const { node, onNodeChange } = props;
  const def = getOperator(node.data.op);

  const setParam = (name: string, value: string): void => {
    onNodeChange(node.id, { params: { ...node.data.params, [name]: value } });
  };

  return (
    <>
      <label className="jp-afdag-field">
        <span>task_id</span>
        <input
          value={node.data.task_id}
          onChange={event =>
            onNodeChange(node.id, { task_id: event.target.value })
          }
        />
      </label>
      {def?.params.map(param => (
        <label key={param.name} className="jp-afdag-field">
          <span>
            {param.label}
            {param.required ? ' *' : ''}
          </span>
          {param.widget === 'textarea' || param.widget === 'code' ? (
            <textarea
              rows={param.widget === 'code' ? 6 : 3}
              value={String(node.data.params[param.name] ?? '')}
              onChange={event => setParam(param.name, event.target.value)}
            />
          ) : (
            <input
              value={String(node.data.params[param.name] ?? '')}
              onChange={event => setParam(param.name, event.target.value)}
            />
          )}
        </label>
      ))}
    </>
  );
}
