import { Handle, Position } from '@xyflow/react';
import type { NodeProps } from '@xyflow/react';
import * as React from 'react';

import type { IAfdagNodeData } from '../graph';
import { getOperator, validateNodeParams } from '../operators';
import { useEditorActions } from './editorContext';

// Operator category -> the `--d4n-cat-*` accent tokens defined in
// style/afdag.css, which colour the category eyebrow so operator families are
// scannable across the canvas. Several registry categories deliberately share
// an accent (Python/Bash and Kubernetes read as SQL's navy, Governance as Flow
// Control's slate) — that grouping comes from the design, not from laziness.
// Anything absent here renders with `--d4n-cat-default`.
const CATEGORY_ACCENT: Record<string, string> = {
  Sensors: 'sensors',
  Storage: 'storage',
  'Data Quality': 'quality',
  SQL: 'sql',
  'Python/Bash': 'sql',
  Kubernetes: 'sql',
  Notifications: 'notifications',
  Compute: 'compute',
  Ingestion: 'ingestion',
  'Flow Control': 'flow',
  Governance: 'flow'
};

// A single Airflow task rendered as a ReactFlow node. The validity flag is
// icon + text + ARIA (never colour-only) so it is accessible.
function AfdagNodeImpl(props: NodeProps): JSX.Element {
  const data = props.data as unknown as IAfdagNodeData;
  const def = getOperator(data.op);
  const result = validateNodeParams(data.op, data.params);
  const { deleteNode } = useEditorActions();
  const className = [
    'jp-afdag-node',
    props.selected ? 'jp-mod-selected' : '',
    result.valid ? '' : 'jp-mod-invalid'
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <div className={className}>
      <Handle type="target" position={Position.Left} />
      <button
        className="jp-afdag-node-del nodrag nopan"
        title="Delete task"
        aria-label={`Delete task ${data.task_id}`}
        onClick={event => {
          event.stopPropagation();
          deleteNode(props.id);
        }}
      >
        ×
      </button>
      <div
        className="jp-afdag-node-cat"
        data-cat={def ? CATEGORY_ACCENT[def.category] : undefined}
      >
        {def?.category ?? 'Unknown'}
      </div>
      <div className="jp-afdag-node-label">{def?.label ?? data.op}</div>
      <code className="jp-afdag-node-taskid">{data.task_id}</code>
      <span
        className={
          result.valid
            ? 'jp-afdag-node-flag jp-mod-ok'
            : 'jp-afdag-node-flag jp-mod-error'
        }
        title={result.valid ? 'Valid' : `Missing: ${result.missing.join(', ')}`}
        aria-label={
          result.valid
            ? 'Node valid'
            : `Missing required fields: ${result.missing.join(', ')}`
        }
      >
        {result.valid
          ? '✓ ready'
          : `✕ ${result.missing.length} ${
              result.missing.length === 1 ? 'error' : 'errors'
            }`}
      </span>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export const AfdagNode = React.memo(AfdagNodeImpl);
