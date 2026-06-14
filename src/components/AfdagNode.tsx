import { Handle, Position } from '@xyflow/react';
import type { NodeProps } from '@xyflow/react';
import * as React from 'react';

import type { IAfdagNodeData } from '../graph';
import { getOperator, validateNodeParams } from '../operators';

// A single Airflow task rendered as a ReactFlow node. The validity flag is
// icon + text + ARIA (never colour-only) so it is accessible.
function AfdagNodeImpl(props: NodeProps): JSX.Element {
  const data = props.data as unknown as IAfdagNodeData;
  const def = getOperator(data.op);
  const result = validateNodeParams(data.op, data.params);
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
      <div className="jp-afdag-node-cat">{def?.category ?? 'Unknown'}</div>
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
        {result.valid ? '✓' : '!'}
      </span>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export const AfdagNode = React.memo(AfdagNodeImpl);
