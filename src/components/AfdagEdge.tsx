import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath } from '@xyflow/react';
import type { EdgeProps } from '@xyflow/react';
import * as React from 'react';

import { useEditorActions } from './editorContext';

// Rounded-corner orthogonal dependency edge (PRD §6.1.1): a smoothstep path with
// an arrowhead, plus a delete (×) control at its midpoint shown when the edge is
// selected. Clicking the connector selects it (ReactFlow default), which both
// highlights it and reveals the delete button; the same selection lets the
// keyboard `Delete`/`Backspace` remove it.
const BORDER_RADIUS = 8;

function AfdagEdgeImpl(props: EdgeProps): JSX.Element {
  const {
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    markerEnd,
    selected
  } = props;
  const { deleteEdge } = useEditorActions();

  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: BORDER_RADIUS
  });

  return (
    <>
      <BaseEdge id={id} path={edgePath} markerEnd={markerEnd} />
      {selected && (
        <EdgeLabelRenderer>
          <button
            className="jp-afdag-edge-del nodrag nopan"
            style={{
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`
            }}
            title="Delete connection"
            aria-label="Delete connection"
            onClick={event => {
              event.stopPropagation();
              deleteEdge(id);
            }}
          >
            ×
          </button>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

export const AfdagEdge = React.memo(AfdagEdgeImpl);
