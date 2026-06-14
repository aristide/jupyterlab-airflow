import * as React from 'react';

import { AfdagFlowNode, IAfdagNodeData } from '../graph';
import { IAfdagIR } from '../ir';
import { IStudioServices } from '../services';
import { CodePanel } from './CodePanel';
import { DagTab } from './DagTab';
import { NodeTab } from './NodeTab';
import { SavedTab } from './SavedTab';

export type InspectorTab = 'dag' | 'node' | 'code' | 'saved';

export interface IInspectorProps {
  dag: IAfdagIR['dag'];
  node: AfdagFlowNode | null;
  ir: IAfdagIR;
  services: IStudioServices | null;
  currentPath: string;
  clientErrors: string[];
  /** Bumped on an external IR reload so form tabs reset their local state. */
  reloadKey: number;
  onDagChange: (patch: Partial<IAfdagIR['dag']>) => void;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
}

const TABS: Array<{ id: InspectorTab; label: string }> = [
  { id: 'dag', label: 'DAG' },
  { id: 'node', label: 'NODE' },
  { id: 'code', label: 'CODE' },
  { id: 'saved', label: 'SAVED' }
];

/**
 * The tabbed inspector (PRD §6.1.3): DAG / NODE / CODE / SAVED. Selecting a node
 * on the canvas focuses the NODE tab. Forms are registry-driven RJSF (DAG/NODE);
 * CODE previews the server-generated Python; SAVED lists workspace `.afdag` docs.
 */
export function Inspector(props: IInspectorProps): JSX.Element {
  const [tab, setTab] = React.useState<InspectorTab>('dag');

  // Focus the NODE tab whenever a different node gets selected.
  const lastNodeId = React.useRef<string | null>(null);
  React.useEffect(() => {
    const id = props.node?.id ?? null;
    if (id && id !== lastNodeId.current) {
      setTab('node');
    }
    lastNodeId.current = id;
  }, [props.node]);

  return (
    <div className="jp-afdag-inspector">
      <div className="jp-afdag-tabs" role="tablist">
        {TABS.map(({ id, label }) => (
          <button
            key={id}
            className={
              tab === id ? 'jp-afdag-tab jp-mod-active' : 'jp-afdag-tab'
            }
            role="tab"
            aria-selected={tab === id}
            onClick={() => setTab(id)}
          >
            {label}
          </button>
        ))}
      </div>
      {tab === 'dag' && (
        <DagTab
          key={props.reloadKey}
          dag={props.dag}
          onDagChange={props.onDagChange}
        />
      )}
      {tab === 'node' && (
        <NodeTab node={props.node} onNodeChange={props.onNodeChange} />
      )}
      {tab === 'code' && (
        <CodePanel ir={props.ir} clientErrors={props.clientErrors} />
      )}
      {tab === 'saved' && (
        <SavedTab services={props.services} currentPath={props.currentPath} />
      )}
    </div>
  );
}
