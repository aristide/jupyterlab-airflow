import * as React from 'react';

import { AfdagFlowNode, IAfdagNodeData } from '../graph';
import { IAfdagIR } from '../ir';
import { IStudioServices } from '../services';
import { CodePanel } from './CodePanel';
import { DagTab } from './DagTab';
import { InfoTab } from './InfoTab';
import { NodeTab } from './NodeTab';
import { SavedTab } from './SavedTab';

export type InspectorTab = 'dag' | 'node' | 'info' | 'code' | 'saved';

export interface IInspectorProps {
  dag: IAfdagIR['dag'];
  node: AfdagFlowNode | null;
  ir: IAfdagIR;
  services: IStudioServices | null;
  currentPath: string;
  clientErrors: string[];
  /** Bumped on an external IR reload so form tabs reset their local state. */
  reloadKey: number;
  /** Whether the panel is collapsed to a rail (canvas reclaims the width). */
  collapsed: boolean;
  /** Toggle the collapsed state. */
  onToggle: () => void;
  onDagChange: (patch: Partial<IAfdagIR['dag']>) => void;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
}

const TABS: Array<{ id: InspectorTab; label: string }> = [
  { id: 'dag', label: 'DAG' },
  { id: 'node', label: 'NODE' },
  { id: 'info', label: 'INFO' },
  { id: 'code', label: 'CODE' },
  { id: 'saved', label: 'SAVED' }
];

/**
 * The tabbed inspector (PRD §6.1.3): DAG / NODE / INFO / CODE / SAVED. Selecting
 * a node focuses the NODE tab; INFO sits beside it with read-only learning
 * content about the selected operator. Forms are registry-driven RJSF (DAG/NODE);
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

  if (props.collapsed) {
    return (
      <div className="jp-afdag-inspector jp-mod-collapsed">
        <button
          className="jp-afdag-collapse-btn"
          title="Expand inspector"
          aria-label="Expand inspector panel"
          aria-expanded={false}
          onClick={props.onToggle}
        >
          «
        </button>
        <div className="jp-afdag-rail-label">Inspector</div>
      </div>
    );
  }

  return (
    <div className="jp-afdag-inspector">
      <div className="jp-afdag-inspector-head">
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
        <button
          className="jp-afdag-collapse-btn"
          title="Collapse inspector"
          aria-label="Collapse inspector panel"
          aria-expanded={true}
          onClick={props.onToggle}
        >
          »
        </button>
      </div>
      {tab === 'dag' && (
        <DagTab
          key={`${props.reloadKey}:${props.dag.dag_id}`}
          dag={props.dag}
          onDagChange={props.onDagChange}
        />
      )}
      {tab === 'node' && (
        <NodeTab node={props.node} onNodeChange={props.onNodeChange} />
      )}
      {tab === 'info' && <InfoTab node={props.node} />}
      {tab === 'code' && (
        <CodePanel ir={props.ir} clientErrors={props.clientErrors} />
      )}
      {tab === 'saved' && (
        <SavedTab services={props.services} currentPath={props.currentPath} />
      )}
    </div>
  );
}
