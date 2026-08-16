import * as React from 'react';

import { AfdagFlowNode, IAfdagNodeData } from '../graph';
import { IAfdagConnection, IAfdagIR, IAfdagVariable } from '../ir';
import { IStudioServices } from '../services';
import { CodePanel } from './CodePanel';
import { ConnectionsTab } from './ConnectionsTab';
import { DagTab } from './DagTab';
import { InfoTab } from './InfoTab';
import { NodeTab } from './NodeTab';
import { NotificationsTab } from './NotificationsTab';
import { SavedTab } from './SavedTab';
import { VariablesTab } from './VariablesTab';

export type InspectorTab =
  | 'dag'
  | 'node'
  | 'info'
  | 'vars'
  | 'conns'
  | 'notify'
  | 'code'
  | 'saved';

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
  /** Bumped by the editor to request focus on the CODE tab (the syntax toggle
   * does this, so switching TaskFlow/Traditional shows what it produced). A
   * counter, not a tab value, so a repeated request re-fires after the user
   * has navigated away. */
  focusCodeNonce?: number;
  onDagChange: (patch: Partial<IAfdagIR['dag']>) => void;
  onNodeChange: (id: string, patch: Partial<IAfdagNodeData>) => void;
  /** Replace the flow's variable declarations (PRD §6.10). Separate from
   * `onDagChange` because variables live on the IR root, not on `ir.dag`. */
  onVariablesChange: (next: IAfdagVariable[]) => void;
  /** Replace the flow's connection declarations (PRD §6.11). Like variables,
   * these live on the IR root rather than on `ir.dag`. */
  onConnectionsChange: (next: IAfdagConnection[]) => void;
}

const TABS: Array<{ id: InspectorTab; label: string }> = [
  { id: 'dag', label: 'DAG' },
  { id: 'node', label: 'NODE' },
  { id: 'info', label: 'INFO' },
  { id: 'vars', label: 'VARS' },
  { id: 'conns', label: 'CONNS' },
  { id: 'notify', label: 'NOTIFY' },
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

  // Focus CODE when the editor asks (the syntax toggle). Compared against the
  // previous nonce rather than keyed on mount, so opening a document does not
  // yank the user off the DAG tab.
  const lastCodeFocus = React.useRef(props.focusCodeNonce ?? 0);
  React.useEffect(() => {
    const nonce = props.focusCodeNonce ?? 0;
    if (nonce !== lastCodeFocus.current) {
      lastCodeFocus.current = nonce;
      setTab('code');
    }
  }, [props.focusCodeNonce]);

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
        <NodeTab
          node={props.node}
          reloadKey={props.reloadKey}
          onNodeChange={props.onNodeChange}
          variables={props.ir.variables}
        />
      )}
      {tab === 'info' && <InfoTab node={props.node} />}
      {tab === 'vars' && (
        <VariablesTab
          ir={props.ir}
          variables={props.ir.variables ?? []}
          onVariablesChange={props.onVariablesChange}
        />
      )}
      {tab === 'conns' && (
        <ConnectionsTab
          ir={props.ir}
          connections={props.ir.connections ?? []}
          onConnectionsChange={props.onConnectionsChange}
        />
      )}
      {tab === 'notify' && (
        <NotificationsTab
          key={`${props.reloadKey}:${props.dag.dag_id}`}
          dag={props.dag}
          onDagChange={props.onDagChange}
        />
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
