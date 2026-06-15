import { DocumentRegistry } from '@jupyterlab/docregistry';
import { UUID } from '@lumino/coreutils';
import { ISignal } from '@lumino/signaling';
import {
  Background,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  addEdge,
  useEdgesState,
  useNodesState
} from '@xyflow/react';
import type {
  Connection,
  Edge,
  NodeTypes,
  ReactFlowInstance
} from '@xyflow/react';
import * as React from 'react';

import {
  AfdagFlowNode,
  IAfdagNodeData,
  flowToIR,
  hasCycle,
  irToFlow
} from '../graph';
import { deployDag } from '../handler';
import { IOperatorDef } from '../interfaces';
import { IAfdagIR, createEmptyIR, dagIdFromPath, stringifyIR } from '../ir';
import { AfdagModel } from '../model';
import {
  getOperator,
  getOperators,
  loadOperators,
  validateNodeParams
} from '../operators';
import { IStudioServices } from '../services';
import { AfdagNode } from './AfdagNode';
import { Inspector } from './Inspector';
import { Palette } from './Palette';

// Custom node types must be a stable, module-scope object or ReactFlow
// re-renders endlessly.
const nodeTypes: NodeTypes = { afdagNode: AfdagNode };

export interface IStudioAppProps {
  context: DocumentRegistry.IContext<AfdagModel>;
  resized: ISignal<unknown, void>;
  services?: IStudioServices | null;
}

export function StudioApp(props: IStudioAppProps): JSX.Element {
  const { context, resized } = props;
  const services = props.services ?? null;
  const model = context.model as AfdagModel;

  const [ready, setReady] = React.useState(false);
  const [operators, setOperators] =
    React.useState<IOperatorDef[]>(getOperators);
  const [opsLoaded, setOpsLoaded] = React.useState(false);
  const [opsError, setOpsError] = React.useState<string | null>(null);
  const [nodes, setNodes, onNodesChange] = useNodesState<AfdagFlowNode>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [dag, setDag] = React.useState<IAfdagIR['dag']>(
    () => createEmptyIR('').dag
  );
  const [selectedId, setSelectedId] = React.useState<string | null>(null);
  const [reloadKey, setReloadKey] = React.useState(0);
  const [deploy, setDeploy] = React.useState<{
    status: 'idle' | 'busy' | 'ok' | 'error';
    message: string;
  }>({ status: 'idle', message: '' });

  const baseRef = React.useRef<IAfdagIR>(createEmptyIR(''));
  const lastWritten = React.useRef<string>('');
  const loadingRef = React.useRef<boolean>(false);
  const rfRef = React.useRef<ReactFlowInstance<AfdagFlowNode, Edge> | null>(
    null
  );

  // Fetch the operator registry (GET operators) once at activation. The palette
  // and node forms are generated from it; getOperator/validateNodeParams read
  // the cached index synchronously once this resolves.
  React.useEffect(() => {
    let cancelled = false;
    loadOperators()
      .then(list => {
        if (!cancelled) {
          setOperators(list);
          setOpsLoaded(true);
        }
      })
      .catch(error => {
        if (!cancelled) {
          setOpsError(String((error && error.message) || error));
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Load the IR from the document model, and reload on external changes.
  React.useEffect(() => {
    let disconnected = false;

    const load = (): void => {
      const text = model.toString();
      let ir: IAfdagIR;
      let canonical: string;
      if (!text.trim()) {
        // A brand-new untitled document: seed it so Save persists a real DAG.
        ir = createEmptyIR(dagIdFromPath(context.path));
        canonical = stringifyIR(ir);
        model.setIR(ir);
      } else {
        try {
          ir = JSON.parse(text) as IAfdagIR;
        } catch {
          ir = createEmptyIR(dagIdFromPath(context.path));
        }
        canonical = text;
      }
      baseRef.current = ir;
      lastWritten.current = canonical;
      loadingRef.current = true;
      const flow = irToFlow(ir);
      setNodes(flow.nodes);
      setEdges(flow.edges);
      setDag(ir.dag);
      setReady(true);
      // Remount the form tabs so they reseed local state from the new IR.
      setReloadKey(key => key + 1);
    };

    const onChanged = (): void => {
      if (!disconnected && model.toString() !== lastWritten.current) {
        load();
      }
    };

    void context.ready.then(() => {
      if (disconnected) {
        return;
      }
      load();
      model.contentChanged.connect(onChanged);
    });

    return () => {
      disconnected = true;
      model.contentChanged.disconnect(onChanged);
    };
  }, [context, model, setNodes, setEdges]);

  // Persist the IR back to the model whenever the graph or DAG config changes.
  React.useEffect(() => {
    if (!ready) {
      return;
    }
    if (loadingRef.current) {
      loadingRef.current = false;
      return;
    }
    const ir = flowToIR(nodes, edges, dag, baseRef.current);
    const next = stringifyIR(ir);
    if (next !== lastWritten.current) {
      lastWritten.current = next;
      model.setIR(ir);
    }
  }, [nodes, edges, dag, ready, model]);

  // Re-fit the canvas when the Lumino widget is shown or resized.
  React.useEffect(() => {
    const refit = (): void => {
      rfRef.current?.fitView();
    };
    resized.connect(refit);
    return () => {
      resized.disconnect(refit);
    };
  }, [resized]);

  const onConnect = React.useCallback(
    (connection: Connection): void => {
      if (connection.source === connection.target) {
        return;
      }
      setEdges(eds =>
        addEdge(
          { ...connection, markerEnd: { type: MarkerType.ArrowClosed } },
          eds
        )
      );
    },
    [setEdges]
  );

  const addNode = React.useCallback(
    (opId: string): void => {
      const def = getOperator(opId);
      if (!def) {
        return;
      }
      setNodes(nds => {
        const taskId = uniqueTaskId(def.taskIdPrefix, nds);
        const node: AfdagFlowNode = {
          id: UUID.uuid4(),
          type: 'afdagNode',
          position: {
            x: 60 + (nds.length % 4) * 220,
            y: 60 + Math.floor(nds.length / 4) * 130
          },
          data: { op: opId, task_id: taskId, params: {} }
        };
        return nds.concat(node);
      });
    },
    [setNodes]
  );

  const updateNode = React.useCallback(
    (id: string, patch: Partial<IAfdagNodeData>): void => {
      setNodes(nds =>
        nds.map(n =>
          n.id === id ? { ...n, data: { ...n.data, ...patch } } : n
        )
      );
    },
    [setNodes]
  );

  const errorCount = React.useMemo(() => {
    let count = 0;
    for (const node of nodes) {
      if (!validateNodeParams(node.data.op, node.data.params).valid) {
        count += 1;
      }
    }
    if (hasCycle(nodes, edges)) {
      count += 1;
    }
    return count;
  }, [nodes, edges]);

  const selected = nodes.find(n => n.id === selectedId) ?? null;

  // The IR projected from the live graph, fed to the CODE preview.
  const currentIR = React.useMemo(
    () => flowToIR(nodes, edges, dag, baseRef.current),
    [nodes, edges, dag]
  );

  // Instant, client-side validation messages for the CODE tab's panel.
  const clientErrors = React.useMemo(() => {
    const messages: string[] = [];
    if (hasCycle(nodes, edges)) {
      messages.push(
        'DAG contains a cycle — Airflow does not support cyclic dependencies.'
      );
    }
    for (const node of nodes) {
      const result = validateNodeParams(node.data.op, node.data.params);
      if (!result.valid) {
        messages.push(
          `Task "${node.data.task_id}" is missing: ${result.missing.join(', ')}`
        );
      }
    }
    return messages;
  }, [nodes, edges]);

  // Deploy: server validates (full pipeline) then atomically writes the .py.
  const onDeploy = React.useCallback(async (): Promise<void> => {
    setDeploy({ status: 'busy', message: 'Deploying…' });
    const res = await deployDag(currentIR);
    if (res.status === 'OK' && res.data?.deployed) {
      setDeploy({
        status: 'ok',
        message: `Deployed ${res.data.filename}${
          res.data.warnings.length ? ' (Airflow will validate on import)' : ''
        }`
      });
    } else {
      const detail =
        res.data?.errors?.join('; ') || res.error || 'Deploy failed';
      setDeploy({ status: 'error', message: detail });
    }
  }, [currentIR]);

  if (opsError) {
    return (
      <div className="jp-afdag-loading jp-mod-error">
        Could not load the operator registry: {opsError}
      </div>
    );
  }

  if (!ready || !opsLoaded) {
    return <div className="jp-afdag-loading">Loading…</div>;
  }

  return (
    <div className="jp-afdag-root">
      <div className="jp-afdag-topbar">
        <span className="jp-afdag-brand">Airflow Studio</span>
        <span className="jp-afdag-dagid">{dag.dag_id || 'untitled'}</span>
        <span className="jp-afdag-count">
          {nodes.length} {nodes.length === 1 ? 'node' : 'nodes'}
        </span>
        <span
          className={
            errorCount
              ? 'jp-afdag-errors jp-mod-error'
              : 'jp-afdag-errors jp-mod-ok'
          }
        >
          {errorCount
            ? `✕ ${errorCount} ${errorCount === 1 ? 'error' : 'errors'}`
            : '✓ no errors'}
        </span>
        {deploy.status !== 'idle' && (
          <span
            className={
              deploy.status === 'error'
                ? 'jp-afdag-deploy-status jp-mod-error'
                : deploy.status === 'ok'
                  ? 'jp-afdag-deploy-status jp-mod-ok'
                  : 'jp-afdag-deploy-status'
            }
            title={deploy.message}
          >
            {deploy.message}
          </span>
        )}
        <span className="jp-afdag-spacer" />
        <button
          className="jp-afdag-btn"
          title="Save (.afdag)"
          onClick={() => void context.save()}
        >
          Save
        </button>
        <button
          className="jp-afdag-btn jp-afdag-btn-primary"
          title={
            errorCount
              ? 'Fix validation errors before deploying'
              : 'Validate and deploy the DAG to Airflow'
          }
          disabled={
            deploy.status === 'busy' || errorCount > 0 || nodes.length === 0
          }
          onClick={() => void onDeploy()}
        >
          {deploy.status === 'busy' ? 'Deploying…' : 'Deploy'}
        </button>
      </div>
      <div className="jp-afdag-body">
        <Palette operators={operators} onAdd={addNode} />
        <div className="jp-afdag-canvas">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onInit={instance => {
              rfRef.current = instance;
              instance.fitView();
            }}
            onNodeClick={(_, node) => setSelectedId(node.id)}
            onPaneClick={() => setSelectedId(null)}
            fitView
            proOptions={{ hideAttribution: true }}
          >
            <Background />
            <MiniMap pannable zoomable />
            <Controls />
          </ReactFlow>
          {nodes.length === 0 && (
            <div className="jp-afdag-empty">
              Add operators from the left panel to get started.
            </div>
          )}
        </div>
        <Inspector
          dag={dag}
          node={selected}
          ir={currentIR}
          services={services}
          currentPath={context.path}
          clientErrors={clientErrors}
          reloadKey={reloadKey}
          onDagChange={patch => setDag(d => ({ ...d, ...patch }))}
          onNodeChange={updateNode}
        />
      </div>
    </div>
  );
}

function uniqueTaskId(prefix: string, nodes: AfdagFlowNode[]): string {
  const used = new Set(nodes.map(n => n.data.task_id));
  let index = 1;
  let candidate = `${prefix}_${index}`;
  while (used.has(candidate)) {
    index += 1;
    candidate = `${prefix}_${index}`;
  }
  return candidate;
}
