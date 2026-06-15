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
import { deployDag, deployStatus, setDagPaused, triggerDag } from '../handler';
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
import { DeployBanner, IDeployState } from './DeployBanner';
import { Inspector } from './Inspector';
import { Palette } from './Palette';

// Deploy poll cadence: a few minutes total, backing off from 2s to 8s. Airflow
// re-parses on min_file_process_interval (~30s) so sub-second polling is wasteful.
const POLL_TIMEOUT_MS = 180000;
const POLL_START_MS = 2000;
const POLL_MAX_MS = 8000;

const sleep = (ms: number): Promise<void> =>
  new Promise(resolve => window.setTimeout(resolve, ms));

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
  const [deploy, setDeploy] = React.useState<IDeployState>({
    phase: 'idle',
    message: ''
  });

  const baseRef = React.useRef<IAfdagIR>(createEmptyIR(''));
  const lastWritten = React.useRef<string>('');
  const loadingRef = React.useRef<boolean>(false);
  const rfRef = React.useRef<ReactFlowInstance<AfdagFlowNode, Edge> | null>(
    null
  );
  // Cancellation token for the in-flight deploy poll loop.
  const pollRef = React.useRef<{ cancelled: boolean } | null>(null);

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

  // Stop any in-flight poll loop (dismiss / unmount / re-deploy).
  const cancelPoll = React.useCallback((): void => {
    if (pollRef.current) {
      pollRef.current.cancelled = true;
      pollRef.current = null;
    }
  }, []);

  // Phase 2-3: poll deploy/status with bounded backoff until the DAG registers,
  // fails to import, or we time out (→ "still processing").
  const pollLifecycle = React.useCallback(
    async (dagId: string, filename: string): Promise<void> => {
      const token = { cancelled: false };
      pollRef.current = token;
      const deadline = Date.now() + POLL_TIMEOUT_MS;
      let delay = POLL_START_MS;

      while (!token.cancelled && Date.now() < deadline) {
        await sleep(delay);
        if (token.cancelled) {
          return;
        }
        const res = await deployStatus(dagId, filename);
        if (token.cancelled) {
          return;
        }
        if (res.status === 'OK' && res.data) {
          if (res.data.state === 'registered') {
            setDeploy({
              phase: 'registered',
              dagId,
              filename,
              isPaused: res.data.dag?.is_paused ?? true,
              message: `Registered ${dagId} (paused).`
            });
            return;
          }
          if (res.data.state === 'failed') {
            setDeploy({
              phase: 'failed',
              dagId,
              filename,
              importError: res.data.import_error,
              message: `${filename} failed to import.`
            });
            return;
          }
        }
        delay = Math.min(delay + 1000, POLL_MAX_MS);
      }

      if (!token.cancelled) {
        setDeploy({
          phase: 'processing',
          dagId,
          filename,
          message:
            'Still processing — Airflow has not picked up the file yet. ' +
            'This can take a few minutes.'
        });
      }
    },
    []
  );

  // Phase 1: validate + atomic write, then enter the polling lifecycle.
  const onDeploy = React.useCallback(async (): Promise<void> => {
    cancelPoll();
    setDeploy({ phase: 'writing', message: 'Writing the DAG file…' });
    const res = await deployDag(currentIR);
    if (res.status !== 'OK' || !res.data?.deployed) {
      const detail =
        res.data?.errors?.join('; ') || res.error || 'Deploy failed';
      setDeploy({ phase: 'error', message: detail });
      return;
    }
    const { dag_id: dagId, filename = '' } = res.data;
    setDeploy({
      phase: 'waiting',
      dagId,
      filename,
      message: 'Waiting for Airflow to pick it up… (up to a few minutes)'
    });
    void pollLifecycle(dagId, filename);
  }, [currentIR, cancelPoll, pollLifecycle]);

  const onDismissDeploy = React.useCallback((): void => {
    cancelPoll();
    setDeploy({ phase: 'idle', message: '' });
  }, [cancelPoll]);

  const onKeepWaiting = React.useCallback((): void => {
    if (deploy.dagId && deploy.filename) {
      setDeploy({
        phase: 'waiting',
        dagId: deploy.dagId,
        filename: deploy.filename,
        message: 'Waiting for Airflow to pick it up…'
      });
      void pollLifecycle(deploy.dagId, deploy.filename);
    }
  }, [deploy.dagId, deploy.filename, pollLifecycle]);

  const onUnpauseTrigger = React.useCallback(async (): Promise<void> => {
    const dagId = deploy.dagId;
    if (!dagId) {
      return;
    }
    await setDagPaused(dagId, false);
    const run = await triggerDag(dagId);
    setDeploy(prev => ({
      ...prev,
      isPaused: false,
      triggered: true,
      message:
        run.status === 'OK'
          ? `Unpaused and triggered ${dagId}.`
          : `Unpaused ${dagId}, but the trigger failed: ${run.error ?? ''}`
    }));
  }, [deploy.dagId]);

  // Cancel any poll loop if the editor unmounts.
  React.useEffect(() => cancelPoll, [cancelPoll]);

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
            deploy.phase === 'writing' ||
            deploy.phase === 'waiting' ||
            errorCount > 0 ||
            nodes.length === 0
          }
          onClick={() => void onDeploy()}
        >
          {deploy.phase === 'writing' || deploy.phase === 'waiting'
            ? 'Deploying…'
            : 'Deploy'}
        </button>
      </div>
      <DeployBanner
        state={deploy}
        onDismiss={onDismissDeploy}
        onUnpauseTrigger={() => void onUnpauseTrigger()}
        onKeepWaiting={onKeepWaiting}
      />
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
