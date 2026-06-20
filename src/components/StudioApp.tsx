import {
  Dialog,
  InputDialog,
  showDialog,
  showErrorMessage
} from '@jupyterlab/apputils';
import { DocumentRegistry } from '@jupyterlab/docregistry';
import { UUID } from '@lumino/coreutils';
import { ISignal } from '@lumino/signaling';
import {
  Background,
  ConnectionLineType,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  addEdge,
  reconnectEdge,
  useEdgesState,
  useNodesState
} from '@xyflow/react';
import type {
  Connection,
  Edge,
  EdgeTypes,
  NodeTypes,
  ReactFlowInstance
} from '@xyflow/react';
import * as React from 'react';

import {
  AFDAG_EDGE_TYPE,
  AFDAG_NOTE_TYPE,
  AfdagFlowNode,
  DEFAULT_NOTE_SIZE,
  IAfdagNodeData,
  NOTE_OP,
  canConnect,
  flowToIR,
  hasCycle,
  irToFlow,
  isNoteNode
} from '../graph';
import {
  deleteDag,
  deployDag,
  deployStatus,
  getDagRun,
  renamePreflight,
  retireOldDag,
  rollbackDag,
  setDagPaused,
  setDagRunState,
  triggerDag
} from '../handler';
import { IOperatorDef } from '../interfaces';
import {
  IAfdagIR,
  SyntaxStyle,
  createEmptyIR,
  dagIdFromPath,
  normalizeAfdagFilename,
  stringifyIR,
  validateDagId
} from '../ir';
import { AfdagModel } from '../model';
import {
  getOperator,
  getOperators,
  loadOperators,
  validateNodeParams
} from '../operators';
import { IStudioServices } from '../services';
import { AfdagEdge } from './AfdagEdge';
import { AfdagNode } from './AfdagNode';
import { DeployBanner, IDeployState } from './DeployBanner';
import { EditorActionsContext, IEditorActions } from './editorContext';
import { Inspector } from './Inspector';
import { NoteNode } from './NoteNode';
import { Palette } from './Palette';

// Deploy poll cadence: a few minutes total, backing off from 2s to 8s. Airflow
// re-parses on min_file_process_interval (~30s) so sub-second polling is wasteful.
const POLL_TIMEOUT_MS = 180000;
const POLL_START_MS = 2000;
const POLL_MAX_MS = 8000;
// Run-on-deploy (§6.5.4): a deployed run is polled to completion, with a longer
// ceiling than registration since a DAG run can legitimately take a while.
const RUN_POLL_TIMEOUT_MS = 600000;
// Give up the run poll after this many consecutive errors (e.g. the run/DAG was
// removed out of band → 404, or Airflow is unreachable) instead of spinning a
// stale "Running…" banner until the deadline.
const MAX_RUN_POLL_ERRORS = 5;
const RUN_TERMINAL_STATES = new Set([
  'success',
  'failed',
  'skipped',
  'upstream_failed',
  'removed'
]);

const sleep = (ms: number): Promise<void> =>
  new Promise(resolve => window.setTimeout(resolve, ms));

// Custom node/edge types must be a stable, module-scope object or ReactFlow
// re-renders endlessly.
const nodeTypes: NodeTypes = {
  afdagNode: AfdagNode,
  [AFDAG_NOTE_TYPE]: NoteNode
};
const edgeTypes: EdgeTypes = { [AFDAG_EDGE_TYPE]: AfdagEdge };

// Applied to every edge (loaded, drawn, or reconnected): a rounded-corner
// smoothstep arrow that can be grabbed by either endpoint to rewire it.
const defaultEdgeOptions = {
  type: AFDAG_EDGE_TYPE,
  reconnectable: true,
  markerEnd: { type: MarkerType.ArrowClosed }
};

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
  const [leftCollapsed, setLeftCollapsed] = React.useState(false);
  const [rightCollapsed, setRightCollapsed] = React.useState(false);
  // The generated-code syntax family (PRD §6.3). Lives in the IR; the toggle
  // persists it and the CODE preview / Deploy regenerate accordingly.
  const [syntaxStyle, setSyntaxStyle] = React.useState<SyntaxStyle>('taskflow');
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
  // While a node drag is in progress we hold off committing the IR (ReactFlow
  // fires a position change every frame); the latest graph is read on drag-stop.
  const draggingRef = React.useRef<boolean>(false);
  const latestRef = React.useRef({ nodes, edges, dag });
  latestRef.current = { nodes, edges, dag };
  // Latest selection, read by onNodesDelete (a stable callback) without
  // re-subscribing on every selection change.
  const selectedIdRef = React.useRef<string | null>(selectedId);
  selectedIdRef.current = selectedId;

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

  // Re-fetch the registry, forcing the server to re-read the target Airflow's
  // installed providers (PRD §6.2.1) — so installing a provider then refreshing
  // un-dims its operators without restarting the editor. A refresh failure is
  // surfaced non-destructively (the last-good operators stay; the editor is
  // never torn down) — it must NOT route through the fatal opsError path.
  const refreshOperators = React.useCallback((): void => {
    loadOperators(true)
      .then(list => setOperators(list))
      .catch(
        error =>
          void showErrorMessage(
            'Could not refresh operators',
            String((error && error.message) || error)
          )
      );
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
      setSyntaxStyle(ir.syntax_style ?? 'taskflow');
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

  // Serialize the latest graph into the model, but only when it actually
  // changed (the compare also skips selection-only churn).
  const commit = React.useCallback((): void => {
    const { nodes, edges, dag } = latestRef.current;
    const ir = flowToIR(nodes, edges, dag, baseRef.current);
    const next = stringifyIR(ir);
    if (next !== lastWritten.current) {
      lastWritten.current = next;
      model.setIR(ir);
    }
  }, [model]);

  // Persist the IR back to the model whenever the graph or DAG config changes —
  // except mid-drag, where the commit is deferred to onNodeDragStop so a drag is
  // one model write rather than one per frame.
  React.useEffect(() => {
    if (!ready) {
      return;
    }
    if (loadingRef.current) {
      loadingRef.current = false;
      return;
    }
    if (draggingRef.current) {
      return;
    }
    commit();
  }, [nodes, edges, dag, ready, commit]);

  const onNodeDragStart = React.useCallback((): void => {
    draggingRef.current = true;
  }, []);

  const onNodeDragStop = React.useCallback((): void => {
    draggingRef.current = false;
    commit();
  }, [commit]);

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

  const toggleLeft = React.useCallback(
    () => setLeftCollapsed(collapsed => !collapsed),
    []
  );
  const toggleRight = React.useCallback(
    () => setRightCollapsed(collapsed => !collapsed),
    []
  );

  // Re-fit the canvas after a side panel collapses/expands. The width change is
  // internal — the Lumino widget itself doesn't resize, so the `resized` signal
  // never fires — so nudge fitView once the CSS width transition has settled.
  React.useEffect(() => {
    const timer = window.setTimeout(() => {
      rfRef.current?.fitView();
    }, 160);
    return () => window.clearTimeout(timer);
  }, [leftCollapsed, rightCollapsed]);

  const onConnect = React.useCallback(
    (connection: Connection): void => {
      if (
        !canConnect(
          connection.source,
          connection.target,
          latestRef.current.edges
        )
      ) {
        return;
      }
      setEdges(eds => addEdge({ ...connection, ...defaultEdgeOptions }, eds));
    },
    [setEdges]
  );

  // Shared connect/reconnect guard (no self-loops, no duplicate edges).
  const isValidConnection = React.useCallback(
    (connection: Connection | Edge): boolean =>
      canConnect(connection.source, connection.target, latestRef.current.edges),
    []
  );

  // Drag an edge endpoint onto a different node to rewire the dependency. An
  // invalid or empty drop is rejected by isValidConnection and the edge snaps
  // back unchanged — deletion stays explicit (× button / Delete key).
  const onReconnect = React.useCallback(
    (oldEdge: Edge, newConnection: Connection): void => {
      setEdges(eds => reconnectEdge(oldEdge, newConnection, eds));
    },
    [setEdges]
  );

  // Remove a task node and its incident edges (× button / NODE-tab path).
  const deleteNode = React.useCallback(
    (id: string): void => {
      setNodes(nds => nds.filter(node => node.id !== id));
      setEdges(eds =>
        eds.filter(edge => edge.source !== id && edge.target !== id)
      );
      setSelectedId(current => (current === id ? null : current));
    },
    [setNodes, setEdges]
  );

  // Remove a single dependency edge, leaving both nodes (on-edge × button).
  const deleteEdge = React.useCallback(
    (id: string): void => {
      setEdges(eds => eds.filter(edge => edge.id !== id));
    },
    [setEdges]
  );

  // Update an annotation note card's text (inline textarea edit).
  const updateNoteText = React.useCallback(
    (id: string, text: string): void => {
      setNodes(nds =>
        nds.map(node =>
          node.id === id ? { ...node, data: { ...node.data, text } } : node
        )
      );
    },
    [setNodes]
  );

  // Clear the inspector selection when the selected node is removed via the
  // keyboard (ReactFlow's built-in Delete path runs through onNodesChange).
  const onNodesDelete = React.useCallback((deleted: AfdagFlowNode[]): void => {
    if (deleted.some(node => node.id === selectedIdRef.current)) {
      setSelectedId(null);
    }
  }, []);

  const editorActions = React.useMemo<IEditorActions>(
    () => ({ deleteNode, deleteEdge, updateNoteText }),
    [deleteNode, deleteEdge, updateNoteText]
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

  // Add an annotation note card (PRD §6.1.7). It shares the ReactFlow `nodes`
  // array with task nodes but is tagged `noteNode` + a marker op, so flowToIR
  // splits it into the IR's separate `notes[]` (never reaching codegen).
  const addNote = React.useCallback((): void => {
    setNodes(nds => {
      const offset = nds.filter(isNoteNode).length * 24;
      const note: AfdagFlowNode = {
        id: UUID.uuid4(),
        type: AFDAG_NOTE_TYPE,
        position: { x: 100 + offset, y: 100 + offset },
        width: DEFAULT_NOTE_SIZE.width,
        height: DEFAULT_NOTE_SIZE.height,
        data: { op: NOTE_OP, task_id: '', params: {}, text: '' }
      };
      return nds.concat(note);
    });
  }, [setNodes]);

  // The Airflow-task nodes only (note cards are excluded from validation, the
  // error badge, the node count, and inspector selection).
  const taskNodes = React.useMemo(
    () => nodes.filter(n => !isNoteNode(n)),
    [nodes]
  );

  const errorCount = React.useMemo(() => {
    let count = 0;
    for (const node of taskNodes) {
      if (!validateNodeParams(node.data.op, node.data.params).valid) {
        count += 1;
      }
    }
    if (hasCycle(taskNodes, edges)) {
      count += 1;
    }
    return count;
  }, [taskNodes, edges]);

  const selected = taskNodes.find(n => n.id === selectedId) ?? null;

  // The IR projected from the live graph, fed to the CODE preview.
  const currentIR = React.useMemo(
    () => flowToIR(nodes, edges, dag, baseRef.current),
    [nodes, edges, dag, syntaxStyle]
  );

  // Switch the codegen syntax family (PRD §6.3): update the base IR (which
  // flowToIR threads through), re-render the CODE preview, and persist so the
  // `.afdag` and the next Deploy use the new style.
  const onToggleSyntax = React.useCallback(
    (next: SyntaxStyle): void => {
      if (next === baseRef.current.syntax_style) {
        return;
      }
      baseRef.current = { ...baseRef.current, syntax_style: next };
      setSyntaxStyle(next);
      commit();
    },
    [commit]
  );

  // Instant, client-side validation messages for the CODE tab's panel.
  const clientErrors = React.useMemo(() => {
    const messages: string[] = [];
    if (hasCycle(taskNodes, edges)) {
      messages.push(
        'DAG contains a cycle — Airflow does not support cyclic dependencies.'
      );
    }
    for (const node of taskNodes) {
      const result = validateNodeParams(node.data.op, node.data.params);
      if (!result.valid) {
        messages.push(
          `Task "${node.data.task_id}" is missing: ${result.missing.join(', ')}`
        );
      }
    }
    return messages;
  }, [taskNodes, edges]);

  // A queued "retire the old DAG" step for a dag_id rename migration (§6.1.8(B)):
  // set just before deploying the renamed DAG; run once it reaches `registered`.
  const pendingRetireRef = React.useRef<{
    oldDagId: string;
    purge: boolean;
  } | null>(null);

  // Stop any in-flight poll loop (dismiss / unmount / re-deploy).
  const cancelPoll = React.useCallback((): void => {
    if (pollRef.current) {
      pollRef.current.cancelled = true;
      pollRef.current = null;
    }
  }, []);

  // Run-on-deploy (§6.5.4): poll the triggered run until it reaches a terminal
  // state, driving the banner running → finished. Shares the poll cancel token.
  const pollRunState = React.useCallback(
    async (
      dagId: string,
      filename: string,
      runId: string,
      token: { cancelled: boolean }
    ): Promise<void> => {
      const deadline = Date.now() + RUN_POLL_TIMEOUT_MS;
      let delay = POLL_START_MS;
      let errors = 0;
      while (!token.cancelled && Date.now() < deadline) {
        await sleep(delay);
        if (token.cancelled) {
          return;
        }
        const res = await getDagRun(dagId, runId);
        if (token.cancelled) {
          return;
        }
        if (res.status === 'OK' && res.data) {
          errors = 0;
          const runState = res.data.state;
          if (RUN_TERMINAL_STATES.has(runState)) {
            setDeploy(prev => ({
              ...prev,
              phase: 'finished',
              dagId,
              filename,
              runId,
              runState,
              message:
                runState === 'success'
                  ? `Run finished — ${dagId} · ✓ success`
                  : `Run finished — ${dagId} · ${runState}`
            }));
            return;
          }
          setDeploy(prev => ({
            ...prev,
            phase: 'running',
            runState,
            message: `Running ${dagId} — ${runState}…`
          }));
        } else if (++errors >= MAX_RUN_POLL_ERRORS) {
          // The run/DAG likely vanished (404) or Airflow is unreachable — stop
          // polling and steer the user to the Manager instead of a stale spinner.
          setDeploy(prev => ({
            ...prev,
            phase: 'finished',
            dagId,
            filename,
            runId,
            runState: 'unknown',
            message: `Lost track of ${dagId}'s run (${res.error ?? 'poll failed'}). Check the Manager.`
          }));
          return;
        }
        delay = Math.min(delay + 1000, POLL_MAX_MS);
      }
      if (!token.cancelled) {
        setDeploy(prev => ({
          ...prev,
          message: `${dagId} is still running — check the Manager for progress.`
        }));
      }
    },
    []
  );

  // Run-on-deploy core: unpause THEN trigger (a run on a paused DAG just sits
  // queued, §8.8), then poll the run to completion. Reused by the banner's
  // "Run again" / "Unpause & trigger" fallback. Guarded by the poll token so a
  // dismiss / re-deploy / unmount cancels it.
  const runAfterDeploy = React.useCallback(
    async (
      dagId: string,
      filename: string,
      token: { cancelled: boolean },
      // Optional secondary line kept visible across the run (e.g. a rename
      // migration's "Old DAG retired/purged" confirmation, §6.1.8(B)).
      note?: string
    ): Promise<void> => {
      setDeploy({
        phase: 'registered',
        dagId,
        filename,
        note,
        triggered: false,
        message: `Registered ${dagId} — unpausing & triggering…`
      });
      await setDagPaused(dagId, false);
      if (token.cancelled) {
        return;
      }
      const run = await triggerDag(dagId);
      if (token.cancelled) {
        return;
      }
      if (run.status !== 'OK' || !run.data?.dag_run_id) {
        setDeploy({
          phase: 'registered',
          dagId,
          filename,
          note,
          isPaused: false,
          triggered: false,
          message: `Unpaused ${dagId}, but the run trigger failed: ${run.error ?? 'unknown error'}. Use “Unpause & trigger” to retry.`
        });
        return;
      }
      const runId = run.data.dag_run_id;
      setDeploy({
        phase: 'running',
        dagId,
        filename,
        note,
        runId,
        runState: run.data.state,
        triggered: true,
        message: `Running ${dagId}…`
      });
      await pollRunState(dagId, filename, runId, token);
    },
    [pollRunState]
  );

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
            // Rename migration: the renamed DAG is live → retire the old one
            // before running the new one.
            let retireNote: string | undefined;
            const pending = pendingRetireRef.current;
            if (pending) {
              pendingRetireRef.current = null;
              const retired = await retireOldDag(
                pending.oldDagId,
                pending.purge
              );
              if (token.cancelled) {
                return;
              }
              if (retired.status !== 'OK') {
                setDeploy({
                  phase: 'registered',
                  dagId,
                  filename,
                  triggered: false,
                  message: `Renamed to ${dagId}, but retiring “${pending.oldDagId}” failed: ${retired.error ?? 'unknown error'}.`
                });
                return;
              }
              retireNote = `Renamed to ${dagId}. Old DAG “${pending.oldDagId}” ${pending.purge ? 'purged' : 'retired — history kept'}.`;
            }
            // Run on deploy (§6.5.4): every deploy unpauses + triggers a run.
            await runAfterDeploy(dagId, filename, token, retireNote);
            return;
          }
          if (res.data.state === 'failed') {
            // A migration's new DAG failed to import → leave the old one intact.
            pendingRetireRef.current = null;
            // Functional update preserves `backedUp` (set on the prior waiting
            // state) so the failed banner can offer Roll back (§7).
            setDeploy(prev => ({
              ...prev,
              phase: 'failed',
              dagId,
              filename,
              importError: res.data?.import_error,
              message: `${filename} failed to import.`
            }));
            return;
          }
        }
        delay = Math.min(delay + 1000, POLL_MAX_MS);
      }

      if (!token.cancelled) {
        // Timed out before the renamed DAG registered → don't retire the old.
        pendingRetireRef.current = null;
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
    [runAfterDeploy]
  );

  // Phase 1: validate + atomic write, then enter the polling lifecycle. Takes an
  // explicit IR so a rename migration can deploy the renamed DAG (§6.1.8(B)).
  const runDeploy = React.useCallback(
    async (ir: IAfdagIR): Promise<void> => {
      cancelPoll();
      setDeploy({ phase: 'writing', message: 'Writing the DAG file…' });
      const res = await deployDag(ir);
      if (res.status !== 'OK' || !res.data?.deployed) {
        const detail =
          res.data?.errors?.join('; ') || res.error || 'Deploy failed';
        // A failed (re)deploy aborts any pending rename migration.
        pendingRetireRef.current = null;
        setDeploy({ phase: 'error', message: detail });
        return;
      }
      const { dag_id: dagId, filename = '' } = res.data;
      setDeploy({
        phase: 'waiting',
        dagId,
        filename,
        // Carried into a later `failed` so the banner can offer Roll back (§7).
        backedUp: res.data.backed_up,
        message: 'Waiting for Airflow to pick it up… (up to a few minutes)'
      });
      void pollLifecycle(dagId, filename);
    },
    [cancelPoll, pollLifecycle]
  );

  // The Deploy button. A plain re-deploy overwrites the same {dag_id}.py, so if
  // the DAG is already registered with a run in flight we guard first: Airflow's
  // LocalDagBundle has no versioning and runs the latest file on disk (§8.8), so
  // overwriting mid-run can corrupt the in-flight run. Same active-run check the
  // rename migration uses (§6.1.8(B)); preflight failure falls through to deploy.
  const onDeploy = React.useCallback(async (): Promise<void> => {
    pendingRetireRef.current = null; // a plain deploy never retires another DAG
    const dagId = currentIR.dag.dag_id;
    const pf = await renamePreflight(dagId); // shared dag-state preflight
    if (pf.status === 'OK' && pf.data?.registered && pf.data.active_runs > 0) {
      const override = await showDialog({
        title: 'A run is in progress',
        body:
          `“${dagId}” has ${pf.data.active_runs} run(s) in progress. Re-deploying ` +
          'overwrites the DAG file while it runs — Airflow runs the latest file ' +
          'on disk, so the in-flight run can break. Wait for it to finish, or ' +
          'deploy anyway.',
        buttons: [
          Dialog.cancelButton({ label: 'Cancel' }),
          Dialog.warnButton({ label: 'Deploy anyway' })
        ]
      });
      if (!override.button.accept) {
        return;
      }
    }
    // Out-of-band drift (§6.5.3): the deployed file was hand-edited since Studio
    // wrote it — re-deploying discards those manual edits.
    if (pf.status === 'OK' && pf.data?.drifted) {
      const overwrite = await showDialog({
        title: 'Modified outside Studio',
        body:
          `“${dagId}” was edited directly in the dags folder since Studio last ` +
          'deployed it. Deploying overwrites those manual edits with the current ' +
          'graph. Overwrite, or cancel and reconcile by hand?',
        buttons: [
          Dialog.cancelButton({ label: 'Cancel' }),
          Dialog.warnButton({ label: 'Overwrite' })
        ]
      });
      if (!overwrite.button.accept) {
        return;
      }
    }
    void runDeploy(currentIR);
  }, [currentIR, runDeploy]);

  // Rename the .afdag DOCUMENT (file). Filesystem-only: it does NOT change the
  // dag_id or affect any deployed/running pipeline (PRD §6.1.8(A)). Changing the
  // dag_id is the separate, deploy-aware migration (§6.1.8(B), follow-up).
  const onRename = React.useCallback(async (): Promise<void> => {
    const base = context.path.split('/').pop() ?? context.path;
    const result = await InputDialog.getText({
      title: 'Rename DAG file',
      text: base,
      // Pre-select the stem so the user edits the name but keeps `.afdag`.
      selectionRange: base.replace(/\.afdag$/i, '').length,
      okLabel: 'Rename'
    });
    if (!result.button.accept) {
      return;
    }
    const normalized = normalizeAfdagFilename(result.value ?? '');
    if ('error' in normalized) {
      void showErrorMessage('Rename failed', normalized.error);
      return;
    }
    if (normalized.name === base) {
      return;
    }
    try {
      await context.rename(normalized.name);
    } catch (err) {
      void showErrorMessage('Rename failed', String(err));
    }
  }, [context]);

  // Change the dag_id (PRD §6.1.8(B)): a deploy-aware migration. Airflow has no
  // rename — a new id is a NEW DAG with no history — so for a deployed DAG we
  // deploy the renamed DAG, then retire the old one (keep history or purge), and
  // we block while a run is in flight. A draft just sets the id.
  const onRenameDagId = React.useCallback(async (): Promise<void> => {
    const current = dag.dag_id;
    const entry = await InputDialog.getText({
      title: 'Rename DAG id',
      text: current,
      okLabel: 'Continue'
    });
    if (!entry.button.accept) {
      return;
    }
    const checked = validateDagId(entry.value ?? '');
    if ('error' in checked) {
      void showErrorMessage('Rename failed', checked.error);
      return;
    }
    const next = checked.id;
    if (next === current) {
      return;
    }

    const pf = await renamePreflight(current);
    if (pf.status !== 'OK' || !pf.data) {
      void showErrorMessage(
        'Rename failed',
        pf.error ?? 'Could not check the current DAG state.'
      );
      return;
    }
    const { file_exists, registered, active_runs } = pf.data;

    // Draft (nothing deployed): just set the id — no migration.
    if (!file_exists && !registered) {
      setDag(d => ({ ...d, dag_id: next }));
      return;
    }

    // A run is in progress → block, with an explicit override.
    if (active_runs > 0) {
      const override = await showDialog({
        title: 'A run is in progress',
        body:
          `“${current}” has ${active_runs} run(s) in progress. Renaming creates ` +
          'a new DAG and removes the old file, which would strand the in-flight ' +
          'run (Airflow runs the latest file on disk). Wait for it to finish, or ' +
          'override and lose it.',
        buttons: [
          Dialog.cancelButton({ label: 'Cancel' }),
          Dialog.warnButton({ label: 'Override (lose run)' })
        ]
      });
      if (!override.button.accept) {
        return;
      }
    }

    // Deployed (idle, or overridden): choose what happens to the old DAG.
    const choice = await showDialog({
      title: 'Rename & redeploy',
      body:
        `Airflow has no rename — this creates a NEW DAG “${next}” (paused, empty ` +
        `history). The old “${current}” history does NOT carry over. Keep the ` +
        'old DAG’s history (paused) or purge it?',
      buttons: [
        Dialog.cancelButton({ label: 'Cancel' }),
        Dialog.okButton({ label: 'Keep history' }),
        Dialog.warnButton({ label: 'Purge old DAG' })
      ]
    });
    if (!choice.button.accept) {
      return;
    }
    const purge = choice.button.label === 'Purge old DAG';

    // Migrate: set the new id in the editor (persisted by the commit effect),
    // then deploy it; pollLifecycle retires the old DAG once the new registers.
    const newIR: IAfdagIR = {
      ...currentIR,
      dag: { ...currentIR.dag, dag_id: next }
    };
    setDag(d => ({ ...d, dag_id: next }));
    pendingRetireRef.current = { oldDagId: current, purge };
    void runDeploy(newIR);
  }, [dag.dag_id, currentIR, runDeploy]);

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

  // Banner "Unpause & trigger" (fallback) / "Run again" (after a finished run):
  // re-run the same unpause→trigger→poll flow under a fresh cancel token.
  const onUnpauseTrigger = React.useCallback((): void => {
    const { dagId, filename } = deploy;
    if (!dagId || !filename) {
      return;
    }
    cancelPoll();
    const token = { cancelled: false };
    pollRef.current = token;
    void runAfterDeploy(dagId, filename, token);
  }, [deploy.dagId, deploy.filename, cancelPoll, runAfterDeploy]);

  // Stop the in-flight run (§6.6): Airflow has no cancel, so PATCH the run to
  // `failed`. Then (re)start the run poll under a fresh token so the banner
  // converges to "finished" — the prior poll may have ended (e.g. the run-poll
  // timeout fired), in which case nothing would otherwise re-observe the run.
  const onStopRun = React.useCallback(async (): Promise<void> => {
    const { dagId, filename, runId } = deploy;
    if (!dagId || !runId) {
      return;
    }
    const res = await setDagRunState(dagId, runId, 'failed');
    if (res.status !== 'OK') {
      setDeploy(prev => ({
        ...prev,
        message: `Stop failed: ${res.error ?? 'unknown error'}`
      }));
      return;
    }
    cancelPoll();
    const token = { cancelled: false };
    pollRef.current = token;
    setDeploy(prev => ({
      ...prev,
      phase: 'running',
      message: `Stopping ${dagId}…`
    }));
    if (filename) {
      void pollRunState(dagId, filename, runId, token);
    }
  }, [deploy.dagId, deploy.filename, deploy.runId, cancelPoll, pollRunState]);

  // Undeploy the open DAG from the editor (PRD §7): the same teardown as the
  // manager's Delete — remove the deployed `.py` + purge run history. The
  // `.afdag` design file stays, so the DAG can be re-deployed.
  const onUndeploy = React.useCallback(async (): Promise<void> => {
    const dagId = deploy.dagId;
    if (!dagId) {
      return;
    }
    const confirmed = await showDialog({
      title: 'Undeploy this DAG?',
      body:
        `Remove “${dagId}” from Airflow? This deletes the deployed .py and ` +
        'purges its run history. The .afdag design stays in your workspace, so ' +
        'you can deploy it again.',
      buttons: [
        Dialog.cancelButton({ label: 'Cancel' }),
        Dialog.warnButton({ label: 'Undeploy' })
      ]
    });
    if (!confirmed.button.accept) {
      return;
    }
    cancelPoll();
    setDeploy(prev => ({
      ...prev,
      phase: 'writing',
      message: `Undeploying ${dagId}…`
    }));
    const res = await deleteDag(dagId);
    if (res.status !== 'OK') {
      setDeploy(prev => ({
        ...prev,
        phase: 'error',
        message: `Undeploy failed: ${res.error ?? 'unknown error'}`
      }));
      return;
    }
    setDeploy({ phase: 'idle', message: '' });
  }, [deploy.dagId, cancelPoll]);

  // Roll the deployed DAG back to its previous version (PRD §6.5.5 / §7) — the
  // recovery path when a re-deploy broke the import. The restored file
  // re-imports, so re-enter the deploy lifecycle.
  const onRollback = React.useCallback(async (): Promise<void> => {
    const { dagId, filename } = deploy;
    if (!dagId || !filename) {
      return;
    }
    cancelPoll();
    setDeploy(prev => ({
      ...prev,
      phase: 'writing',
      message: `Rolling ${dagId} back to the previous version…`
    }));
    const res = await rollbackDag(dagId);
    if (res.status !== 'OK' || !res.data?.rolled_back) {
      setDeploy(prev => ({
        ...prev,
        phase: 'failed',
        backedUp: false,
        message:
          res.status === 'OK' && res.data && !res.data.rolled_back
            ? 'No previous version to roll back to.'
            : `Rollback failed: ${res.error ?? 'unknown error'}`
      }));
      return;
    }
    setDeploy({
      phase: 'waiting',
      dagId,
      filename,
      message:
        'Rolled back — waiting for Airflow to reload the previous version…'
    });
    void pollLifecycle(dagId, filename);
  }, [deploy.dagId, deploy.filename, cancelPoll, pollLifecycle]);

  // Cancel any poll loop if the editor unmounts.
  React.useEffect(() => cancelPoll, [cancelPoll]);

  // Only a *first-load* registry failure is fatal (there's no editor to show
  // yet). A later failure (e.g. a palette refresh blip) must never tear down a
  // working editor — it's surfaced non-destructively instead (refreshOperators).
  if (opsError && !opsLoaded) {
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
    <EditorActionsContext.Provider value={editorActions}>
      <div className="jp-afdag-root">
        <div className="jp-afdag-topbar">
          <span className="jp-afdag-brand">Airflow Studio</span>
          <span className="jp-afdag-dagid">{dag.dag_id || 'untitled'}</span>
          <span className="jp-afdag-count">
            {taskNodes.length} {taskNodes.length === 1 ? 'node' : 'nodes'}
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
          <div
            className="jp-afdag-syntax-toggle"
            role="group"
            aria-label="Generated code syntax"
          >
            <button
              className={
                syntaxStyle === 'taskflow'
                  ? 'jp-afdag-syntax-opt jp-mod-active'
                  : 'jp-afdag-syntax-opt'
              }
              aria-pressed={syntaxStyle === 'taskflow'}
              title="TaskFlow — @dag / @task decorators (Airflow-3 idiomatic)"
              onClick={() => onToggleSyntax('taskflow')}
            >
              TaskFlow
            </button>
            <button
              className={
                syntaxStyle === 'traditional'
                  ? 'jp-afdag-syntax-opt jp-mod-active'
                  : 'jp-afdag-syntax-opt'
              }
              aria-pressed={syntaxStyle === 'traditional'}
              title="Traditional — with DAG(…) + operator instances + >> wiring"
              onClick={() => onToggleSyntax('traditional')}
            >
              Traditional
            </button>
          </div>
          <span className="jp-afdag-spacer" />
          <button
            className="jp-afdag-btn"
            title="Rename the .afdag file (does not change the dag_id or affect a deployed DAG)"
            onClick={() => void onRename()}
          >
            Rename file…
          </button>
          <button
            className="jp-afdag-btn"
            title="Change the dag_id — a guided migration for a deployed DAG (Airflow has no rename)"
            onClick={() => void onRenameDagId()}
          >
            Rename DAG id…
          </button>
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
              taskNodes.length === 0
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
          onUnpauseTrigger={onUnpauseTrigger}
          onStopRun={() => void onStopRun()}
          onKeepWaiting={onKeepWaiting}
          onUndeploy={() => void onUndeploy()}
          onRollback={() => void onRollback()}
        />
        <div className="jp-afdag-body">
          <Palette
            operators={operators}
            onAdd={addNode}
            onAddNote={addNote}
            onRefresh={refreshOperators}
            collapsed={leftCollapsed}
            onToggle={toggleLeft}
          />
          <div className="jp-afdag-canvas">
            <ReactFlow
              nodes={nodes}
              edges={edges}
              nodeTypes={nodeTypes}
              edgeTypes={edgeTypes}
              defaultEdgeOptions={defaultEdgeOptions}
              connectionLineType={ConnectionLineType.SmoothStep}
              deleteKeyCode={['Delete', 'Backspace']}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              isValidConnection={isValidConnection}
              onReconnect={onReconnect}
              onNodesDelete={onNodesDelete}
              onNodeDragStart={onNodeDragStart}
              onNodeDragStop={onNodeDragStop}
              onInit={instance => {
                rfRef.current = instance;
                instance.fitView();
              }}
              onNodeClick={(_, node) =>
                setSelectedId(isNoteNode(node) ? null : node.id)
              }
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
            collapsed={rightCollapsed}
            onToggle={toggleRight}
            onDagChange={patch => setDag(d => ({ ...d, ...patch }))}
            onNodeChange={updateNode}
          />
        </div>
      </div>
    </EditorActionsContext.Provider>
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
