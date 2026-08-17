import { Dialog, showDialog, showErrorMessage } from '@jupyterlab/apputils';
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
  apiError,
  cancelDeployLifecycle,
  resumeDeployLifecycle,
  deleteDag,
  deployDag,
  deployLifecycle,
  deployStatus,
  getDagRun,
  renamePreflight,
  retireOldDag,
  rollbackDag,
  setDagPaused,
  setDagRunState,
  triggerDag,
  IDeployLifecycleReq
} from '../handler';
import { keepWaitingPlan } from '../deployLifecycle';
import { explainImportError } from '../importErrors';
import { IDeployLifecycleRes, IOperatorDef } from '../interfaces';
import { tidyLayout } from '../layout';
import {
  AfdagCallbacksValue,
  IAfdagCallbackEntry,
  IAfdagIR,
  IAfdagConnection,
  IAfdagVariable,
  SyntaxStyle,
  createEmptyIR,
  dagIdFromPath,
  stringifyIR
} from '../ir';
import { AfdagModel } from '../model';
import { loadNotifiers, validateNotifierParams } from '../notifiers';
import {
  getOperator,
  getOperators,
  loadOperators,
  validateNodeParams
} from '../operators';
import { IStudioServices } from '../services';
import { AfdagEdge } from './AfdagEdge';
import { AfdagNode } from './AfdagNode';
import {
  CanEditContext,
  useFetchCanEdit,
  VIEW_ONLY_HINT
} from './capabilitiesContext';
import { Coachmark, CoachStep } from './Coachmark';
import { DagIdField } from './DagIdField';
import { DeployBanner, DeployPhase, IDeployState } from './DeployBanner';
import { EditorActionsContext, IEditorActions } from './editorContext';
import { Inspector } from './Inspector';
import { NoteNode } from './NoteNode';
import { Palette } from './Palette';

// Deploy poll cadence: a few minutes total, backing off from 2s to 8s. Airflow
// re-parses on min_file_process_interval (~30s) so sub-second polling is wasteful.
// Must exceed Airflow's *new-file* discovery interval, not just its re-parse
// interval. On the 3.0.2 target `dag_processor.refresh_interval` (and
// `scheduler.dag_dir_list_interval`) default to **300s**, while an already-known
// file is re-read on `min_file_process_interval` (~30s). A first deploy and a
// rename both create a NEW file, and how long that takes depends on where the
// write lands in the scan cycle — anywhere from seconds to the full 300s
// (measured here: 171s for one probe). So the old 180s budget didn't fail
// every time, it failed *unpredictably* — roughly whenever the deploy landed in
// the back half of a cycle — leaving the deploy looking hung and a rename with
// its old DAG stranded. 7 minutes covers a full scan plus parse/serialize.
const POLL_TIMEOUT_MS = 420000;
const POLL_START_MS = 2000;
const POLL_MAX_MS = 8000;
// Run-on-deploy (§6.5.4): a deployed run is polled to completion, with a longer
// ceiling than registration since a DAG run can legitimately take a while.
const RUN_POLL_TIMEOUT_MS = 600000;
// Observing a server-driven deploy (PRD §6.5.4): the server's own budget is
// 900s, so watch a little past it — reaching this deadline means the tab gave up
// watching, never that the deploy stopped.
const OBSERVE_TIMEOUT_MS = 960000;
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

// First-run onboarding is shown once per browser (PRD §7).
const ONBOARDED_KEY = 'jp-afdag-onboarded';

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

  // Whether this user may run privileged actions (PRD §9). Advisory: the server
  // rejects a privileged request from a viewer regardless, so this only shapes
  // what the UI offers.
  const canEdit = useFetchCanEdit();

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
  // Bumped to ask the inspector to focus its CODE tab (see `onPickSyntax`). A
  // counter rather than a boolean/tab value so repeated requests still fire:
  // the user may navigate away from CODE and toggle the syntax again.
  const [codeFocusNonce, setCodeFocusNonce] = React.useState(0);
  // The generated-code syntax family (PRD §6.3). Lives in the IR; the toggle
  // persists it and the CODE preview / Deploy regenerate accordingly.
  const [syntaxStyle, setSyntaxStyle] = React.useState<SyntaxStyle>('taskflow');
  // First-run onboarding (PRD §7): 0 = hidden, 1/2/3 = the active step. Set by
  // `load()` (step 1 only for a genuinely empty new doc, and only if not yet
  // onboarded — a localStorage flag, once per browser), advanced from state.
  const [coachStep, setCoachStep] = React.useState<number>(0);
  const [deploy, setDeploy] = React.useState<IDeployState>({
    phase: 'idle',
    message: ''
  });

  // Bumped whenever the flow's variables change, so the `currentIR` memo (which
  // reads them out of `baseRef`) recomputes — a ref is not a reactive dep.
  const [variablesRev, setVariablesRev] = React.useState(0);

  const baseRef = React.useRef<IAfdagIR>(createEmptyIR(''));
  const lastWritten = React.useRef<string>('');
  const loadingRef = React.useRef<boolean>(false);
  const rfRef = React.useRef<ReactFlowInstance<AfdagFlowNode, Edge> | null>(
    null
  );
  const tidyTimerRef = React.useRef<number | undefined>(undefined);
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

  // Load the notifier registry (PRD §6.8) for the Notifications tab. The ready
  // flag just re-renders so the tab shows the channels once the module cache
  // populates; a load failure degrades gracefully (the tab says none are
  // available) and never tears down the editor.
  const [notifiersReady, setNotifiersReady] = React.useState(false);
  React.useEffect(() => {
    let cancelled = false;
    loadNotifiers()
      .then(() => {
        if (!cancelled) {
          setNotifiersReady(true);
        }
      })
      .catch(() => undefined);
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
      // Onboarding (§7): start the tour only for a genuinely empty new document
      // (don't mis-stage over an already-built DAG), and only if the user hasn't
      // been onboarded in this browser.
      let onboarded = true;
      try {
        onboarded = !!window.localStorage.getItem(ONBOARDED_KEY);
      } catch {
        /* localStorage blocked → treat as onboarded (don't nag). */
      }
      setCoachStep(!onboarded && flow.nodes.length === 0 ? 1 : 0);
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

  // One-click "Tidy layout" (PRD §8.2): re-position the task nodes via a dagre
  // top-to-bottom layered layout. The persist effect saves the new positions;
  // re-fit once they've rendered. Notes stay where they are.
  const onTidyLayout = React.useCallback((): void => {
    const { nodes: liveNodes, edges: liveEdges } = latestRef.current;
    const positions = tidyLayout(liveNodes, liveEdges);
    if (positions.size === 0) {
      return;
    }
    setNodes(nds =>
      nds.map(node => {
        const next = positions.get(node.id);
        return next ? { ...node, position: next } : node;
      })
    );
    // Re-fit after the position change renders, scoped to just the task nodes
    // we laid out so a far-parked note card doesn't drag the view wide. Cancel
    // any pending timer (and on unmount, below) so it can't fire post-teardown.
    window.clearTimeout(tidyTimerRef.current);
    tidyTimerRef.current = window.setTimeout(
      () =>
        rfRef.current?.fitView({
          padding: 0.2,
          nodes: Array.from(positions.keys(), id => ({ id }))
        }),
      0
    );
  }, [setNodes]);

  React.useEffect(() => () => window.clearTimeout(tidyTimerRef.current), []);

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
    // Notification callbacks (PRD §6.8): a notifier missing a required param
    // (e.g. Slack `text`) blocks deploy too — but only once the notifier
    // registry has loaded (else don't false-block before it resolves). Both the
    // DAG-level (`dag.callbacks`) and per-task (`node.callbacks`) surfaces count.
    if (notifiersReady) {
      const blocks = [
        dag.callbacks,
        ...taskNodes.map(n => n.data.callbacks)
      ] as Array<AfdagCallbacksValue | undefined>;
      for (const callbacks of blocks) {
        if (!callbacks) {
          continue;
        }
        for (const list of Object.values(callbacks)) {
          for (const entry of (list as IAfdagCallbackEntry[]) ?? []) {
            if (
              !validateNotifierParams(entry.notifier_id, entry.params).valid
            ) {
              count += 1;
            }
          }
        }
      }
    }
    return count;
  }, [taskNodes, edges, dag.callbacks, notifiersReady]);

  const selected = taskNodes.find(n => n.id === selectedId) ?? null;

  // Onboarding (PRD §7): dismiss/finish the tour and don't show it again.
  const completeOnboarding = React.useCallback((): void => {
    setCoachStep(0);
    try {
      window.localStorage.setItem(ONBOARDED_KEY, '1');
    } catch {
      /* localStorage blocked — the tour just won't persist; harmless. */
    }
  }, []);

  // Step 1 → 2 once the first task is on the canvas.
  React.useEffect(() => {
    if (coachStep === 1 && taskNodes.length > 0) {
      setCoachStep(2);
    }
  }, [coachStep, taskNodes.length]);

  // Deploying (from any step) means the user has it — finish the tour.
  React.useEffect(() => {
    if (coachStep !== 0 && deploy.phase !== 'idle') {
      completeOnboarding();
    }
  }, [coachStep, deploy.phase, completeOnboarding]);

  // The IR projected from the live graph, fed to the CODE preview.
  const currentIR = React.useMemo(
    () => flowToIR(nodes, edges, dag, baseRef.current),
    [nodes, edges, dag, syntaxStyle, variablesRev]
  );

  // Plain-language translation of a failed import (PRD §7), with a best-effort
  // map back to the offending task using the live IR.
  const deployExplanation = React.useMemo(
    () =>
      deploy.phase === 'failed'
        ? explainImportError(deploy.importError?.stack_trace, currentIR)
        : undefined,
    [deploy.phase, deploy.importError, currentIR]
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

  // Picking a syntax reveals the generated code, because the code IS the point
  // of the switch — otherwise you flip it and nothing visibly happens. Wrapped
  // around `onToggleSyntax` rather than folded into it so the reveal fires on
  // EVERY click, including re-clicking the already-active option (which
  // `onToggleSyntax` short-circuits): "show me the code" is a reasonable
  // reading of that click too. Expanding a collapsed inspector is part of the
  // same intent — focusing a tab in a hidden panel would do nothing.
  const onPickSyntax = React.useCallback(
    (next: SyntaxStyle): void => {
      onToggleSyntax(next);
      setRightCollapsed(false);
      setCodeFocusNonce(n => n + 1);
    },
    [onToggleSyntax]
  );

  // Replace the flow's variable declarations (PRD §6.10). Variables live on the
  // IR *root*, which `flowToIR` carries through from `base` — so, exactly like
  // the syntax toggle, the write goes into `baseRef` (not React state) and then
  // commits. `variablesRev` only exists to re-run the `currentIR` memo, since
  // the ref itself is not a reactive dependency.
  const onVariablesChange = React.useCallback(
    (next: IAfdagVariable[]): void => {
      const base = { ...baseRef.current };
      if (next.length > 0) {
        base.variables = next;
      } else {
        delete base.variables;
      }
      baseRef.current = base;
      setVariablesRev(rev => rev + 1);
      commit();
    },
    [commit]
  );

  // Same shape for connection declarations (PRD §6.11): they live on the IR
  // root, so the write goes through `baseRef` and then commits.
  const onConnectionsChange = React.useCallback(
    (next: IAfdagConnection[]): void => {
      const base = { ...baseRef.current };
      if (next.length > 0) {
        base.connections = next;
      } else {
        delete base.connections;
      }
      baseRef.current = base;
      setVariablesRev(rev => rev + 1);
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

  // A queued "retire the old DAG" step for a dag_id rename migration (§6.1.8(B)).
  //
  // This ref is now the FALLBACK path only — when the server did not journal the
  // deploy (`lifecycle.reconciled === false`: the reconciler is switched off, or
  // the journal could not be written). On the normal path the intent is sent to
  // the server with the deploy and lives in the journal, which is the whole point:
  // an intent held in a React ref dies with the tab, and a rename whose tab closed
  // before Airflow registered the new DAG left the old one live forever.
  const fallbackRetireRef = React.useRef<{
    oldDagId: string;
    purge: boolean;
  } | null>(null);
  // The deploy currently being observed, so Dismiss can call it off server-side.
  const observedRef = React.useRef<string | null>(null);
  // A journaled deploy whose server-side budget ran out. "Keep waiting" must
  // re-arm the SERVER for it — never fall through to the legacy browser path,
  // which would unpause and trigger the new DAG while the rename's old DAG is
  // still live and unpaused (the retire intent lives only in the journal).
  const resumableRef = React.useRef<string | null>(null);

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
      // Check the unpause instead of discarding it. A failure here is silent
      // otherwise, and the banner below goes on to report the deploy as a
      // success — leaving a DAG that looks live but is paused and will never
      // run, which is exactly the state a user cannot diagnose from the UI.
      const unpaused = await setDagPaused(dagId, false);
      if (token.cancelled) {
        return;
      }
      if (unpaused.status !== 'OK') {
        setDeploy({
          phase: 'registered',
          dagId,
          filename,
          note,
          triggered: false,
          message:
            `Deployed ${dagId}, but could not unpause it` +
            `${unpaused.error ? ` — ${unpaused.error}` : ''}. ` +
            'It will not run until you unpause it in the DAG list.'
        });
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
            const pending = fallbackRetireRef.current;
            if (pending) {
              fallbackRetireRef.current = null;
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
            fallbackRetireRef.current = null;
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
        // Timed out. `fallbackRetireRef` is deliberately NOT cleared: retiring is
        // gated on actually observing `registered`, so keeping the intent is
        // safe and lets "Keep waiting" finish a migration that was merely slow.
        // Clearing it here used to strand the old DAG permanently — the rename
        // half-completed with no way to finish it.
        setDeploy({
          phase: 'processing',
          dagId,
          filename,
          message: fallbackRetireRef.current
            ? `Still processing — Airflow has not picked up “${dagId}” yet, so ` +
              `“${fallbackRetireRef.current.oldDagId}” has not been retired. ` +
              'Keep waiting to finish the rename.'
            : 'Still processing — Airflow has not picked up the file yet. ' +
              'This can take a few minutes.'
        });
      }
    },
    [runAfterDeploy]
  );

  // Observe (never perform) a deploy the SERVER is completing (PRD §6.5.4).
  //
  // This is the other half of the fix: when the deploy is journaled, retiring,
  // unpausing and triggering all happen server-side, so this loop calls no
  // mutating API at all. Closing the tab now stops the watching, not the deploy.
  const observeLifecycle = React.useCallback(
    async (deployId: string): Promise<void> => {
      const token = { cancelled: false };
      pollRef.current = token;
      observedRef.current = deployId;
      const deadline = Date.now() + OBSERVE_TIMEOUT_MS;
      let delay = POLL_START_MS;
      let errors = 0;

      while (!token.cancelled && Date.now() < deadline) {
        const res = await deployLifecycle({ deployId });
        if (token.cancelled) {
          return;
        }
        if (res.status === 'OK' && res.data) {
          errors = 0;
          const entry = res.data;
          if (entry.outcome === null) {
            setDeploy(prev => ({ ...prev, ...ongoingState(entry) }));
          } else {
            observedRef.current = null;
            // `expired` is "Airflow was still slow when the budget ran out", and
            // its skipped steps are re-armable server-side. Remember it so
            // "Keep waiting" asks the server for more time instead of taking the
            // work back into this tab.
            resumableRef.current =
              entry.outcome === 'expired' ? deployId : null;
            setDeploy(prev => ({ ...prev, ...terminalState(entry) }));
            // A created run is where the server's job ends — a run can take
            // hours and Airflow owns its state, so the existing run poll takes
            // over from the journal's run_id.
            if (
              entry.outcome === 'completed' &&
              entry.run_id &&
              entry.steps.trigger.state === 'done'
            ) {
              await pollRunState(
                entry.dag_id,
                entry.filename,
                entry.run_id,
                token
              );
            }
            return;
          }
        } else if (++errors >= MAX_RUN_POLL_ERRORS) {
          setDeploy(prev => ({
            ...prev,
            phase: 'processing',
            message:
              `Lost track of this deploy (${res.error ?? 'poll failed'}), but ` +
              'the server is still finishing it. Check the Manager.'
          }));
          return;
        }
        await sleep(delay);
        delay = Math.min(delay + 1000, POLL_MAX_MS);
      }

      if (!token.cancelled) {
        setDeploy(prev => ({
          ...prev,
          phase: 'processing',
          message:
            'Still processing — Airflow has not picked up the file yet. The ' +
            'server keeps finishing this deploy either way.'
        }));
      }
    },
    [pollRunState]
  );

  // Phase 1: validate + atomic write, then either observe the server finishing
  // the deploy or (when it did not journal it) drive the rest here. Takes an
  // explicit IR so a rename migration can deploy the renamed DAG (§6.1.8(B)),
  // and the migration's retire intent so the SERVER owns it.
  const runDeploy = React.useCallback(
    async (
      ir: IAfdagIR,
      lifecycle: IDeployLifecycleReq = {}
    ): Promise<void> => {
      cancelPoll();
      resumableRef.current = null;
      setDeploy({ phase: 'writing', message: 'Writing the DAG file…' });
      const res = await deployDag(ir, lifecycle);
      if (res.status !== 'OK' || !res.data?.deployed) {
        // Validation errors (when the server got far enough to produce them)
        // are the most specific thing to show; otherwise fall back to the
        // transport-level message, which for a 403 carries the view-only
        // explanation plus what to do about it.
        const detail =
          res.data?.errors?.join('; ') || apiError(res, 'Deploy failed');
        // A failed (re)deploy aborts any pending rename migration.
        fallbackRetireRef.current = null;
        setDeploy({ phase: 'error', message: detail });
        return;
      }
      const { dag_id: dagId, filename = '' } = res.data;
      const reconciled = res.data.lifecycle?.reconciled === true;
      if (reconciled) {
        // Exactly one performer: the intent is in the journal now, so it must
        // not also sit in a ref here.
        fallbackRetireRef.current = null;
      } else if (lifecycle.retire) {
        fallbackRetireRef.current = {
          oldDagId: lifecycle.retire.dag_id,
          purge: lifecycle.retire.purge
        };
      }
      setDeploy({
        phase: 'waiting',
        dagId,
        filename,
        // Carried into a later `failed` so the banner can offer Roll back (§7).
        backedUp: res.data.backed_up,
        // A brand-new file (first deploy or a rename) waits on Airflow's 300s
        // directory scan, so say so rather than letting it look hung.
        message:
          'Waiting for Airflow to pick it up… A new DAG file can take up to ' +
          '5 minutes to be discovered.' +
          (reconciled ? ' This continues even if you close this tab.' : '')
      });
      if (reconciled && res.data.lifecycle) {
        void observeLifecycle(res.data.lifecycle.deploy_id);
      } else {
        void pollLifecycle(dagId, filename);
      }
    },
    [cancelPoll, pollLifecycle, observeLifecycle]
  );

  // The Deploy button. A plain re-deploy overwrites the same {dag_id}.py, so if
  // the DAG is already registered with a run in flight we guard first: Airflow's
  // LocalDagBundle has no versioning and runs the latest file on disk (§8.8), so
  // overwriting mid-run can corrupt the in-flight run. Same active-run check the
  // rename migration uses (§6.1.8(B)); preflight failure falls through to deploy.
  const onDeploy = React.useCallback(async (): Promise<void> => {
    // Deliberately NOT cleared here. A plain deploy does not itself retire
    // another DAG, but clearing unconditionally means one Deploy click while a
    // rename migration is still waiting for Airflow silently discards the
    // retire intent — and the old dag_id is then stranded with no trace of why.
    // Retiring stays gated on actually observing `registered`, so keeping the
    // intent cannot retire anything that was not renamed.
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

  // Change the dag_id (PRD §6.1.8(B)): a deploy-aware migration. Airflow has no
  // rename — a new id is a NEW DAG with no history — so for a deployed DAG we
  // deploy the renamed DAG, then retire the old one (keep history or purge), and
  // we block while a run is in flight. A draft just sets the id.
  const onRenameDagId = React.useCallback(
    async (next: string): Promise<void> => {
      const current = dag.dag_id;
      if (!next || next === current) {
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

      // Deployed and idle: no prompt. Renaming is one action — type the new
      // name, press Enter. The old DAG is retired KEEPING its history, which
      // is reversible: the history stays in Airflow and the old `.py` is
      // regenerable from this same IR, so there is nothing here that warrants
      // interrupting the user to decide.
      //
      // Purge is deliberately NOT offered on this path any more. It used to be
      // a co-equal button on a routine rename, and choosing it destroys the old
      // DAG's run history irreversibly — a footgun sitting one click away from
      // the ordinary case. It now lives where the user has evidence the new DAG
      // works: the manager's per-row Delete, and the post-rename banner.
      const newIR: IAfdagIR = {
        ...currentIR,
        dag: { ...currentIR.dag, dag_id: next }
      };
      setDag(d => ({ ...d, dag_id: next }));
      void runDeploy(newIR, { retire: { dag_id: current, purge: false } });
    },
    [dag.dag_id, currentIR, runDeploy]
  );

  // Dismiss. While the server is finishing a deploy, dismissing the banner has
  // to mean something stronger than "stop looking": closing the tab no longer
  // stops the work, so this is the escape hatch that used to be implicit.
  const onDismissDeploy = React.useCallback(async (): Promise<void> => {
    const deployId = observedRef.current;
    if (deployId) {
      const confirmed = await showDialog({
        title: 'Stop finishing this deploy?',
        body:
          'Airflow keeps the deployed file, but Studio will not retire the old ' +
          'DAG, unpause it, or trigger a run. You can do those from the Manager.',
        buttons: [
          Dialog.cancelButton({ label: 'Keep going' }),
          Dialog.warnButton({ label: 'Stop' })
        ]
      });
      if (!confirmed.button.accept) {
        return;
      }
      observedRef.current = null;
      await cancelDeployLifecycle(deployId);
    }
    resumableRef.current = null;
    cancelPoll();
    setDeploy({ phase: 'idle', message: '' });
  }, [cancelPoll]);

  // "Keep waiting": re-attach to the server's work when it owns the deploy,
  // otherwise resume polling ourselves (the fallback path).
  const onKeepWaiting = React.useCallback(async (): Promise<void> => {
    const plan = keepWaitingPlan({
      observedDeployId: observedRef.current,
      resumableDeployId: resumableRef.current,
      dagId: deploy.dagId,
      filename: deploy.filename
    });
    if (plan.kind === 'observe') {
      setDeploy(prev => ({
        ...prev,
        phase: 'waiting',
        message: KEEP_WAITING_HINT
      }));
      void observeLifecycle(plan.deployId);
      return;
    }
    // The server ran out of budget. Ask it for more rather than re-running the
    // tail here: on the journaled path this tab holds no retire intent, so
    // finishing it locally would leave the renamed-away DAG live beside the new
    // one — both scheduled, both processing the same data.
    if (plan.kind === 'resume') {
      const res = await resumeDeployLifecycle(plan.deployId);
      if (res.status === 'OK' && res.data?.resumed) {
        resumableRef.current = null;
        setDeploy(prev => ({
          ...prev,
          phase: 'waiting',
          message: KEEP_WAITING_HINT
        }));
        void observeLifecycle(plan.deployId);
        return;
      }
      setDeploy(prev => ({
        ...prev,
        phase: 'processing',
        message:
          'Could not keep waiting: ' +
          (res.data?.reason ||
            apiError(res, 'the deploy could not be re-armed')) +
          '. Finish it from the Manager.'
      }));
      return;
    }
    if (plan.kind === 'poll') {
      setDeploy({
        phase: 'waiting',
        dagId: plan.dagId,
        filename: plan.filename,
        message: 'Waiting for Airflow to pick it up…'
      });
      void pollLifecycle(plan.dagId, plan.filename);
    }
  }, [deploy.dagId, deploy.filename, pollLifecycle, observeLifecycle]);

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

  // Re-attach to this flow's deploy after a reload (PRD §6.5.4). Reopening the
  // document used to show nothing at all while the server (or, before, nobody)
  // finished the work — this is the user-visible half of the fix: a still-running
  // deploy gets its banner back, and one that finished while the tab was closed
  // says so instead of vanishing.
  React.useEffect(() => {
    if (!ready) {
      return;
    }
    const afdagId = baseRef.current.provenance?.afdag_id;
    if (!afdagId) {
      return;
    }
    let cancelled = false;
    void deployLifecycle({ afdagId }).then(res => {
      if (cancelled || res.status !== 'OK' || !res.data) {
        return;
      }
      const entry = res.data;
      if (entry.outcome === null) {
        void observeLifecycle(entry.deploy_id);
        return;
      }
      const state = terminalState(entry);
      if (state.phase === 'idle') {
        return; // superseded/cancelled: nothing worth reporting on reopen
      }
      // Same rule as the live observer: only the server can resume an expired
      // lifecycle, so record it rather than letting "Keep waiting" fall back.
      resumableRef.current =
        entry.outcome === 'expired' ? entry.deploy_id : null;
      setDeploy(prev => ({
        ...prev,
        ...state,
        // A run that was created while the tab was closed is history by now, so
        // report the deploy rather than re-entering the live "Running…" poll.
        phase: state.phase === 'running' ? 'finished' : state.phase,
        message: `Deployed while this tab was closed — ${entry.message}`
      }));
    });
    return () => {
      cancelled = true;
    };
  }, [ready, observeLifecycle]);

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
    <CanEditContext.Provider value={canEdit}>
      <EditorActionsContext.Provider value={editorActions}>
        <div className="jp-afdag-root">
          <div className="jp-afdag-topbar">
            <span className="jp-afdag-brand">Airflow Studio</span>
            <DagIdField
              dagId={dag.dag_id}
              onCommit={next => void onRenameDagId(next)}
            />
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
            <span className="jp-afdag-spacer" />
            {/* The syntax toggle sits with the other actions on the right, not
              beside the title: it is something you DO to the flow, like Tidy or
              Deploy, not a fact about it like the node count or error badge.
              Grouping it left mixed those two readings. */}
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
                onClick={() => onPickSyntax('taskflow')}
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
                onClick={() => onPickSyntax('traditional')}
              >
                Traditional
              </button>
            </div>
            {/* Disabled rather than hidden: a toolbar that silently loses three
              buttons reads as a broken build, while a greyed-out one with a
              reason reads as a permission. */}
            <button
              className="jp-afdag-btn"
              title={
                canEdit
                  ? 'Auto-arrange the tasks (top-to-bottom layered layout)'
                  : VIEW_ONLY_HINT
              }
              disabled={!canEdit || taskNodes.length === 0}
              onClick={onTidyLayout}
            >
              ≣ Tidy
            </button>
            <button
              className="jp-afdag-btn"
              title={canEdit ? 'Save (.afdag)' : VIEW_ONLY_HINT}
              disabled={!canEdit}
              onClick={() => void context.save()}
            >
              Save
            </button>
            <button
              className="jp-afdag-btn jp-afdag-btn-primary"
              title={
                !canEdit
                  ? VIEW_ONLY_HINT
                  : errorCount
                    ? 'Fix validation errors before deploying'
                    : 'Validate and deploy the DAG to Airflow'
              }
              disabled={
                !canEdit ||
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
          {!canEdit && (
            <div className="jp-afdag-viewonly" role="status">
              <span className="jp-afdag-viewonly-badge">View only</span>
              <span>
                You can open, read and generate the code for this flow, but not
                change or deploy it. Ask an administrator for edit access.
              </span>
            </div>
          )}
          <DeployBanner
            state={deploy}
            explanation={deployExplanation}
            onDismiss={() => void onDismissDeploy()}
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
              {coachStep !== 0 && (
                <Coachmark
                  step={coachStep as CoachStep}
                  onSkip={completeOnboarding}
                  onNext={() =>
                    coachStep >= 3
                      ? completeOnboarding()
                      : setCoachStep(coachStep + 1)
                  }
                />
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
              focusCodeNonce={codeFocusNonce}
              onDagChange={patch => setDag(d => ({ ...d, ...patch }))}
              onNodeChange={updateNode}
              onVariablesChange={onVariablesChange}
              onConnectionsChange={onConnectionsChange}
            />
          </div>
          <div className="jp-afdag-statusbar">
            <span className="jp-afdag-statusbar-file">
              {context.path.split('/').pop() ?? context.path}
            </span>
            <span className="jp-afdag-statusbar-sep">·</span>
            <span>
              {syntaxStyle === 'taskflow' ? 'TaskFlow' : 'Traditional'}
            </span>
            <span className="jp-afdag-statusbar-sep">·</span>
            <span className="jp-afdag-statusbar-state">
              {STATUS_LABEL[deploy.phase] ??
                (errorCount
                  ? `${errorCount} unresolved ${
                      errorCount === 1 ? 'field' : 'fields'
                    }`
                  : 'Saved')}
            </span>
          </div>
        </div>
      </EditorActionsContext.Provider>
    </CanEditContext.Provider>
  );
}

// The banner is a projection of the server's journal entry while the server is
// finishing a deploy (PRD §6.5.4). The existing `DeployPhase` union is enough —
// what changed is who performs the steps, not what the user is told.
const KEEP_WAITING_HINT =
  'Waiting for Airflow to pick it up… A new DAG file can take up to 5 minutes ' +
  'to be discovered. This continues even if you close this tab.';

function retireNote(entry: IDeployLifecycleRes): string | undefined {
  const old = entry.retire_dag_id;
  if (!old) {
    return undefined;
  }
  const step = entry.steps.retire;
  if (step.state === 'done') {
    return `Old DAG “${old}” retired.`;
  }
  if (step.state === 'skipped') {
    return `Old DAG “${old}” was left alone — ${
      step.skipped_reason ?? 'the step was skipped'
    }.`;
  }
  return undefined;
}

/** A banner patch: always carries a phase, so it can be spread over the previous
 * state without widening `phase` to `undefined`. */
type LifecycleView = Partial<IDeployState> & { phase: DeployPhase };

function ongoingState(entry: IDeployLifecycleRes): LifecycleView {
  const waiting = entry.phase === 'awaiting_registration';
  return {
    phase: waiting ? 'waiting' : 'registered',
    dagId: entry.dag_id,
    filename: entry.filename,
    note: retireNote(entry),
    // The server does the unpause + trigger, so never offer "Unpause & trigger"
    // alongside it — that is what "observe, don't perform" means in the UI.
    triggered: true,
    message: waiting
      ? KEEP_WAITING_HINT
      : `${entry.message || `Registered ${entry.dag_id}`} ${
          entry.phase === 'retiring'
            ? `Retiring “${entry.retire_dag_id ?? ''}”…`
            : entry.phase === 'unpausing'
              ? 'Unpausing…'
              : 'Triggering a run…'
        } You can close this tab.`
  };
}

function terminalState(entry: IDeployLifecycleRes): LifecycleView {
  const base = {
    dagId: entry.dag_id,
    filename: entry.filename,
    note: retireNote(entry)
  };
  switch (entry.outcome) {
    case 'completed':
      if (entry.run_id && entry.steps.trigger.state === 'done') {
        return {
          ...base,
          phase: 'running',
          runId: entry.run_id,
          runState: entry.run_state ?? undefined,
          triggered: true,
          message: `Running ${entry.dag_id}…`
        };
      }
      return {
        ...base,
        phase: 'finished',
        triggered: false,
        message: entry.message || `Deployed ${entry.dag_id}.`
      };
    case 'import_failed':
      return {
        ...base,
        phase: 'failed',
        importError: entry.import_error,
        message: entry.message || `${entry.filename} failed to import.`
      };
    case 'expired':
      // "Keep waiting" now re-attaches to the server's work rather than
      // re-performing it.
      return { ...base, phase: 'processing', message: entry.message };
    case 'superseded':
    case 'cancelled':
      return { phase: 'idle', message: '' };
    default:
      return {
        ...base,
        phase: 'error',
        message:
          entry.message ||
          `Could not finish deploying ${entry.dag_id} (${entry.outcome}).`
      };
  }
}

// Short status-bar wording per deploy phase. `idle` is deliberately absent so
// it falls through to the validation/saved summary — with nothing deploying,
// what the user wants to know is whether the flow is ready, not that it is
// doing nothing. The deploy *target* is not shown: no target string exists
// client-side, and fetching one just to label a footer is not worth a request.
const STATUS_LABEL: Partial<Record<IDeployState['phase'], string>> = {
  writing: 'Writing DAG file…',
  waiting: 'Waiting for Airflow…',
  processing: 'Still processing…',
  registered: 'Registered',
  running: 'Running',
  finished: 'Deployed · run finished',
  failed: 'Import error',
  error: 'Deploy failed'
};

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
