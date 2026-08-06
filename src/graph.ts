import { MarkerType } from '@xyflow/react';
import type { Edge, Node } from '@xyflow/react';

import { IAfdagIR, IAfdagNode, IAfdagNote, IAfdagTaskCallbacks } from './ir';

// Bidirectional mapping between the `.afdag` IR and ReactFlow's nodes/edges,
// plus instant (client-side) cycle detection for the live error badge.

// Custom edge type id (a rounded-corner smoothstep arrow with an on-edge delete
// control — see AfdagEdge). Registered in StudioApp's `edgeTypes`. The IR stores
// only {source, target}, so the type/reconnectable flags are presentation-only
// and never persisted.
export const AFDAG_EDGE_TYPE = 'afdagEdge';

// Annotation note cards (PRD §6.1.7). They share ReactFlow's `nodes` array with
// task nodes but are tagged with this type and a marker `op`, and are split back
// into the IR's separate `notes[]` array on persist — so they never reach
// codegen, cycle detection, or required-field validation.
export const AFDAG_NOTE_TYPE = 'noteNode';
export const NOTE_OP = '__note__';
export const DEFAULT_NOTE_SIZE = { width: 220, height: 120 };

export interface IAfdagNodeData {
  op: string;
  task_id: string;
  params: Record<string, unknown>;
  /** Per-task common settings (PRD §6.1.3); see `IAfdagNode.common`. */
  common?: Record<string, unknown>;
  /** Per-task notification callbacks (PRD §6.8); see `IAfdagNode.callbacks`. */
  callbacks?: IAfdagTaskCallbacks;
  /** Assets this task consumes/produces (PRD §6.9); see `IAfdagNode.inlets`/`outlets`. */
  inlets?: string[];
  outlets?: string[];
  [key: string]: unknown;
}

export type AfdagFlowNode = Node<IAfdagNodeData>;

/** True for an annotation note card (vs an Airflow-task node). */
export function isNoteNode(node: AfdagFlowNode): boolean {
  return node.type === AFDAG_NOTE_TYPE;
}

export function irToFlow(ir: IAfdagIR): {
  nodes: AfdagFlowNode[];
  edges: Edge[];
} {
  const taskNodes: AfdagFlowNode[] = ir.nodes.map(node => ({
    id: node.id,
    type: 'afdagNode',
    position: node.position ?? { x: 0, y: 0 },
    data: {
      op: node.op,
      task_id: node.task_id,
      params: node.params ?? {},
      common: node.common ?? {},
      callbacks: node.callbacks,
      inlets: node.inlets,
      outlets: node.outlets
    }
  }));
  const noteNodes: AfdagFlowNode[] = (ir.notes ?? []).map(note => ({
    id: note.id,
    type: AFDAG_NOTE_TYPE,
    position: note.position ?? { x: 0, y: 0 },
    width: note.size?.width ?? DEFAULT_NOTE_SIZE.width,
    height: note.size?.height ?? DEFAULT_NOTE_SIZE.height,
    data: { op: NOTE_OP, task_id: '', params: {}, text: note.text ?? '' }
  }));
  const edges: Edge[] = ir.edges.map(edge => ({
    id: `e_${edge.source}__${edge.target}`,
    source: edge.source,
    target: edge.target,
    type: AFDAG_EDGE_TYPE,
    reconnectable: true,
    markerEnd: { type: MarkerType.ArrowClosed }
  }));
  return { nodes: taskNodes.concat(noteNodes), edges };
}

/** Drop empty event arrays from a per-task callbacks block, returning undefined
 * when nothing remains — so the IR (and the deployed `.py`) stays clean and a
 * node without callbacks omits the field entirely (back-compatible). */
function pruneCallbacks(
  callbacks: IAfdagTaskCallbacks | undefined
): IAfdagTaskCallbacks | undefined {
  if (!callbacks) {
    return undefined;
  }
  const out: IAfdagTaskCallbacks = {};
  let any = false;
  for (const event of Object.keys(callbacks) as Array<
    keyof IAfdagTaskCallbacks
  >) {
    const list = callbacks[event];
    if (list && list.length > 0) {
      out[event] = list;
      any = true;
    }
  }
  return any ? out : undefined;
}

export function flowToIR(
  nodes: AfdagFlowNode[],
  edges: Edge[],
  dag: IAfdagIR['dag'],
  base: IAfdagIR
): IAfdagIR {
  const irNodes: IAfdagNode[] = nodes
    .filter(node => !isNoteNode(node))
    .map(node => {
      const irNode: IAfdagNode = {
        id: node.id,
        op: node.data.op,
        task_id: node.data.task_id,
        params: node.data.params,
        code: (node.data.params.code as string) ?? null,
        position: {
          x: Math.round(node.position.x),
          y: Math.round(node.position.y)
        }
      };
      // Persist per-task common settings only when some are set (keeps the IR
      // and the deployed `.py` clean, and stays back-compatible).
      const common = node.data.common;
      if (common && Object.keys(common).length > 0) {
        irNode.common = common;
      }
      // Persist per-task callbacks only when an event actually carries an entry
      // (an empty event array is omitted, like `common` — back-compatible).
      const callbacks = pruneCallbacks(node.data.callbacks);
      if (callbacks) {
        irNode.callbacks = callbacks;
      }
      // Persist asset inlets/outlets only when non-empty (PRD §6.9) — keeps the
      // IR clean and back-compatible (absent on pre-asset `.afdag` files).
      const inlets = (node.data.inlets ?? []).filter(a => a.trim() !== '');
      if (inlets.length > 0) {
        irNode.inlets = inlets;
      }
      const outlets = (node.data.outlets ?? []).filter(a => a.trim() !== '');
      if (outlets.length > 0) {
        irNode.outlets = outlets;
      }
      return irNode;
    });
  const irEdges = edges.map(edge => ({
    source: edge.source,
    target: edge.target
  }));
  const irNotes: IAfdagNote[] = nodes.filter(isNoteNode).map(node => {
    const width = node.width ?? node.measured?.width;
    const height = node.height ?? node.measured?.height;
    const note: IAfdagNote = {
      id: node.id,
      text: String(node.data.text ?? ''),
      position: {
        x: Math.round(node.position.x),
        y: Math.round(node.position.y)
      }
    };
    if (typeof width === 'number' && typeof height === 'number') {
      note.size = { width: Math.round(width), height: Math.round(height) };
    }
    return note;
  });
  const ir: IAfdagIR = { ...base, dag, nodes: irNodes, edges: irEdges };
  if (irNotes.length > 0) {
    ir.notes = irNotes;
  } else {
    delete ir.notes;
  }
  return ir;
}

/**
 * Connection guard shared by `onConnect` and edge reconnect: reject self-loops
 * and duplicate `(source, target)` pairs. Duplicates must be rejected because
 * the IR derives a deterministic edge id `e_{source}__{target}` that would
 * otherwise collide on reload.
 */
export function canConnect(
  source: string | null | undefined,
  target: string | null | undefined,
  edges: Edge[]
): boolean {
  if (!source || !target || source === target) {
    return false;
  }
  return !edges.some(edge => edge.source === source && edge.target === target);
}

/**
 * Kahn's algorithm: returns true when the graph contains a cycle (Airflow
 * rejects cyclic DAGs at parse time).
 */
export function hasCycle(nodes: AfdagFlowNode[], edges: Edge[]): boolean {
  const indegree = new Map<string, number>();
  const adjacency = new Map<string, string[]>();
  for (const node of nodes) {
    indegree.set(node.id, 0);
    adjacency.set(node.id, []);
  }
  for (const edge of edges) {
    if (!indegree.has(edge.source) || !indegree.has(edge.target)) {
      continue;
    }
    indegree.set(edge.target, (indegree.get(edge.target) ?? 0) + 1);
    adjacency.get(edge.source)?.push(edge.target);
  }
  const queue: string[] = [];
  indegree.forEach((degree, id) => {
    if (degree === 0) {
      queue.push(id);
    }
  });
  let visited = 0;
  while (queue.length > 0) {
    const id = queue.shift() as string;
    visited += 1;
    for (const next of adjacency.get(id) ?? []) {
      const degree = (indegree.get(next) ?? 0) - 1;
      indegree.set(next, degree);
      if (degree === 0) {
        queue.push(next);
      }
    }
  }
  return visited < nodes.length;
}
