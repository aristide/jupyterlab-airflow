import { MarkerType } from '@xyflow/react';
import type { Edge, Node } from '@xyflow/react';

import { IAfdagIR, IAfdagNode } from './ir';

// Bidirectional mapping between the `.afdag` IR and ReactFlow's nodes/edges,
// plus instant (client-side) cycle detection for the live error badge.

export interface AfdagNodeData {
  op: string;
  task_id: string;
  params: Record<string, unknown>;
  [key: string]: unknown;
}

export type AfdagFlowNode = Node<AfdagNodeData>;

export function irToFlow(ir: IAfdagIR): {
  nodes: AfdagFlowNode[];
  edges: Edge[];
} {
  const nodes: AfdagFlowNode[] = ir.nodes.map(node => ({
    id: node.id,
    type: 'afdagNode',
    position: node.position ?? { x: 0, y: 0 },
    data: { op: node.op, task_id: node.task_id, params: node.params ?? {} }
  }));
  const edges: Edge[] = ir.edges.map(edge => ({
    id: `e_${edge.source}__${edge.target}`,
    source: edge.source,
    target: edge.target,
    markerEnd: { type: MarkerType.ArrowClosed }
  }));
  return { nodes, edges };
}

export function flowToIR(
  nodes: AfdagFlowNode[],
  edges: Edge[],
  dag: IAfdagIR['dag'],
  base: IAfdagIR
): IAfdagIR {
  const irNodes: IAfdagNode[] = nodes.map(node => ({
    id: node.id,
    op: node.data.op,
    task_id: node.data.task_id,
    params: node.data.params,
    code: (node.data.params.code as string) ?? null,
    position: {
      x: Math.round(node.position.x),
      y: Math.round(node.position.y)
    }
  }));
  const irEdges = edges.map(edge => ({
    source: edge.source,
    target: edge.target
  }));
  return { ...base, dag, nodes: irNodes, edges: irEdges };
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
