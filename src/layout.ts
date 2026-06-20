import dagre from '@dagrejs/dagre';
import type { Edge } from '@xyflow/react';

import { AfdagFlowNode, isNoteNode } from './graph';

// Fallbacks when a node hasn't been measured yet (matches the node card's
// min-width + typical height in style/afdag.css).
const DEFAULT_NODE_WIDTH = 170;
const DEFAULT_NODE_HEIGHT = 52;

/**
 * One-click "Tidy layout" (PRD §8.2): compute clean top-to-bottom positions for
 * the **task** nodes via a dagre layered layout. Annotation note cards are left
 * where they are — they aren't part of the task graph. Pure: returns a map of
 * node id → new top-left position; a node absent from the map keeps its current
 * position (so an empty graph or notes-only graph is a no-op).
 */
export function tidyLayout(
  nodes: AfdagFlowNode[],
  edges: Edge[]
): Map<string, { x: number; y: number }> {
  const positions = new Map<string, { x: number; y: number }>();
  const taskNodes = nodes.filter(node => !isNoteNode(node));
  if (taskNodes.length === 0) {
    return positions;
  }

  const graph = new dagre.graphlib.Graph();
  graph.setGraph({
    rankdir: 'TB',
    nodesep: 40,
    ranksep: 70,
    marginx: 20,
    marginy: 20
  });
  graph.setDefaultEdgeLabel(() => ({}));

  const ids = new Set(taskNodes.map(node => node.id));
  for (const node of taskNodes) {
    graph.setNode(node.id, {
      width: node.measured?.width ?? node.width ?? DEFAULT_NODE_WIDTH,
      height: node.measured?.height ?? node.height ?? DEFAULT_NODE_HEIGHT
    });
  }
  for (const edge of edges) {
    if (ids.has(edge.source) && ids.has(edge.target)) {
      graph.setEdge(edge.source, edge.target);
    }
  }

  dagre.layout(graph);

  for (const node of taskNodes) {
    const laid = graph.node(node.id);
    if (laid) {
      // dagre positions are node centres; ReactFlow positions are top-left.
      positions.set(node.id, {
        x: Math.round(laid.x - laid.width / 2),
        y: Math.round(laid.y - laid.height / 2)
      });
    }
  }
  return positions;
}
