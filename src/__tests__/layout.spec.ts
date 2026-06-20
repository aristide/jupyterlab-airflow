import type { Edge } from '@xyflow/react';

import { AFDAG_NOTE_TYPE, AfdagFlowNode } from '../graph';
import { tidyLayout } from '../layout';

function taskNode(id: string): AfdagFlowNode {
  return {
    id,
    type: 'afdagNode',
    position: { x: 0, y: 0 },
    data: { op: 'bash', task_id: id, params: {} }
  } as AfdagFlowNode;
}

function noteNode(id: string): AfdagFlowNode {
  return {
    id,
    type: AFDAG_NOTE_TYPE,
    position: { x: 5, y: 5 },
    data: { op: '__note__', task_id: '', params: {} }
  } as AfdagFlowNode;
}

const edge = (source: string, target: string): Edge => ({
  id: `e_${source}_${target}`,
  source,
  target
});

describe('tidyLayout', () => {
  it('lays task nodes out top-to-bottom along the edges', () => {
    const nodes = [taskNode('a'), taskNode('b'), taskNode('c')];
    const pos = tidyLayout(nodes, [edge('a', 'b'), edge('b', 'c')]);
    expect(pos.size).toBe(3);
    // Top-to-bottom: a above b above c (increasing y).
    expect(pos.get('a')!.y).toBeLessThan(pos.get('b')!.y);
    expect(pos.get('b')!.y).toBeLessThan(pos.get('c')!.y);
    // Positions are integers (rounded).
    expect(Number.isInteger(pos.get('a')!.x)).toBe(true);
  });

  it('excludes note cards from the layout', () => {
    const pos = tidyLayout([noteNode('n1'), taskNode('t')], []);
    expect(pos.has('n1')).toBe(false);
    expect(pos.has('t')).toBe(true);
  });

  it('is a no-op when there are no task nodes', () => {
    expect(tidyLayout([noteNode('n1')], []).size).toBe(0);
    expect(tidyLayout([], []).size).toBe(0);
  });
});
