import { MarkerType } from '@xyflow/react';
import type { Edge } from '@xyflow/react';

import {
  AFDAG_EDGE_TYPE,
  AFDAG_NOTE_TYPE,
  canConnect,
  flowToIR,
  hasCycle,
  irToFlow
} from '../graph';
import { IAfdagIR, createEmptyIR } from '../ir';

function makeIR(): IAfdagIR {
  const ir = createEmptyIR('test_dag');
  ir.nodes = [
    {
      id: 'n1',
      op: 'bash',
      task_id: 'extract',
      params: { bash_command: 'echo hi' },
      position: { x: 10.4, y: 20.6 }
    },
    {
      id: 'n2',
      op: 'bash',
      task_id: 'load',
      params: { bash_command: 'echo bye' },
      position: { x: 200, y: 20 }
    }
  ];
  ir.edges = [{ source: 'n1', target: 'n2' }];
  return ir;
}

describe('irToFlow / flowToIR mapping', () => {
  it('maps edges to the custom rounded-arrow edge type with a marker', () => {
    const { nodes, edges } = irToFlow(makeIR());
    expect(nodes.map(n => n.type)).toEqual(['afdagNode', 'afdagNode']);
    expect(edges).toHaveLength(1);
    const edge = edges[0];
    expect(edge.id).toBe('e_n1__n2');
    expect(edge.source).toBe('n1');
    expect(edge.target).toBe('n2');
    expect(edge.type).toBe(AFDAG_EDGE_TYPE);
    expect(edge.reconnectable).toBe(true);
    expect(edge.markerEnd).toEqual({ type: MarkerType.ArrowClosed });
  });

  it('round-trips per-node common settings, omitting an empty common', () => {
    const ir = makeIR();
    ir.nodes[0].common = { retries: 3, retry_delay: 120 };
    const { nodes, edges } = irToFlow(ir);
    expect(nodes[0].data.common).toEqual({ retries: 3, retry_delay: 120 });
    const back = flowToIR(nodes, edges, ir.dag, ir);
    expect(back.nodes[0].common).toEqual({ retries: 3, retry_delay: 120 });
    // The node with no common settings doesn't get an empty `common` key.
    expect('common' in back.nodes[1]).toBe(false);
  });

  it('round-trips per-task callbacks, pruning empty event arrays', () => {
    const ir = makeIR();
    ir.nodes[0].callbacks = {
      on_failure: [{ notifier_id: 'slack', params: { text: 'down' } }],
      on_retry: [] // empty -> must not survive the round-trip
    };
    const { nodes, edges } = irToFlow(ir);
    expect(nodes[0].data.callbacks).toEqual(ir.nodes[0].callbacks);
    const back = flowToIR(nodes, edges, ir.dag, ir);
    expect(back.nodes[0].callbacks).toEqual({
      on_failure: [{ notifier_id: 'slack', params: { text: 'down' } }]
    });
    // The empty event array is dropped, and a node with no callbacks omits it.
    expect(back.nodes[0].callbacks?.on_retry).toBeUndefined();
    expect('callbacks' in back.nodes[1]).toBe(false);
  });

  it('round-trips per-node asset inlets/outlets, dropping empty/blank ones', () => {
    const ir = makeIR();
    ir.nodes[0].outlets = ['s3://lake/orders.csv', 'curated'];
    ir.nodes[0].inlets = ['', '  ']; // blank-only -> must not survive
    const { nodes, edges } = irToFlow(ir);
    expect(nodes[0].data.outlets).toEqual(['s3://lake/orders.csv', 'curated']);
    const back = flowToIR(nodes, edges, ir.dag, ir);
    expect(back.nodes[0].outlets).toEqual(['s3://lake/orders.csv', 'curated']);
    // The blank-only inlets list is dropped; a node with neither omits both keys.
    expect('inlets' in back.nodes[0]).toBe(false);
    expect('inlets' in back.nodes[1]).toBe(false);
    expect('outlets' in back.nodes[1]).toBe(false);
  });

  it('flowToIR strips edges to {source,target} and rounds positions', () => {
    const ir = makeIR();
    const { nodes, edges } = irToFlow(ir);
    const back = flowToIR(nodes, edges, ir.dag, ir);
    expect(back.edges).toEqual([{ source: 'n1', target: 'n2' }]);
    expect(back.nodes[0].position).toEqual({ x: 10, y: 21 });
    expect(back.nodes.map(n => n.task_id)).toEqual(['extract', 'load']);
  });

  it('deleting an edge from the flow drops it from the IR but keeps both nodes', () => {
    const ir = makeIR();
    const { nodes, edges } = irToFlow(ir);
    const remaining = edges.filter(e => e.id !== 'e_n1__n2');
    const back = flowToIR(nodes, remaining, ir.dag, ir);
    expect(back.edges).toEqual([]);
    expect(back.nodes).toHaveLength(2);
  });
});

describe('canConnect guard (shared by connect + reconnect)', () => {
  const edges: Edge[] = [{ id: 'e_a__b', source: 'a', target: 'b' }];

  it('rejects self-loops', () => {
    expect(canConnect('a', 'a', edges)).toBe(false);
  });

  it('rejects null/undefined endpoints', () => {
    expect(canConnect(null, 'b', edges)).toBe(false);
    expect(canConnect('a', undefined, edges)).toBe(false);
  });

  it('rejects a duplicate (source, target) pair', () => {
    expect(canConnect('a', 'b', edges)).toBe(false);
  });

  it('allows a new valid connection (including the reverse direction)', () => {
    expect(canConnect('a', 'c', edges)).toBe(true);
    expect(canConnect('b', 'a', edges)).toBe(true);
  });
});

describe('hasCycle', () => {
  it('accepts a DAG and rejects a cycle', () => {
    const acyclic = irToFlow(makeIR());
    expect(hasCycle(acyclic.nodes, acyclic.edges)).toBe(false);

    const cyclic = irToFlow({
      ...makeIR(),
      edges: [
        { source: 'n1', target: 'n2' },
        { source: 'n2', target: 'n1' }
      ]
    });
    expect(hasCycle(cyclic.nodes, cyclic.edges)).toBe(true);
  });
});

describe('annotation notes (separate IR array, excluded from the task graph)', () => {
  it('round-trips notes and keeps them out of nodes[]/edges[]', () => {
    const ir = makeIR();
    ir.notes = [
      {
        id: 'note1',
        text: 'owner: data-eng',
        position: { x: 50, y: 200 },
        size: { width: 200, height: 100 }
      }
    ];
    const { nodes, edges } = irToFlow(ir);

    // The note shares the ReactFlow nodes array, tagged as a note type.
    expect(nodes).toHaveLength(3);
    const noteFlow = nodes.find(n => n.id === 'note1');
    expect(noteFlow?.type).toBe(AFDAG_NOTE_TYPE);
    expect(noteFlow?.width).toBe(200);
    expect((noteFlow?.data as { text?: string }).text).toBe('owner: data-eng');

    const back = flowToIR(nodes, edges, ir.dag, ir);
    // Notes are split back OUT of nodes[] — codegen/validation never see them.
    expect(back.nodes.map(n => n.id)).toEqual(['n1', 'n2']);
    expect(back.nodes.some(n => n.op === '__note__')).toBe(false);
    expect(back.notes).toEqual([
      {
        id: 'note1',
        text: 'owner: data-eng',
        position: { x: 50, y: 200 },
        size: { width: 200, height: 100 }
      }
    ]);
  });

  it('omits the notes key entirely when there are none', () => {
    const ir = makeIR();
    const { nodes, edges } = irToFlow(ir);
    const back = flowToIR(nodes, edges, ir.dag, ir);
    expect('notes' in back).toBe(false);
  });
});
