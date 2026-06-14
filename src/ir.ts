import { UUID } from '@lumino/coreutils';

// The `.afdag` intermediate representation (IR): a versioned, syntax-agnostic
// JSON model of a DAG graph. It is the source of truth the visual editor reads
// and writes; the generated `.py` (a separate deploy artifact) is produced from
// it server-side. See docs/PRD.md (Appendix B).

export type SyntaxStyle = 'taskflow' | 'traditional';

export interface IAfdagProvenance {
  generator: string;
  studio_version: string;
  afdag_id: string;
  ir_hash?: string;
  created_at?: string;
  updated_at?: string;
}

export interface IAfdagDagConfig {
  dag_id: string;
  description?: string;
  schedule?: string | null;
  start_date?: string;
  catchup?: boolean;
  retries?: number;
  retry_delay_seconds?: number;
  tags?: string[];
  owner?: string;
  params?: Record<string, unknown>;
  default_args?: Record<string, unknown>;
}

export interface IAfdagNode {
  id: string;
  op: string;
  task_id: string;
  params: Record<string, unknown>;
  code?: string | null;
  position?: { x: number; y: number };
}

export interface IAfdagEdge {
  source: string;
  target: string;
}

export interface IAfdagIR {
  schema_version: string;
  provenance: IAfdagProvenance;
  syntax_style: SyntaxStyle;
  dag: IAfdagDagConfig;
  nodes: IAfdagNode[];
  edges: IAfdagEdge[];
}

export const AFDAG_SCHEMA_VERSION = '1.0';

/**
 * Build a blank IR for a new DAG, defaulting to Airflow 3.x-friendly values
 * (catchup disabled, a daily schedule, the TaskFlow syntax style).
 */
export function createEmptyIR(dagId: string): IAfdagIR {
  return {
    schema_version: AFDAG_SCHEMA_VERSION,
    provenance: {
      generator: 'airflow-studio',
      studio_version: '0.1.0',
      afdag_id: UUID.uuid4()
    },
    syntax_style: 'taskflow',
    dag: {
      dag_id: dagId || 'untitled_dag',
      description: '',
      schedule: '@daily',
      start_date: '2026-01-01',
      catchup: false,
      retries: 0,
      retry_delay_seconds: 300,
      tags: ['studio'],
      owner: '',
      params: {},
      default_args: {}
    },
    nodes: [],
    edges: []
  };
}

export function stringifyIR(ir: IAfdagIR): string {
  return `${JSON.stringify(ir, null, 2)}\n`;
}

/**
 * Derive a valid Python-identifier dag_id from a document path's basename.
 */
export function dagIdFromPath(path: string): string {
  const base = path.split('/').pop() ?? 'untitled_dag';
  const stem = base.replace(/\.afdag$/i, '');
  const safe = stem.replace(/[^A-Za-z0-9_]/g, '_').replace(/^([0-9])/, '_$1');
  return safe || 'untitled_dag';
}
