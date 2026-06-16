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

/**
 * An annotation note (PRD §6.1.7): a free-text card on the canvas for team
 * documentation. Deliberately stored OUTSIDE `nodes[]`/`edges[]` so the
 * executable task graph that codegen + validation read is untouched — a note
 * never becomes a task, an edge, or a cycle/required-field error.
 */
export interface IAfdagNote {
  id: string;
  text: string;
  position: { x: number; y: number };
  size?: { width: number; height: number };
}

export interface IAfdagIR {
  schema_version: string;
  provenance: IAfdagProvenance;
  syntax_style: SyntaxStyle;
  dag: IAfdagDagConfig;
  nodes: IAfdagNode[];
  edges: IAfdagEdge[];
  /** Annotation cards (optional; absent on pre-notes `.afdag` files). */
  notes?: IAfdagNote[];
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

/**
 * Validate/normalize a user-entered rename into an `.afdag` basename. Renaming
 * the *document* is filesystem-only and has no effect on the `dag_id` or any
 * deployed/running pipeline (see docs/PRD.md §6.1.8(A)) — changing the `dag_id`
 * is the separate, deploy-aware migration in §6.1.8(B). Returns the normalized
 * basename (with the `.afdag` extension ensured), or an error string to show.
 */
export function normalizeAfdagFilename(
  input: string
): { name: string } | { error: string } {
  const trimmed = input.trim();
  if (!trimmed) {
    return { error: 'Enter a file name.' };
  }
  if (/[\\/]/.test(trimmed)) {
    return { error: 'The name cannot contain a path separator.' };
  }
  const name = /\.afdag$/i.test(trimmed) ? trimmed : `${trimmed}.afdag`;
  return { name };
}

const PY_KEYWORDS = new Set([
  'False',
  'None',
  'True',
  'and',
  'as',
  'assert',
  'async',
  'await',
  'break',
  'class',
  'continue',
  'def',
  'del',
  'elif',
  'else',
  'except',
  'finally',
  'for',
  'from',
  'global',
  'if',
  'import',
  'in',
  'is',
  'lambda',
  'nonlocal',
  'not',
  'or',
  'pass',
  'raise',
  'return',
  'try',
  'while',
  'with',
  'yield'
]);

/**
 * Validate a candidate `dag_id` client-side for instant feedback before the
 * deploy-aware rename migration (docs/PRD.md §6.1.8(B)). It must be a valid
 * Python identifier and not a keyword; the server re-validates authoritatively
 * (§8.4 ③). Returns the id, or an error message to show.
 */
export function validateDagId(
  input: string
): { id: string } | { error: string } {
  const id = input.trim();
  if (!id) {
    return { error: 'Enter a DAG id.' };
  }
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(id)) {
    return {
      error:
        'The DAG id must be a valid Python identifier — letters, digits, and ' +
        'underscores only, not starting with a digit.'
    };
  }
  if (PY_KEYWORDS.has(id)) {
    return { error: `"${id}" is a Python keyword and cannot be a DAG id.` };
  }
  return { id };
}
