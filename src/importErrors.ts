// Plain-language translation of an Airflow import-error traceback into a
// friendly recovery card, plus a best-effort map back to the offending task
// node (PRD §7, M6 — the make-or-break recovery surface). Pure + unit-tested:
// no DOM, no network. The raw traceback is always kept available behind a
// "Show technical details" expander; this module only decides what to say up
// front and which task to point at.

import { IAfdagIR } from './ir';

export interface IExplainedError {
  /** Short headline for the card, e.g. "A provider package isn't installed". */
  title: string;
  /** One- or two-sentence plain-language summary of what went wrong. */
  summary: string;
  /** Actionable next step, when we recognise the failure (e.g. a pip install). */
  hint?: string;
  /** Best-effort `task_id` the error points at, when one can be identified. */
  nodeTaskId?: string;
}

/** The last non-empty line of a traceback — almost always the exception line. */
function lastLine(trace: string): string {
  const lines = trace
    .split('\n')
    .map(l => l.trim())
    .filter(Boolean);
  return lines.length ? lines[lines.length - 1] : '';
}

// Airflow-2 import paths that fail to import under Airflow 3 — codegen never
// emits these, but a hand-edited deployed file (drift) might.
const AIRFLOW2_PATHS = [
  'airflow.operators.',
  'airflow.sensors.',
  'airflow.contrib.',
  'airflow.hooks.',
  'airflow.models.dag',
  'airflow.decorators'
];

/**
 * Map a failed `airflow.providers.<provider>....` module to its pip package,
 * e.g. `airflow.providers.cncf.kubernetes.operators.pod` →
 * `apache-airflow-providers-cncf-kubernetes`. Returns null for a non-provider
 * module. The provider segments run until a known submodule boundary.
 */
export function providerPackageForModule(mod: string): string | null {
  const prefix = 'airflow.providers.';
  if (!mod.startsWith(prefix)) {
    return null;
  }
  const rest = mod.slice(prefix.length).split('.');
  const boundary = [
    'operators',
    'sensors',
    'hooks',
    'transfers',
    'triggers',
    'utils',
    'example_dags'
  ];
  const segs: string[] = [];
  for (const seg of rest) {
    if (boundary.indexOf(seg) !== -1) {
      break;
    }
    segs.push(seg);
  }
  if (!segs.length) {
    return null;
  }
  return 'apache-airflow-providers-' + segs.join('-').replace(/_/g, '-');
}

/**
 * Best-effort: which task node does this traceback point at? The generated
 * Python names every task by its `task_id`, so a `task_id` appearing as a whole
 * word in the traceback is a strong signal. Returns the first matching task_id.
 */
export function matchNode(trace: string, ir?: IAfdagIR): string | undefined {
  if (!ir || !ir.nodes || !ir.nodes.length) {
    return undefined;
  }
  for (const node of ir.nodes) {
    const id = node.task_id;
    if (!id) {
      continue;
    }
    // Whole-word match so a short id can't match inside a longer identifier.
    const re = new RegExp(
      '\\b' + id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b'
    );
    if (re.test(trace)) {
      return id;
    }
  }
  return undefined;
}

/**
 * Translate an import-error stack trace into a friendly card and (given the IR)
 * point at the offending task where possible. Falls back to the raw exception
 * line for failures we don't specifically recognise.
 */
export function explainImportError(
  stackTrace: string | undefined,
  ir?: IAfdagIR
): IExplainedError {
  const trace = (stackTrace ?? '').trim();
  const nodeTaskId = matchNode(trace, ir);
  if (!trace) {
    return {
      title: "Your DAG couldn't be loaded",
      summary:
        'Airflow reported an import error but gave no details. Open the DAG in Studio and re-deploy.',
      nodeTaskId
    };
  }

  const last = lastLine(trace);

  // Missing module — the most common deploy failure (a provider not installed).
  const noModule =
    /(?:ModuleNotFoundError|ImportError): No module named ['"]([^'"]+)['"]/.exec(
      last
    );
  if (noModule) {
    const mod = noModule[1];
    const pkg = providerPackageForModule(mod);
    if (pkg) {
      return {
        title: "A provider package isn't installed",
        summary: `This DAG uses an operator from “${mod}”, but that provider isn't installed in your Airflow.`,
        hint: `pip install ${pkg} in the Airflow environment, then re-deploy.`,
        nodeTaskId
      };
    }
    return {
      title: 'A Python module is missing',
      summary: `Airflow couldn't import “${mod}” on the Airflow side.`,
      hint: `Install ${mod} in the Airflow environment, then re-deploy.`,
      nodeTaskId
    };
  }

  // An Airflow-2 import path (only possible via a hand-edited deployed file).
  if (AIRFLOW2_PATHS.some(p => trace.indexOf(p) !== -1)) {
    return {
      title: 'An Airflow 2 import was used',
      summary:
        'The deployed file imports from an Airflow 2 path that no longer exists in Airflow 3. If you edited the .py by hand, re-deploy from Studio to regenerate it.',
      nodeTaskId
    };
  }

  // cannot import name X from Y — wrong symbol / version mismatch.
  const cannotImport =
    /ImportError: cannot import name ['"]([^'"]+)['"] from ['"]([^'"]+)['"]/.exec(
      last
    );
  if (cannotImport) {
    return {
      title: "An import didn't resolve",
      summary: `Airflow couldn't import “${cannotImport[1]}” from “${cannotImport[2]}” — usually a provider version mismatch.`,
      hint: 'Check the installed provider version in Airflow, then re-deploy.',
      nodeTaskId
    };
  }

  // Syntax / indentation error — almost always a code node's body.
  const syntax = /(SyntaxError|IndentationError|TabError): (.+)$/.exec(last);
  if (syntax) {
    return {
      title: 'There is a syntax error in your code',
      summary: nodeTaskId
        ? `The Python in the “${nodeTaskId}” task doesn't parse: ${syntax[2]}`
        : `A code node's Python doesn't parse: ${syntax[2]}`,
      hint: 'Open it in Studio and fix the highlighted code, then re-deploy.',
      nodeTaskId
    };
  }

  // Undefined name — typically a code node referencing something it didn't import.
  const nameErr = /NameError: name ['"]([^'"]+)['"] is not defined/.exec(last);
  if (nameErr) {
    return {
      title: 'A name is used but never defined',
      summary: nodeTaskId
        ? `The “${nodeTaskId}” task uses “${nameErr[1]}”, which isn't defined or imported.`
        : `A code node uses “${nameErr[1]}”, which isn't defined or imported.`,
      hint: 'Define or import it in the task code, then re-deploy.',
      nodeTaskId
    };
  }

  // Anything else: surface the exception line verbatim as the summary.
  return {
    title: "Your DAG couldn't be loaded",
    summary: last,
    hint: 'See the technical details below, fix it in Studio, then re-deploy.',
    nodeTaskId
  };
}
