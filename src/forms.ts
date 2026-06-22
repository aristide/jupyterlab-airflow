// Registry -> RJSF JSON Schema / uiSchema, plus the IR <-> form-data converters.
// The registry is the single source of truth for the NODE form (PRD §6.2); the
// DAG form is a fixed schema. `json`/object params are edited as JSON *text*
// (a `json` CodeMirror widget) and converted to/from objects at the IR boundary
// so RJSF only ever sees primitive field types.

import { RJSFSchema, UiSchema } from '@rjsf/utils';

import { IAfdagDagConfig } from './ir';
import { IOperatorDef, IOperatorParam } from './interfaces';

export interface IFormSpec {
  schema: RJSFSchema;
  uiSchema: UiSchema;
}

const SCHEDULE_PRESETS = [
  'None',
  '@once',
  '@hourly',
  '@daily',
  '@weekly',
  '@monthly'
];

export { SCHEDULE_PRESETS };

function isJsonParam(param: IOperatorParam): boolean {
  return param.widget === 'json' || param.type === 'object';
}

// The NODE form holds the operator's per-task common settings under this nested
// object key (rendered as a "Common settings" fieldset); split back out at the
// IR boundary into `node.common`.
const COMMON_KEY = '__common__';

interface ICommonParamDef {
  label: string;
  type: 'integer' | 'boolean' | 'string';
  help: string;
  enum?: string[];
}

// The fixed set of Airflow per-task common settings (PRD §6.1.3). Each op's
// `commonParams` (from the registry) selects which apply; codegen emits them
// (retry_delay -> timedelta) so they override the DAG defaults.
const COMMON_PARAM_DEFS: Record<string, ICommonParamDef> = {
  retries: {
    label: 'Retries',
    type: 'integer',
    help: 'How many times to retry this task if it fails — overrides the DAG default.'
  },
  retry_delay: {
    label: 'Retry delay (seconds)',
    type: 'integer',
    help: 'Seconds to wait between retries.'
  },
  depends_on_past: {
    label: 'Depends on past',
    type: 'boolean',
    help: 'Only run once this same task succeeded in the previous DAG run.'
  },
  mode: {
    label: 'Sensor mode',
    type: 'string',
    enum: ['poke', 'reschedule'],
    help: '“poke” holds a worker slot while waiting; “reschedule” frees it between checks (better for long waits).'
  },
  poke_interval: {
    label: 'Poke interval (seconds)',
    type: 'integer',
    help: 'How often the sensor checks, in seconds.'
  },
  timeout: {
    label: 'Timeout (seconds)',
    type: 'integer',
    help: 'Give up waiting after this many seconds.'
  }
};

function commonNames(op: IOperatorDef): string[] {
  return (op.commonParams ?? []).filter(name => name in COMMON_PARAM_DEFS);
}

function commonParamSchema(name: string): RJSFSchema {
  const def = COMMON_PARAM_DEFS[name];
  const base: RJSFSchema = { title: def.label, description: def.help };
  if (def.type === 'integer') {
    return { ...base, type: 'integer', minimum: 0 };
  }
  if (def.type === 'boolean') {
    return { ...base, type: 'boolean' };
  }
  return def.enum
    ? { ...base, type: 'string', enum: def.enum }
    : { ...base, type: 'string' };
}

function paramSchema(param: IOperatorParam): RJSFSchema {
  // The registry `help` becomes the JSON-Schema `description`, rendered by the
  // custom DescriptionFieldTemplate as a hoverable `ⓘ` info bubble (AfdagForm).
  const base: RJSFSchema = { title: param.label };
  if (param.help) {
    base.description = param.help;
  }
  if (isJsonParam(param)) {
    return { ...base, type: 'string' }; // edited as JSON text
  }
  if (param.widget === 'code' || param.widget === 'textarea') {
    return { ...base, type: 'string' };
  }
  switch (param.type) {
    case 'integer':
      return { ...base, type: 'integer' };
    case 'number':
      return { ...base, type: 'number' };
    case 'boolean':
      return { ...base, type: 'boolean' };
    default:
      return { ...base, type: 'string' };
  }
}

function paramUi(param: IOperatorParam): UiSchema | undefined {
  if (isJsonParam(param)) {
    return { 'ui:widget': 'json' };
  }
  if (param.widget === 'code') {
    return { 'ui:widget': 'code' };
  }
  if (param.widget === 'textarea') {
    return { 'ui:widget': 'textarea' };
  }
  return undefined;
}

/** RJSF schema + uiSchema for a node: `task_id` plus the operator's params. */
export function nodeForm(op: IOperatorDef): IFormSpec {
  const properties: Record<string, RJSFSchema> = {
    task_id: { type: 'string', title: 'task_id' }
  };
  const required: string[] = ['task_id'];
  const order = ['task_id', ...op.params.map(p => p.name)];
  const uiSchema: UiSchema = {};

  for (const param of op.params) {
    properties[param.name] = paramSchema(param);
    if (param.required) {
      required.push(param.name);
    }
    const ui = paramUi(param);
    if (ui) {
      uiSchema[param.name] = ui;
    }
  }

  // The operator's per-task common settings as a nested "Common settings"
  // fieldset, ordered after the operator params (PRD §6.1.3).
  const names = commonNames(op);
  if (names.length > 0) {
    const commonProps: Record<string, RJSFSchema> = {};
    for (const name of names) {
      commonProps[name] = commonParamSchema(name);
    }
    properties[COMMON_KEY] = {
      type: 'object',
      title: 'Common settings',
      properties: commonProps
    };
    order.push(COMMON_KEY);
  }

  uiSchema['ui:order'] = order;
  return {
    schema: { type: 'object', properties, required },
    uiSchema
  };
}

/** Node IR (task_id + params + common) -> RJSF formData (object params -> JSON text). */
export function nodeToFormData(
  op: IOperatorDef,
  taskId: string,
  params: Record<string, unknown>,
  common: Record<string, unknown> = {}
): Record<string, unknown> {
  const data: Record<string, unknown> = { task_id: taskId };
  for (const param of op.params) {
    const value = params[param.name];
    if (isJsonParam(param)) {
      data[param.name] =
        value === undefined ? '' : JSON.stringify(value, null, 2);
    } else if (value !== undefined) {
      data[param.name] = value;
    }
  }
  const names = commonNames(op);
  if (names.length > 0) {
    const nested: Record<string, unknown> = {};
    for (const name of names) {
      if (common[name] !== undefined) {
        nested[name] = common[name];
      }
    }
    data[COMMON_KEY] = nested;
  }
  return data;
}

/** RJSF formData -> node (JSON text -> object). Returns task_id + params + common.
 * Only explicitly-set common values are kept (a `false` toggle is the default,
 * so it is omitted) — an unset common field inherits the DAG default. */
export function formDataToNode(
  op: IOperatorDef,
  formData: Record<string, unknown>
): {
  task_id: string;
  params: Record<string, unknown>;
  common: Record<string, unknown>;
} {
  const params: Record<string, unknown> = {};
  for (const param of op.params) {
    const value = formData[param.name];
    if (value === undefined) {
      continue;
    }
    if (isJsonParam(param)) {
      params[param.name] = parseJsonOr(value, {});
    } else {
      params[param.name] = value;
    }
  }

  const common: Record<string, unknown> = {};
  const nested = (formData[COMMON_KEY] as Record<string, unknown>) ?? {};
  for (const name of commonNames(op)) {
    const value = nested[name];
    if (value === undefined || value === null || value === '') {
      continue;
    }
    if (typeof value === 'boolean') {
      if (value) {
        common[name] = true; // false == the Airflow default, so omit it
      }
      continue;
    }
    common[name] = value;
  }

  return { task_id: String(formData.task_id ?? ''), params, common };
}

function parseJsonOr(value: unknown, fallback: unknown): unknown {
  if (typeof value !== 'string' || value.trim() === '') {
    return fallback;
  }
  try {
    return JSON.parse(value);
  } catch {
    return value; // keep the raw text so the user doesn't lose edits
  }
}

/**
 * Parse the comma-separated `tags` text field into a trimmed, de-duplicated
 * list (an empty input yields `[]`). Tags are edited as plain text because
 * RJSF's default array widget renders poorly without a Bootstrap theme.
 */
function parseTags(value: unknown): string[] {
  const seen = new Set<string>();
  for (const tag of String(value ?? '').split(',')) {
    const trimmed = tag.trim();
    if (trimmed) {
      seen.add(trimmed);
    }
  }
  return Array.from(seen);
}

// --------------------------------------------------------------------------- //
// DAG form (fixed schema)
// --------------------------------------------------------------------------- //
export function dagForm(): IFormSpec {
  const schema: RJSFSchema = {
    type: 'object',
    required: ['dag_id'],
    properties: {
      dag_id: {
        type: 'string',
        title: 'dag_id',
        description:
          'Renaming the DAG id is a guided migration — use “Rename DAG id…” in the toolbar.'
      },
      description: {
        type: 'string',
        title: 'description',
        description:
          'A free-text summary shown next to the DAG in the Airflow UI.'
      },
      schedule: {
        type: 'string',
        title: 'schedule',
        description:
          'How often the DAG runs — a preset like @daily, a cron expression (0 9 * * *), or None for manual / triggered-only.'
      },
      start_date: {
        type: 'string',
        title: 'start_date',
        format: 'date',
        description:
          'The first date the scheduler considers; runs are never created before it. With catchup off (the default) only the most recent interval runs; with catchup on, every interval since this date is back-filled.'
      },
      catchup: {
        type: 'boolean',
        title: 'catchup',
        description:
          'When on, Airflow back-fills every missed interval between start_date and now. Default off — most DAGs only run going forward.'
      },
      retries: {
        type: 'integer',
        title: 'retries',
        minimum: 0,
        description:
          'How many times a failed task is retried (the DAG default; a task can override it in Common settings).'
      },
      retry_delay_seconds: {
        type: 'integer',
        title: 'retry_delay (seconds)',
        minimum: 0,
        description: 'Seconds to wait between retry attempts.'
      },
      tags: {
        type: 'string',
        title: 'tags',
        description:
          'Comma-separated labels for grouping/filtering DAGs in the Airflow UI, e.g. studio, etl.'
      },
      owner: {
        type: 'string',
        title: 'owner',
        description:
          'The Airflow owner attributed to every task; shown in the UI and usable as a filter.'
      },
      params: {
        type: 'string',
        title: 'params (JSON)',
        description:
          'DAG-level runtime parameters as a JSON object; surfaced in the Trigger form and referenced as {{ params.x }}.'
      },
      default_args: {
        type: 'string',
        title: 'default_args (JSON)',
        description:
          'Defaults applied to every task as a JSON object (e.g. retries, retry_delay, owner). Per-task Common settings override these.'
      }
    }
  };
  const uiSchema: UiSchema = {
    'ui:order': [
      'dag_id',
      'description',
      'schedule',
      'start_date',
      'catchup',
      'retries',
      'retry_delay_seconds',
      'tags',
      'owner',
      'params',
      'default_args'
    ],
    dag_id: { 'ui:readonly': true },
    description: { 'ui:widget': 'textarea' },
    schedule: { 'ui:widget': 'schedule' },
    tags: { 'ui:placeholder': 'studio, etl' },
    params: { 'ui:widget': 'json' },
    default_args: { 'ui:widget': 'json' }
  };
  return { schema, uiSchema };
}

export function dagToFormData(dag: IAfdagDagConfig): Record<string, unknown> {
  return {
    dag_id: dag.dag_id,
    description: dag.description ?? '',
    schedule: dag.schedule ?? 'None',
    start_date: dag.start_date ?? '',
    catchup: dag.catchup ?? false,
    retries: dag.retries ?? 0,
    retry_delay_seconds: dag.retry_delay_seconds ?? 300,
    tags: (dag.tags ?? []).join(', '),
    owner: dag.owner ?? '',
    params: dag.params ? JSON.stringify(dag.params, null, 2) : '',
    default_args: dag.default_args
      ? JSON.stringify(dag.default_args, null, 2)
      : ''
  };
}

export function formDataToDag(
  formData: Record<string, unknown>
): Partial<IAfdagDagConfig> {
  const schedule = String(formData.schedule ?? '');
  return {
    dag_id: String(formData.dag_id ?? ''),
    description: String(formData.description ?? ''),
    schedule: schedule === 'None' || schedule === '' ? null : schedule,
    start_date: String(formData.start_date ?? ''),
    catchup: Boolean(formData.catchup),
    retries: Number(formData.retries ?? 0),
    retry_delay_seconds: Number(formData.retry_delay_seconds ?? 300),
    tags: parseTags(formData.tags),
    owner: String(formData.owner ?? ''),
    params: parseJsonOr(formData.params, {}) as Record<string, unknown>,
    default_args: parseJsonOr(formData.default_args, {}) as Record<
      string,
      unknown
    >
  };
}
