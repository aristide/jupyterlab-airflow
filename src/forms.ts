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

function paramSchema(param: IOperatorParam): RJSFSchema {
  // The registry `help` becomes the JSON-Schema `description`, which RJSF renders
  // as inline field help (`.field-description`) — contextual learning per field.
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
  const uiSchema: UiSchema = {
    'ui:order': ['task_id', ...op.params.map(p => p.name)]
  };

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

  return {
    schema: { type: 'object', properties, required },
    uiSchema
  };
}

/** Node IR (task_id + params) -> RJSF formData (object params -> JSON text). */
export function nodeToFormData(
  op: IOperatorDef,
  taskId: string,
  params: Record<string, unknown>
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
  return data;
}

/** RJSF formData -> node params (JSON text -> object). Returns task_id + params. */
export function formDataToNode(
  op: IOperatorDef,
  formData: Record<string, unknown>
): { task_id: string; params: Record<string, unknown> } {
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
  return { task_id: String(formData.task_id ?? ''), params };
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
      description: { type: 'string', title: 'description' },
      schedule: { type: 'string', title: 'schedule' },
      start_date: { type: 'string', title: 'start_date', format: 'date' },
      catchup: { type: 'boolean', title: 'catchup' },
      retries: { type: 'integer', title: 'retries', minimum: 0 },
      retry_delay_seconds: {
        type: 'integer',
        title: 'retry_delay (seconds)',
        minimum: 0
      },
      tags: {
        type: 'string',
        title: 'tags',
        description: 'Comma-separated, e.g. studio, etl'
      },
      owner: { type: 'string', title: 'owner' },
      params: { type: 'string', title: 'params (JSON)' },
      default_args: { type: 'string', title: 'default_args (JSON)' }
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
