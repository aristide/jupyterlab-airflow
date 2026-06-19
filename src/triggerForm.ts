// Pure mapping between a DAG's serialized `params` (from `GET /dags/{id}/details`)
// and the manager's trigger-with-conf form (PRD §6.6 / §15.10).
//
// Airflow serializes each param as `{ value (default), description, schema }`,
// where `schema` is a JSON-Schema fragment (`type`, `enum`, `format`,
// `minimum`/`maximum`). We project that onto a small set of typed controls and,
// on submit, build the run `conf` back out.

import { IDagParam, IDagParamSchema } from './interfaces';

export type ConfFieldKind =
  | 'enum'
  | 'bool'
  | 'int'
  | 'number'
  | 'date'
  | 'datetime'
  | 'text'
  | 'json';

export interface IConfField {
  name: string;
  kind: ConfFieldKind;
  /** The param schema admits `null` (e.g. `type: ["null", "string"]`). */
  nullable: boolean;
  description?: string | null;
  /** Allowed values when the param is an enum (rendered as a dropdown). */
  enumValues?: unknown[];
  default: unknown;
  min?: number;
  max?: number;
}

// The editable per-field draft: a string for text/number/date/json controls, a
// boolean for checkboxes, or the chosen enum index (as a string) for dropdowns.
export type ConfDraft = Record<string, string | boolean>;

export interface IBuildResult {
  conf: Record<string, unknown>;
  /** Per-field validation messages (bad JSON / number); blocks submit. */
  errors: Record<string, string>;
}

const typeList = (schema: IDagParamSchema): string[] => {
  const t = schema.type;
  if (Array.isArray(t)) {
    return t.map(String);
  }
  return t === undefined ? [] : [String(t)];
};

const isNullish = (value: unknown): boolean =>
  value === null || value === undefined;

// When the schema has no `type` (Airflow emits `{}` for a bare default), fall
// back to the default value's own runtime type so plain defaults still get a
// sensible control.
const inferType = (value: unknown): string | undefined => {
  if (typeof value === 'boolean') {
    return 'boolean';
  }
  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'integer' : 'number';
  }
  if (typeof value === 'string') {
    return 'string';
  }
  if (value !== null && typeof value === 'object') {
    return 'object';
  }
  return undefined;
};

export function classifyParam(name: string, param: IDagParam): IConfField {
  const schema = param.schema ?? {};
  const types = typeList(schema);
  const nullable = types.includes('null');
  const field: IConfField = {
    name,
    kind: 'text',
    nullable,
    description: param.description,
    default: param.value,
    min: typeof schema.minimum === 'number' ? schema.minimum : undefined,
    max: typeof schema.maximum === 'number' ? schema.maximum : undefined
  };

  if (Array.isArray(schema.enum) && schema.enum.length > 0) {
    return { ...field, kind: 'enum', enumValues: schema.enum };
  }

  const base = types.find(t => t !== 'null') ?? inferType(param.value);
  switch (base) {
    case 'boolean':
      return { ...field, kind: 'bool' };
    case 'integer':
      return { ...field, kind: 'int' };
    case 'number':
      return { ...field, kind: 'number' };
    case 'object':
    case 'array':
      return { ...field, kind: 'json' };
    case 'string':
      if (schema.format === 'date') {
        return { ...field, kind: 'date' };
      }
      if (schema.format === 'date-time') {
        return { ...field, kind: 'datetime' };
      }
      return { ...field, kind: 'text' };
    default:
      return { ...field, kind: 'text' };
  }
}

export function classifyParams(
  params: Record<string, IDagParam>
): IConfField[] {
  return Object.keys(params).map(name => classifyParam(name, params[name]));
}

// The select stores the chosen enum *index*; -1 is the nullable "none" option.
const enumIndex = (field: IConfField): number => {
  const values = field.enumValues ?? [];
  const idx = values.findIndex(
    v => JSON.stringify(v) === JSON.stringify(field.default)
  );
  if (idx >= 0) {
    return idx;
  }
  return field.nullable ? -1 : 0;
};

const pad2 = (n: number): string => String(n).padStart(2, '0');

const parseDate = (value: unknown): Date | null => {
  if (isNullish(value)) {
    return null;
  }
  const d = new Date(String(value));
  return Number.isNaN(d.getTime()) ? null : d;
};

// `<input type="date">` value — the leading `YYYY-MM-DD`. We slice rather than
// reformat a parsed Date so a bare `date` string (Airflow's usual form) never
// shifts a day across the UTC/local boundary.
const toDateInput = (value: unknown): string => {
  const s = isNullish(value) ? '' : String(value);
  const m = s.match(/^\d{4}-\d{2}-\d{2}/);
  return m ? m[0] : s;
};

// `<input type="datetime-local">` value — local wall-clock `YYYY-MM-DDTHH:mm`,
// no offset. Airflow serializes a `date-time` default as an offset-bearing ISO
// string the control would otherwise reject (rendering blank), so reformat it.
const toDatetimeLocal = (value: unknown): string => {
  const d = parseDate(value);
  if (!d) {
    return isNullish(value) ? '' : String(value);
  }
  return (
    `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}` +
    `T${pad2(d.getHours())}:${pad2(d.getMinutes())}`
  );
};

export function initialFieldValue(field: IConfField): string | boolean {
  switch (field.kind) {
    case 'bool':
      return field.default === true;
    case 'enum':
      return String(enumIndex(field));
    case 'json':
      return isNullish(field.default)
        ? ''
        : JSON.stringify(field.default, null, 2);
    case 'date':
      return toDateInput(field.default);
    case 'datetime':
      return toDatetimeLocal(field.default);
    default:
      // text / int / number — string-encoded for the input.
      return isNullish(field.default) ? '' : String(field.default);
  }
}

export function initialDraft(fields: IConfField[]): ConfDraft {
  const draft: ConfDraft = {};
  for (const field of fields) {
    draft[field.name] = initialFieldValue(field);
  }
  return draft;
}

// Build the run `conf` from the draft. A cleared field falls back to the param
// default (key omitted) unless the schema is nullable, in which case it is sent
// as an explicit `null`.
export function buildConf(
  fields: IConfField[],
  draft: ConfDraft
): IBuildResult {
  const conf: Record<string, unknown> = {};
  const errors: Record<string, string> = {};

  for (const field of fields) {
    const raw = draft[field.name];
    switch (field.kind) {
      case 'bool':
        conf[field.name] = raw === true;
        break;
      case 'enum': {
        const idx = parseInt(String(raw), 10);
        if (idx < 0) {
          if (field.nullable) {
            conf[field.name] = null;
          }
        } else {
          conf[field.name] = (field.enumValues ?? [])[idx];
        }
        break;
      }
      case 'int': {
        const s = String(raw).trim();
        if (s === '') {
          if (field.nullable) {
            conf[field.name] = null;
          }
        } else {
          // `parseInt` would silently truncate `1.5` → `1`; require a whole
          // number instead so the conf matches what the user typed.
          const n = Number(s);
          if (!Number.isInteger(n)) {
            errors[field.name] = 'Enter a whole number.';
          } else {
            conf[field.name] = n;
          }
        }
        break;
      }
      case 'number': {
        const s = String(raw).trim();
        if (s === '') {
          if (field.nullable) {
            conf[field.name] = null;
          }
        } else {
          const n = Number(s);
          if (Number.isNaN(n)) {
            errors[field.name] = 'Enter a valid number.';
          } else {
            conf[field.name] = n;
          }
        }
        break;
      }
      case 'json': {
        const s = String(raw).trim();
        if (s === '') {
          if (field.nullable) {
            conf[field.name] = null;
          }
        } else {
          try {
            conf[field.name] = JSON.parse(s);
          } catch {
            errors[field.name] = 'Invalid JSON.';
          }
        }
        break;
      }
      case 'datetime': {
        const s = String(raw).trim();
        if (s === '') {
          // A cleared field falls back to the param default (or explicit null
          // when nullable) — never an empty string, which Airflow's `date-time`
          // format validation rejects.
          if (field.nullable) {
            conf[field.name] = null;
          }
        } else {
          // The control yields a local, offset-less `YYYY-MM-DDTHH:mm`; Airflow
          // requires an offset-bearing ISO string (same as the logical_date
          // pick path), so normalize to UTC.
          const d = new Date(s);
          if (Number.isNaN(d.getTime())) {
            errors[field.name] = 'Enter a valid date and time.';
          } else {
            conf[field.name] = d.toISOString();
          }
        }
        break;
      }
      case 'date': {
        // `YYYY-MM-DD` already satisfies the `date` format; a cleared field
        // falls back to the default (or null) rather than the invalid empty
        // string.
        const s = String(raw).trim();
        if (s === '') {
          if (field.nullable) {
            conf[field.name] = null;
          }
        } else {
          conf[field.name] = s;
        }
        break;
      }
      default: {
        // text — an empty string can be a legitimate value, so keep it.
        const s = String(raw);
        if (s === '' && field.nullable) {
          conf[field.name] = null;
        } else {
          conf[field.name] = s;
        }
      }
    }
  }

  return { conf, errors };
}
