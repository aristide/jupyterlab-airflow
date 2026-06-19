import { IDagParam } from '../interfaces';
import {
  buildConf,
  classifyParam,
  classifyParams,
  initialDraft,
  initialFieldValue
} from '../triggerForm';

const param = (
  value: unknown,
  schema: Record<string, unknown> = {},
  description: string | null = null
): IDagParam => ({ value, description, schema });

describe('classifyParam', () => {
  it('maps an enum schema to a dropdown', () => {
    const f = classifyParam(
      'region',
      param('eu-west-1', { type: 'string', enum: ['eu-west-1', 'us-east-1'] })
    );
    expect(f.kind).toBe('enum');
    expect(f.enumValues).toEqual(['eu-west-1', 'us-east-1']);
  });

  it('maps schema types to controls', () => {
    expect(classifyParam('b', param(true, { type: 'boolean' })).kind).toBe(
      'bool'
    );
    expect(classifyParam('i', param(5, { type: 'integer' })).kind).toBe('int');
    expect(classifyParam('n', param(0.5, { type: 'number' })).kind).toBe(
      'number'
    );
    expect(
      classifyParam(
        'd',
        param('2026-06-15', { type: 'string', format: 'date' })
      ).kind
    ).toBe('date');
    expect(
      classifyParam(
        'dt',
        param('2026-06-15T10:30:00Z', { type: 'string', format: 'date-time' })
      ).kind
    ).toBe('datetime');
    expect(classifyParam('s', param('hi', { type: 'string' })).kind).toBe(
      'text'
    );
    expect(classifyParam('o', param({ a: 1 }, { type: 'object' })).kind).toBe(
      'json'
    );
  });

  it('infers the control from the default when the schema has no type', () => {
    // Airflow emits `schema: {}` for a bare default value.
    expect(classifyParam('s', param('hi')).kind).toBe('text');
    expect(classifyParam('i', param(5)).kind).toBe('int');
    expect(classifyParam('f', param(1.5)).kind).toBe('number');
    expect(classifyParam('b', param(true)).kind).toBe('bool');
  });

  it('detects a nullable union type', () => {
    const f = classifyParam('o', param(null, { type: ['null', 'string'] }));
    expect(f.kind).toBe('text');
    expect(f.nullable).toBe(true);
  });

  it('carries min/max and description', () => {
    const f = classifyParam(
      't',
      param(0.5, { type: 'number', minimum: 0, maximum: 1 }, 'A threshold')
    );
    expect(f.min).toBe(0);
    expect(f.max).toBe(1);
    expect(f.description).toBe('A threshold');
  });
});

describe('initialFieldValue', () => {
  it('selects the enum index of the default', () => {
    const f = classifyParam(
      'region',
      param('us-east-1', { type: 'string', enum: ['eu-west-1', 'us-east-1'] })
    );
    expect(initialFieldValue(f)).toBe('1');
  });

  it('pretty-prints object defaults as JSON', () => {
    const f = classifyParam('o', param({ a: 1 }, { type: 'object' }));
    expect(initialFieldValue(f)).toBe('{\n  "a": 1\n}');
  });

  it('renders a null default as an empty string', () => {
    const f = classifyParam('o', param(null, { type: ['null', 'string'] }));
    expect(initialFieldValue(f)).toBe('');
  });

  it('keeps a date default as YYYY-MM-DD for the date input', () => {
    const f = classifyParam(
      'd',
      param('2026-06-15', { type: 'string', format: 'date' })
    );
    expect(initialFieldValue(f)).toBe('2026-06-15');
  });

  it('normalizes an offset-bearing date-time default to the local-input shape', () => {
    // Airflow serializes a date-time default with an offset/Z, which the
    // datetime-local control would reject (render blank) — it must be reshaped.
    const f = classifyParam(
      'dt',
      param('2026-06-15T10:30:00Z', { type: 'string', format: 'date-time' })
    );
    expect(initialFieldValue(f)).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/);
  });
});

describe('buildConf', () => {
  const fields = classifyParams({
    region: param('eu-west-1', {
      type: 'string',
      enum: ['eu-west-1', 'us-east-1']
    }),
    threshold: param(0.5, { type: 'number' }),
    count: param(3, { type: 'integer' }),
    dry_run: param(true, { type: 'boolean' }),
    label: param('hello', { type: 'string' }),
    tags: param({ a: 1 }, { type: 'object' })
  });

  it('round-trips defaults back into conf', () => {
    const { conf, errors } = buildConf(fields, initialDraft(fields));
    expect(errors).toEqual({});
    expect(conf).toEqual({
      region: 'eu-west-1',
      threshold: 0.5,
      count: 3,
      dry_run: true,
      label: 'hello',
      tags: { a: 1 }
    });
  });

  it('applies edited values with the right types', () => {
    const draft = initialDraft(fields);
    draft.region = '1'; // pick us-east-1
    draft.threshold = '0.75';
    draft.count = '10';
    draft.dry_run = false;
    draft.label = 'world';
    const { conf } = buildConf(fields, draft);
    expect(conf.region).toBe('us-east-1');
    expect(conf.threshold).toBe(0.75);
    expect(conf.count).toBe(10);
    expect(conf.dry_run).toBe(false);
    expect(conf.label).toBe('world');
  });

  it('flags an invalid number and invalid JSON', () => {
    const draft = initialDraft(fields);
    draft.threshold = 'abc';
    draft.tags = '{not json}';
    const { errors } = buildConf(fields, draft);
    expect(errors.threshold).toBeTruthy();
    expect(errors.tags).toBeTruthy();
  });

  it('sends explicit null for a cleared nullable field', () => {
    const nullable = classifyParams({
      opt: param(null, { type: ['null', 'string'] })
    });
    const { conf } = buildConf(nullable, { opt: '' });
    expect(conf.opt).toBeNull();
  });

  it('omits a cleared non-nullable number so Airflow uses the default', () => {
    const { conf } = buildConf(fields, { ...initialDraft(fields), count: '' });
    expect('count' in conf).toBe(false);
  });

  it('rejects a non-integer in an int field instead of truncating it', () => {
    const { conf, errors } = buildConf(fields, {
      ...initialDraft(fields),
      count: '1.5'
    });
    expect(errors.count).toBeTruthy();
    expect('count' in conf).toBe(false);
  });

  it('converts a datetime field to an offset-bearing ISO string', () => {
    const dt = classifyParams({
      when: param('2026-06-15T10:30:00Z', {
        type: 'string',
        format: 'date-time'
      })
    });
    const { conf, errors } = buildConf(dt, { when: '2026-06-15T10:30' });
    expect(errors).toEqual({});
    // Airflow's date-time format validation requires an offset/Z.
    expect(typeof conf.when).toBe('string');
    expect(conf.when).toMatch(/(Z|[+-]\d{2}:\d{2})$/);
    expect(Number.isNaN(new Date(conf.when as string).getTime())).toBe(false);
  });

  it('omits a cleared non-nullable date rather than sending an invalid empty string', () => {
    const dateField = classifyParams({
      d: param('2026-06-15', { type: 'string', format: 'date' })
    });
    const { conf } = buildConf(dateField, { d: '' });
    expect('d' in conf).toBe(false);
  });
});
