import {
  dagForm,
  dagToFormData,
  formDataToDag,
  formDataToNode,
  nodeForm,
  notifierForm,
  nodeToFormData
} from '../forms';
import { INotifierDef, IOperatorDef } from '../interfaces';

const bashOp: IOperatorDef = {
  id: 'bash',
  label: 'Bash operator',
  category: 'Python/Bash',
  taskIdPrefix: 'bash',
  params: [
    {
      name: 'bash_command',
      label: 'Bash Command',
      required: true,
      widget: 'textarea'
    },
    {
      name: 'env',
      label: 'Env',
      required: false,
      widget: 'json',
      type: 'object'
    },
    { name: 'code', label: 'Code', required: false, widget: 'code' }
  ]
};

describe('registry -> RJSF schema (nodeForm)', () => {
  it('includes task_id plus operator params, with required + widgets', () => {
    const { schema, uiSchema } = nodeForm(bashOp);
    const props = schema.properties as Record<string, { type?: string }>;
    // task_id + the operator's params, then the always-present Assets section.
    expect(Object.keys(props)).toEqual([
      'task_id',
      'bash_command',
      'env',
      'code',
      '__assets__'
    ]);
    expect(schema.required).toContain('task_id');
    expect(schema.required).toContain('bash_command');
    expect(schema.required).not.toContain('env');
    // JSON/code fields are edited as text.
    expect(props.env.type).toBe('string');
    expect(props.code.type).toBe('string');
    expect(uiSchema.env).toEqual({ 'ui:widget': 'json' });
    expect(uiSchema.code).toEqual({ 'ui:widget': 'code' });
    expect(uiSchema.bash_command).toEqual({ 'ui:widget': 'textarea' });
  });

  it('maps a param `help` to the schema description (inline field help)', () => {
    const op: IOperatorDef = {
      id: 'x',
      label: 'X',
      category: 'C',
      taskIdPrefix: 'x',
      params: [
        {
          name: 'cmd',
          label: 'Cmd',
          required: true,
          widget: 'text',
          help: 'Run this command.'
        },
        { name: 'plain', label: 'Plain', required: false, widget: 'text' }
      ]
    };
    const { schema } = nodeForm(op);
    const props = schema.properties as Record<string, { description?: string }>;
    expect(props.cmd.description).toBe('Run this command.');
    expect(props.plain.description).toBeUndefined();
  });
});

describe('node IR <-> form data', () => {
  it('stringifies object params for the form and parses them back', () => {
    const params = { bash_command: 'echo hi', env: { A: '1' } };
    const formData = nodeToFormData(bashOp, 'extract', params);
    expect(formData.task_id).toBe('extract');
    expect(formData.bash_command).toBe('echo hi');
    expect(typeof formData.env).toBe('string'); // JSON text
    expect(JSON.parse(formData.env as string)).toEqual({ A: '1' });

    const back = formDataToNode(bashOp, formData);
    expect(back.task_id).toBe('extract');
    expect(back.params.bash_command).toBe('echo hi');
    expect(back.params.env).toEqual({ A: '1' }); // parsed back to an object
  });

  it('keeps raw text for invalid JSON rather than dropping the edit', () => {
    const back = formDataToNode(bashOp, {
      task_id: 't',
      bash_command: 'x',
      env: '{ not json'
    });
    expect(back.params.env).toBe('{ not json');
  });
});

const sensorOp: IOperatorDef = {
  id: 'file_sensor',
  label: 'File sensor',
  category: 'Sensors',
  taskIdPrefix: 'fs',
  params: [
    { name: 'filepath', label: 'File path', required: true, widget: 'text' }
  ],
  commonParams: [
    'retries',
    'retry_delay',
    'depends_on_past',
    'mode',
    'poke_interval',
    'timeout'
  ]
};

describe('common params (NODE "Common settings")', () => {
  it('adds a __common__ fieldset with the declared common params, ordered last', () => {
    const { schema, uiSchema } = nodeForm(sensorOp);
    const props = schema.properties as Record<string, any>;
    expect(props.__common__.type).toBe('object');
    expect(props.__common__.title).toBe('Common settings');
    expect(Object.keys(props.__common__.properties)).toEqual(
      sensorOp.commonParams
    );
    expect(props.__common__.properties.mode.enum).toEqual([
      'poke',
      'reschedule'
    ]);
    expect(props.__common__.properties.retries.type).toBe('integer');
    const order = uiSchema['ui:order'] as string[];
    // Common settings come after the params; the Assets section is ordered last.
    expect(order.indexOf('__common__')).toBeGreaterThan(order.indexOf('mode') - 1);
    expect(order[order.length - 1]).toBe('__assets__');
    expect(order.indexOf('__common__')).toBeLessThan(order.indexOf('__assets__'));
  });

  it('round-trips common values; omits false booleans and blanks', () => {
    const fd = nodeToFormData(
      sensorOp,
      'wait',
      { filepath: '/d' },
      { mode: 'reschedule', poke_interval: 30 }
    );
    expect((fd.__common__ as any).mode).toBe('reschedule');
    expect((fd.__common__ as any).poke_interval).toBe(30);

    const back = formDataToNode(sensorOp, {
      task_id: 'wait',
      filepath: '/d',
      __common__: {
        mode: 'reschedule',
        poke_interval: 30,
        depends_on_past: false, // default -> omitted
        retries: '' // blank -> omitted
      }
    });
    expect(back.common).toEqual({ mode: 'reschedule', poke_interval: 30 });
  });

  it('omits the __common__ section for an op with no commonParams', () => {
    const { schema } = nodeForm(bashOp);
    expect((schema.properties as any).__common__).toBeUndefined();
    expect(
      formDataToNode(bashOp, { task_id: 't', bash_command: 'x' }).common
    ).toEqual({});
  });
});

describe('asset inlets/outlets (NODE "Assets") + DAG schedule_assets, PRD §6.9', () => {
  it('adds an __assets__ fieldset (inlets/outlets) to every op, ordered last', () => {
    const { schema, uiSchema } = nodeForm(bashOp); // bashOp has no commonParams
    const props = schema.properties as Record<string, any>;
    expect(props.__assets__.type).toBe('object');
    expect(Object.keys(props.__assets__.properties)).toEqual([
      'inlets',
      'outlets'
    ]);
    const order = uiSchema['ui:order'] as string[];
    expect(order[order.length - 1]).toBe('__assets__');
  });

  it('round-trips inlets/outlets as comma-separated text (trim + de-dup)', () => {
    const fd = nodeToFormData(
      bashOp,
      'build',
      { bash_command: 'x' },
      {},
      { outlets: ['s3://lake/o.csv', 'curated'], inlets: [] }
    );
    expect((fd.__assets__ as any).outlets).toBe('s3://lake/o.csv, curated');
    expect((fd.__assets__ as any).inlets).toBe('');

    const back = formDataToNode(bashOp, {
      task_id: 'build',
      bash_command: 'x',
      __assets__: { outlets: 'a, b ,a', inlets: '  ' } // dup + blank-only
    });
    expect(back.outlets).toEqual(['a', 'b']);
    expect(back.inlets).toEqual([]);
  });

  it('round-trips DAG schedule_assets as comma-separated text', () => {
    expect(
      dagToFormData({ dag_id: 'd', schedule_assets: ['orders', 's3://x'] })
        .schedule_assets
    ).toBe('orders, s3://x');
    expect(
      formDataToDag({ dag_id: 'd', schedule_assets: 'orders, s3://x ,orders' })
        .schedule_assets
    ).toEqual(['orders', 's3://x']);
    expect(
      formDataToDag({ dag_id: 'd', schedule_assets: '' }).schedule_assets
    ).toEqual([]);
    // The DAG form exposes the field with a placeholder.
    const { schema } = dagForm();
    expect((schema.properties as any).schedule_assets.type).toBe('string');
  });
});

describe('DAG form data', () => {
  it('maps schedule None -> null and parses JSON fields', () => {
    const formData = {
      dag_id: 'd',
      schedule: 'None',
      catchup: true,
      retries: 2,
      retry_delay_seconds: 60,
      tags: ['a'],
      params: '{"x": 1}',
      default_args: ''
    };
    const dag = formDataToDag(formData);
    expect(dag.dag_id).toBe('d');
    expect(dag.schedule).toBeNull();
    expect(dag.catchup).toBe(true);
    expect(dag.params).toEqual({ x: 1 });
    expect(dag.default_args).toEqual({});
  });

  it('round-trips a populated dag config', () => {
    const formData = dagToFormData({
      dag_id: 'etl',
      schedule: '@daily',
      start_date: '2026-01-01',
      catchup: false,
      retries: 1,
      retry_delay_seconds: 300,
      tags: ['studio'],
      owner: 'dana',
      params: { a: 1 }
    });
    expect(formData.schedule).toBe('@daily');
    expect(formData.tags).toBe('studio');
    expect(formData.params).toBe(JSON.stringify({ a: 1 }, null, 2));
    const dag = formDataToDag(formData);
    expect(dag.schedule).toBe('@daily');
    expect(dag.owner).toBe('dana');
    expect(dag.tags).toEqual(['studio']);
    expect(dag.params).toEqual({ a: 1 });
  });

  it('edits tags as comma-separated text (trim + de-dup, empty -> [])', () => {
    // IR array -> comma-separated string for the form.
    expect(dagToFormData({ dag_id: 'd', tags: ['a', 'b'] }).tags).toBe('a, b');
    // Form string -> trimmed, de-duplicated array for the IR.
    expect(
      formDataToDag({ dag_id: 'd', tags: 'studio, etl ,studio' }).tags
    ).toEqual(['studio', 'etl']);
    expect(formDataToDag({ dag_id: 'd', tags: '' }).tags).toEqual([]);
  });

  it('builds a schema with the schedule widget and a string tags field', () => {
    const { schema, uiSchema } = dagForm();
    expect(uiSchema.schedule).toEqual({ 'ui:widget': 'schedule' });
    expect(uiSchema.params).toEqual({ 'ui:widget': 'json' });
    // tags is a plain string field (no RJSF array widget).
    const props = schema.properties as Record<string, { type?: string }>;
    expect(props.tags.type).toBe('string');
  });

  it('gives every DAG field help text (the ⓘ info-bubble content, PRD §6.1.3)', () => {
    const { schema, uiSchema } = dagForm();
    const props = schema.properties as Record<string, { description?: string }>;
    // Every field in the form order must carry a non-empty description so the
    // DescriptionFieldTemplate can render an info bubble for it.
    const order = uiSchema['ui:order'] as string[];
    for (const field of order) {
      expect(typeof props[field].description).toBe('string');
      expect((props[field].description as string).length).toBeGreaterThan(0);
    }
    // A couple of specific ones, to guard the wording source.
    expect(props.schedule.description).toMatch(/how often/i);
    expect(props.catchup.description).toMatch(/back-fill/i);
  });
});

describe('notifier form (notifierForm, PRD §6.8)', () => {
  const smtp: INotifierDef = {
    id: 'smtp',
    label: 'Email (SMTP)',
    params: [
      { name: 'to', label: 'To', required: true, widget: 'text', help: 'Recipient' },
      { name: 'subject', label: 'Subject', required: false, widget: 'text' }
    ]
  };

  it('builds a schema from the notifier params (no task_id / common section)', () => {
    const { schema, uiSchema } = notifierForm(smtp);
    const props = schema.properties as Record<string, { description?: string }>;
    expect(Object.keys(props)).toEqual(['to', 'subject']);
    expect(schema.required).toEqual(['to']);
    expect(uiSchema['ui:order']).toEqual(['to', 'subject']);
    // Per-param help flows to the field description (the ⓘ bubble).
    expect(props.to.description).toBe('Recipient');
  });
});
