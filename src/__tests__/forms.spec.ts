import {
  dagForm,
  dagToFormData,
  formDataToDag,
  formDataToNode,
  nodeForm,
  nodeToFormData
} from '../forms';
import { IOperatorDef } from '../interfaces';

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
    expect(Object.keys(props)).toEqual([
      'task_id',
      'bash_command',
      'env',
      'code'
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
    expect(formData.params).toBe(JSON.stringify({ a: 1 }, null, 2));
    const dag = formDataToDag(formData);
    expect(dag.schedule).toBe('@daily');
    expect(dag.owner).toBe('dana');
    expect(dag.params).toEqual({ a: 1 });
  });

  it('builds a schema with the schedule widget wired', () => {
    const { uiSchema } = dagForm();
    expect(uiSchema.schedule).toEqual({ 'ui:widget': 'schedule' });
    expect(uiSchema.params).toEqual({ 'ui:widget': 'json' });
  });
});
