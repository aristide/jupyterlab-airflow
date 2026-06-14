// A small placeholder operator catalogue for the scaffold. In later milestones
// this is replaced by the YAML operator registry served from the Jupyter server
// extension (GET operators), which also drives server-side Jinja2 codegen.

export type OperatorWidget = 'text' | 'textarea' | 'code' | 'json';

export interface IOperatorParam {
  name: string;
  label: string;
  required?: boolean;
  widget?: OperatorWidget;
}

export interface IOperatorDef {
  id: string;
  label: string;
  category: string;
  taskIdPrefix: string;
  params: IOperatorParam[];
}

export const OPERATORS: IOperatorDef[] = [
  {
    id: 'empty',
    label: 'Empty operator',
    category: 'Flow Control',
    taskIdPrefix: 'empty',
    params: []
  },
  {
    id: 'bash',
    label: 'Bash operator',
    category: 'Python / Bash',
    taskIdPrefix: 'bash',
    params: [
      {
        name: 'bash_command',
        label: 'Bash Command',
        required: true,
        widget: 'textarea'
      }
    ]
  },
  {
    id: 'python_task',
    label: 'Python @task',
    category: 'Python / Bash',
    taskIdPrefix: 'task',
    params: [
      { name: 'code', label: 'Python code', required: true, widget: 'code' }
    ]
  },
  {
    id: 'branch',
    label: 'Branch operator',
    category: 'Flow Control',
    taskIdPrefix: 'branch',
    params: [
      {
        name: 'code',
        label: 'Branch code (return a task_id)',
        required: true,
        widget: 'code'
      }
    ]
  },
  {
    id: 'trigger_dagrun',
    label: 'Trigger DAG run',
    category: 'Flow Control',
    taskIdPrefix: 'trigger',
    params: [
      {
        name: 'trigger_dag_id',
        label: 'DAG to trigger',
        required: true,
        widget: 'text'
      }
    ]
  }
];

const OPERATOR_INDEX: Record<string, IOperatorDef> = {};
for (const operator of OPERATORS) {
  OPERATOR_INDEX[operator.id] = operator;
}

export function getOperator(id: string): IOperatorDef | undefined {
  return OPERATOR_INDEX[id];
}

export interface IValidationResult {
  valid: boolean;
  missing: string[];
}

/**
 * Client-side, instant required-field validation for a node. The authoritative
 * check (parse / DagBag) happens server-side before deploy.
 */
export function validateNodeParams(
  opId: string,
  params: Record<string, unknown>
): IValidationResult {
  const def = getOperator(opId);
  if (!def) {
    return { valid: false, missing: ['unknown operator'] };
  }
  const missing = def.params
    .filter(param => param.required)
    .filter(param => {
      const value = params[param.name];
      return (
        value === undefined || value === null || String(value).trim() === ''
      );
    })
    .map(param => param.label);
  return { valid: missing.length === 0, missing };
}
