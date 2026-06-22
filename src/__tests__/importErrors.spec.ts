import {
  explainImportError,
  matchNode,
  providerPackageForModule
} from '../importErrors';
import { IAfdagIR, createEmptyIR } from '../ir';

function irWithTasks(taskIds: string[]): IAfdagIR {
  const ir = createEmptyIR('demo_dag');
  ir.nodes = taskIds.map((id, i) => ({
    id: `n${i}`,
    op: 'bash',
    task_id: id,
    params: {}
  }));
  return ir;
}

describe('providerPackageForModule', () => {
  it('maps a provider module to its pip package', () => {
    expect(
      providerPackageForModule(
        'airflow.providers.cncf.kubernetes.operators.pod'
      )
    ).toBe('apache-airflow-providers-cncf-kubernetes');
    expect(
      providerPackageForModule('airflow.providers.http.operators.http')
    ).toBe('apache-airflow-providers-http');
  });

  it('returns null for a non-provider module', () => {
    expect(providerPackageForModule('pandas')).toBeNull();
    expect(providerPackageForModule('airflow.sdk')).toBeNull();
  });
});

describe('matchNode', () => {
  it('matches a task_id appearing in the traceback', () => {
    const ir = irWithTasks(['load_data', 'transform']);
    const trace =
      'Traceback ...\n  File "/dags/demo.py", line 12, in load_data\nNameError: ...';
    expect(matchNode(trace, ir)).toBe('load_data');
  });

  it('does not match a partial identifier', () => {
    const ir = irWithTasks(['load']);
    expect(matchNode('reference to download_thing here', ir)).toBeUndefined();
  });

  it('returns undefined with no IR', () => {
    expect(matchNode('anything')).toBeUndefined();
  });
});

describe('explainImportError', () => {
  it('explains a missing provider and offers a pip install', () => {
    const trace =
      "Traceback (most recent call last):\nModuleNotFoundError: No module named 'airflow.providers.http'";
    const e = explainImportError(trace);
    expect(e.title).toMatch(/provider/i);
    expect(e.hint).toContain('pip install apache-airflow-providers-http');
  });

  it('explains a non-provider missing module', () => {
    const e = explainImportError(
      "ModuleNotFoundError: No module named 'pandas'"
    );
    expect(e.hint).toContain('pandas');
    expect(e.hint).not.toContain('provider');
  });

  it('explains a syntax error and points at the code task', () => {
    const ir = irWithTasks(['clean_rows']);
    const trace =
      '  File "/dags/demo.py", line 20, in clean_rows\nSyntaxError: invalid syntax';
    const e = explainImportError(trace, ir);
    expect(e.title).toMatch(/syntax/i);
    expect(e.nodeTaskId).toBe('clean_rows');
  });

  it('explains a NameError', () => {
    const e = explainImportError("NameError: name 'pd' is not defined");
    expect(e.summary).toContain('pd');
  });

  it('flags an Airflow-2 import path', () => {
    const trace =
      'from airflow.operators.bash_operator import BashOperator\nImportError: boom';
    const e = explainImportError(trace);
    expect(e.title).toMatch(/Airflow 2/i);
  });

  it('falls back to the exception line for unknown errors', () => {
    const e = explainImportError(
      'Traceback ...\nValueError: something specific went wrong'
    );
    expect(e.summary).toContain('something specific went wrong');
  });

  it('handles an empty trace gracefully', () => {
    const e = explainImportError('');
    expect(e.summary).toBeTruthy();
  });
});
