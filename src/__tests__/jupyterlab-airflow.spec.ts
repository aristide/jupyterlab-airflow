import { ServerConnection } from '@jupyterlab/services';

import { deployDag, generateDag, listDags } from '../handler';
import { createEmptyIR } from '../ir';
import {
  getOperator,
  getOperators,
  loadOperators,
  validateNodeParams
} from '../operators';

// Verify the handler layer normalises an Airflow/server error payload into an
// IApiRes with status 'ERR', by stubbing ServerConnection at the transport
// level (no real network).
describe('jupyterlab-airflow handler', () => {
  it('maps a server { error } payload to status ERR', async () => {
    const makeRequest = jest
      .spyOn(ServerConnection, 'makeRequest')
      .mockResolvedValue({
        ok: false,
        statusText: 'Bad Gateway',
        text: async () => JSON.stringify({ error: 'boom', detail: 'nope' })
      } as unknown as Response);

    try {
      const res = await listDags();
      expect(res.status).toBe('ERR');
      expect(res.error).toBe('boom');
      expect(res.detail).toBe('nope');
    } finally {
      makeRequest.mockRestore();
    }
  });

  it('maps a successful { data } payload to status OK', async () => {
    const makeRequest = jest
      .spyOn(ServerConnection, 'makeRequest')
      .mockResolvedValue({
        ok: true,
        statusText: 'OK',
        text: async () =>
          JSON.stringify({ data: { dags: [], total_entries: 0 } })
      } as unknown as Response);

    try {
      const res = await listDags();
      expect(res.status).toBe('OK');
      expect(res.data?.total_entries).toBe(0);
    } finally {
      makeRequest.mockRestore();
    }
  });
});

// The operator catalogue is fetched from `GET operators` and cached in a
// module-level index that getOperator/validateNodeParams read synchronously.
describe('operator registry loader', () => {
  const registry = [
    {
      id: 'bash',
      label: 'Bash operator',
      category: 'Python/Bash',
      taskIdPrefix: 'bash',
      params: [{ name: 'bash_command', label: 'Bash Command', required: true }]
    }
  ];

  it('loads the registry and indexes it for sync lookups', async () => {
    const makeRequest = jest
      .spyOn(ServerConnection, 'makeRequest')
      .mockResolvedValue({
        ok: true,
        statusText: 'OK',
        text: async () => JSON.stringify({ data: registry })
      } as unknown as Response);

    try {
      const list = await loadOperators(true);
      expect(list).toHaveLength(1);
      expect(getOperators()).toHaveLength(1);
      expect(getOperator('bash')?.label).toBe('Bash operator');
      // Required-field validation comes from the loaded registry.
      expect(validateNodeParams('bash', {}).valid).toBe(false);
      expect(
        validateNodeParams('bash', { bash_command: 'echo hi' }).valid
      ).toBe(true);
    } finally {
      makeRequest.mockRestore();
    }
  });

  it('rejects (and allows retry) when the registry fails to load', async () => {
    const makeRequest = jest
      .spyOn(ServerConnection, 'makeRequest')
      .mockResolvedValue({
        ok: false,
        statusText: 'Server Error',
        text: async () => JSON.stringify({ error: 'boom' })
      } as unknown as Response);

    try {
      await expect(loadOperators(true)).rejects.toThrow('boom');
    } finally {
      makeRequest.mockRestore();
    }
  });
});

// The CODE tab POSTs the IR to `generate` and renders the returned Python.
describe('generateDag handler', () => {
  it('POSTs the IR and returns the generated code payload', async () => {
    const makeRequest = jest
      .spyOn(ServerConnection, 'makeRequest')
      .mockResolvedValue({
        ok: true,
        statusText: 'OK',
        text: async () =>
          JSON.stringify({
            data: {
              code: 'from airflow.sdk import dag',
              valid: true,
              errors: []
            }
          })
      } as unknown as Response);

    try {
      const res = await generateDag(createEmptyIR('demo'));
      expect(res.status).toBe('OK');
      expect(res.data?.valid).toBe(true);
      expect(res.data?.code).toContain('airflow.sdk');
      // The request carried the IR as a POST body.
      const init = makeRequest.mock.calls[0][1] as RequestInit;
      expect(init.method).toBe('POST');
      expect(String(init.body)).toContain('"dag_id":"demo"');
    } finally {
      makeRequest.mockRestore();
    }
  });
});

// Deploy POSTs the IR to `deploy`; the server validates then writes the file.
describe('deployDag handler', () => {
  it('returns the deploy result payload', async () => {
    const makeRequest = jest
      .spyOn(ServerConnection, 'makeRequest')
      .mockResolvedValue({
        ok: true,
        statusText: 'OK',
        text: async () =>
          JSON.stringify({
            data: {
              deployed: true,
              filename: 'demo.py',
              path: '/dags/demo.py',
              dag_id: 'demo',
              warnings: [],
              errors: [],
              dagbag: { status: 'skipped' }
            }
          })
      } as unknown as Response);

    try {
      const res = await deployDag(createEmptyIR('demo'));
      expect(res.status).toBe('OK');
      expect(res.data?.deployed).toBe(true);
      expect(res.data?.filename).toBe('demo.py');
      const init = makeRequest.mock.calls[0][1] as RequestInit;
      expect(init.method).toBe('POST');
    } finally {
      makeRequest.mockRestore();
    }
  });
});
