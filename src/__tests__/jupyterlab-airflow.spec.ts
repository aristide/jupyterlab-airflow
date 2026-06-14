import { ServerConnection } from '@jupyterlab/services';

import { listDags } from '../handler';

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
