import { IAfdagConnection, IAfdagIR } from '../ir';

/**
 * Connection declarations live on the IR root (like `variables`/`notes`), so
 * `flowToIR` must carry them through untouched — the same round-trip guarantee
 * that lets the CONNECTIONS tab persist an edit. These assert the IR contract
 * the tab depends on; the usage scanning itself is server-side (registry-driven)
 * and is covered by test_connections.py.
 */
describe('IR connections contract', () => {
  const base = (): IAfdagIR => ({
    schema_version: '1.0',
    provenance: {
      generator: 'airflow-studio',
      studio_version: '0.1.0',
      afdag_id: 'x'
    },
    syntax_style: 'taskflow',
    dag: { dag_id: 'd' },
    nodes: [],
    edges: []
  });

  it('is absent on a flow that declares none, keeping older .afdag valid', () => {
    const ir = base();
    expect(ir.connections).toBeUndefined();
    expect('connections' in ir).toBe(false);
  });

  it('round-trips a local declaration through JSON with every field', () => {
    const conn: IAfdagConnection = {
      conn_id: 'warehouse',
      scope: 'local',
      conn_type: 'postgres',
      host: 'db.internal',
      login: 'etl',
      password: 's3cret',
      schema: 'public',
      port: '5432',
      extra: '{"sslmode":"require"}',
      description: 'warehouse'
    };
    const ir = { ...base(), connections: [conn] };
    const parsed = JSON.parse(JSON.stringify(ir)) as IAfdagIR;
    expect(parsed.connections).toEqual([conn]);
  });

  it('keeps a remote declaration free of settings — they live in Airflow', () => {
    const conn: IAfdagConnection = {
      conn_id: 'shared_api',
      scope: 'remote',
      description: 'managed by the platform team'
    };
    const parsed = JSON.parse(
      JSON.stringify({ ...base(), connections: [conn] })
    ) as IAfdagIR;
    const [got] = parsed.connections ?? [];
    expect(got.scope).toBe('remote');
    expect(got.password).toBeUndefined();
    expect(got.host).toBeUndefined();
  });

  it('carries variables and connections side by side', () => {
    const ir: IAfdagIR = {
      ...base(),
      variables: [{ key: 'k', scope: 'local', value: 'v' }],
      connections: [{ conn_id: 'c', scope: 'remote' }]
    };
    const parsed = JSON.parse(JSON.stringify(ir)) as IAfdagIR;
    expect(parsed.variables).toHaveLength(1);
    expect(parsed.connections).toHaveLength(1);
  });
});
