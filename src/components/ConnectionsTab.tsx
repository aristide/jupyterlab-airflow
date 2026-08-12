import * as React from 'react';

import { inspectConnections } from '../handler';
import { IConnectionStatus, IConnectionsInspectRes } from '../interfaces';
import { IAfdagConnection, IAfdagIR } from '../ir';

export interface IConnectionsTabProps {
  ir: IAfdagIR;
  connections: IAfdagConnection[];
  onConnectionsChange: (next: IAfdagConnection[]) => void;
}

function emptyConnection(conn_id = ''): IAfdagConnection {
  return {
    conn_id,
    scope: 'local',
    conn_type: '',
    description: '',
    host: '',
    login: '',
    password: '',
    schema: '',
    port: '',
    extra: ''
  };
}

/** A handful of common Airflow connection types, offered as datalist hints.
 * Deliberately not an exhaustive enum — any provider can define its own, so the
 * field stays free-text and these are only suggestions. */
const COMMON_CONN_TYPES = [
  'aws',
  'azure',
  'discord',
  'fs',
  'ftp',
  'google_cloud_platform',
  'http',
  'imap',
  'kubernetes',
  'mysql',
  'opsgenie',
  'postgres',
  'slack',
  'smtp',
  'spark',
  'sqlite',
  'ssh',
  'telegram',
  'trino'
];

function ScopeBadges(props: {
  entry: IAfdagConnection;
  status?: IConnectionStatus;
}) {
  const { entry, status } = props;
  const remote = entry.scope === 'remote';
  const missing = status && !status.exists;
  return (
    <>
      <span
        className={
          remote
            ? 'jp-afdag-var-badge jp-afdag-var-badge-remote'
            : 'jp-afdag-var-badge jp-afdag-var-badge-local'
        }
        title={
          remote
            ? 'Already exists in Airflow. This flow can use it, but never change or delete it.'
            : 'Created and owned by this flow: written to Airflow on deploy, removed when the flow is undeployed.'
        }
      >
        {remote ? 'Airflow' : 'Flow'}
      </span>
      {missing && (
        <span
          className="jp-afdag-var-badge jp-afdag-var-badge-missing"
          title={
            remote
              ? 'Not found in Airflow right now — deploying will fail until it exists.'
              : 'Not in Airflow yet; it will be created on the next deploy.'
          }
        >
          {remote ? 'missing' : 'not yet created'}
        </span>
      )}
    </>
  );
}

function ConnectionRow(props: {
  entry: IAfdagConnection;
  status?: IConnectionStatus;
  onChange: (patch: Partial<IAfdagConnection>) => void;
  onRemove: () => void;
}): JSX.Element {
  const { entry, status, onChange, onRemove } = props;
  const usedBy = status?.used_by ?? [];
  const inUse = usedBy.length > 0;
  const remote = entry.scope === 'remote';

  return (
    <div className="jp-afdag-var-row">
      <div className="jp-afdag-var-head">
        <input
          className="jp-afdag-var-key"
          value={entry.conn_id}
          placeholder="conn_id"
          aria-label="Connection id"
          onChange={e => onChange({ conn_id: e.target.value })}
        />
        <ScopeBadges entry={entry} status={status} />
        <button
          className="jp-afdag-var-remove"
          onClick={onRemove}
          disabled={inUse}
          title={
            inUse
              ? `Still used by ${usedBy.join(', ')} — clear those fields first.`
              : 'Remove this connection from the flow'
          }
        >
          ✕
        </button>
      </div>

      <div className="jp-afdag-var-fields">
        <label>
          <span>Scope</span>
          <select
            value={entry.scope}
            onChange={e =>
              onChange({ scope: e.target.value as IAfdagConnection['scope'] })
            }
          >
            <option value="local">
              Flow connection (this flow creates it)
            </option>
            <option value="remote">Airflow connection (already exists)</option>
          </select>
        </label>

        {/* Only a flow-owned connection carries settings; a remote one lives in
            Airflow and is never copied here. */}
        {!remote && (
          <>
            <label>
              <span>Type</span>
              <input
                list="jp-afdag-conn-types"
                value={entry.conn_type ?? ''}
                placeholder="postgres"
                onChange={e => onChange({ conn_type: e.target.value })}
              />
            </label>
            <label>
              <span>Host</span>
              <input
                value={entry.host ?? ''}
                onChange={e => onChange({ host: e.target.value })}
              />
            </label>
            <label>
              <span>Port</span>
              <input
                value={entry.port ?? ''}
                placeholder="5432"
                onChange={e => onChange({ port: e.target.value })}
              />
            </label>
            <label>
              <span>Login</span>
              <input
                value={entry.login ?? ''}
                onChange={e => onChange({ login: e.target.value })}
              />
            </label>
            <label>
              <span>Password</span>
              <input
                type="password"
                value={entry.password ?? ''}
                onChange={e => onChange({ password: e.target.value })}
              />
            </label>
            <label>
              <span>Schema</span>
              <input
                value={entry.schema ?? ''}
                onChange={e => onChange({ schema: e.target.value })}
              />
            </label>
            <label className="jp-afdag-var-wide">
              <span>Extra (JSON)</span>
              <textarea
                rows={2}
                value={entry.extra ?? ''}
                placeholder='{"sslmode": "require"}'
                onChange={e => onChange({ extra: e.target.value })}
              />
            </label>
          </>
        )}

        <label className="jp-afdag-var-wide">
          <span>Description</span>
          <input
            value={entry.description ?? ''}
            onChange={e => onChange({ description: e.target.value })}
          />
        </label>
      </div>

      {inUse && (
        <div className="jp-afdag-var-usage">Used by {usedBy.join(', ')}</div>
      )}
      {!inUse && (
        <div className="jp-afdag-var-usage">
          Not referenced yet — set it as the connection on a task (e.g. its{' '}
          <code>conn_id</code> field).
        </div>
      )}
      {status?.redacted && (
        <div className="jp-afdag-var-usage">
          Airflow hides this password on read, so it cannot be shown here.
        </div>
      )}
    </div>
  );
}

/**
 * CONNECTIONS tab (PRD §6.11 / §15.16): declare the Airflow connections this
 * flow depends on.
 *
 * Same two scopes as variables. A **flow** connection is owned here — created in
 * Airflow on deploy, removed on undeploy. An **Airflow** connection already
 * exists in the target; the flow may reference it but never create, change or
 * delete it, and deploy verifies it still exists.
 *
 * Usage is detected structurally from the operator registry (any `conn_id` /
 * `*_conn_id` param), including the operator's own default when the field is
 * left blank — so "which tasks use this connection?" is exact rather than a
 * text guess.
 */
export function ConnectionsTab(props: IConnectionsTabProps): JSX.Element {
  const { ir, connections, onConnectionsChange } = props;
  const [inspect, setInspect] = React.useState<IConnectionsInspectRes | null>(
    null
  );
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [showPicker, setShowPicker] = React.useState(false);

  React.useEffect(() => {
    let cancelled = false;
    setLoading(true);
    const timer = window.setTimeout(() => {
      void inspectConnections(ir).then(res => {
        if (cancelled) {
          return;
        }
        setLoading(false);
        if (res.status === 'OK' && res.data) {
          setInspect(res.data);
          setError(null);
        } else {
          setError(res.error ?? 'Could not check connections against Airflow.');
        }
      });
    }, 400);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [ir]);

  const statusFor = React.useCallback(
    (connId: string): IConnectionStatus | undefined =>
      inspect?.connections.find(entry => entry.conn_id === connId),
    [inspect]
  );

  const update = (index: number, patch: Partial<IAfdagConnection>): void =>
    onConnectionsChange(
      connections.map((entry, i) =>
        i === index ? { ...entry, ...patch } : entry
      )
    );

  const remove = (index: number): void =>
    onConnectionsChange(connections.filter((_, i) => i !== index));

  const add = (entry: IAfdagConnection): void =>
    onConnectionsChange([...connections, entry]);

  const undeclared = inspect?.undeclared ?? [];
  const available = (inspect?.available ?? []).filter(c => !c.declared);

  return (
    <div className="jp-afdag-tabpanel">
      <datalist id="jp-afdag-conn-types">
        {COMMON_CONN_TYPES.map(t => (
          <option key={t} value={t} />
        ))}
      </datalist>

      <p className="jp-afdag-var-intro">
        Airflow connections this flow depends on. A{' '}
        <strong>flow connection</strong> is created in Airflow when you deploy
        and removed when you undeploy. An <strong>Airflow connection</strong>{' '}
        already exists there — this flow can use it, but never change or delete
        it.
      </p>
      <p className="jp-afdag-var-intro jp-afdag-var-secrets">
        A flow connection&apos;s password and Extra are stored in this{' '}
        <code>.afdag</code> file in plain text. For anything sensitive, create
        the connection in Airflow and reference it as an Airflow connection.
      </p>

      {undeclared.length > 0 && (
        <div className="jp-afdag-var-alert">
          <div>
            Used but not declared here — deploying still works, but Studio
            can&apos;t check these for you:
          </div>
          {undeclared.map(entry => (
            <div key={entry.conn_id} className="jp-afdag-conn-pending">
              <code>{entry.conn_id}</code>
              {entry.implicit && <em> (operator default)</em>}
              {!entry.exists_in_airflow && (
                <span className="jp-afdag-conn-missing"> — not in Airflow</span>
              )}
              <button
                className="jp-afdag-conn-declare"
                onClick={() =>
                  add({
                    ...emptyConnection(entry.conn_id),
                    scope: entry.exists_in_airflow ? 'remote' : 'local'
                  })
                }
              >
                ＋ declare
              </button>
            </div>
          ))}
        </div>
      )}

      {inspect && !inspect.airflow_reachable && (
        <div className="jp-afdag-var-alert">
          Airflow is unreachable, so “does it exist?” could not be checked. The
          deploy will re-check.
        </div>
      )}
      {error && <div className="jp-afdag-var-alert">{error}</div>}

      {connections.length === 0 && (
        <p className="jp-afdag-var-empty">
          No connections declared. Add one to have Studio create it on deploy,
          or to check that an existing Airflow connection is really there.
        </p>
      )}

      {connections.map((entry, index) => (
        <ConnectionRow
          key={index}
          entry={entry}
          status={statusFor(entry.conn_id)}
          onChange={patch => update(index, patch)}
          onRemove={() => remove(index)}
        />
      ))}

      <div className="jp-afdag-var-actions">
        <button onClick={() => add(emptyConnection())}>
          ＋ Flow connection
        </button>
        <button
          onClick={() => setShowPicker(v => !v)}
          disabled={available.length === 0}
          title={
            available.length === 0
              ? 'No other connections found in Airflow'
              : 'Reference a connection that already exists in Airflow'
          }
        >
          ＋ Airflow connection
          {available.length > 0 ? ` (${available.length})` : ''}
        </button>
        {loading && <span className="jp-afdag-var-loading">checking…</span>}
      </div>

      {showPicker && (
        <div className="jp-afdag-var-picker">
          {available.map(entry => (
            <button
              key={entry.conn_id}
              className="jp-afdag-var-pick"
              onClick={() => {
                add({
                  conn_id: entry.conn_id,
                  scope: 'remote',
                  description: entry.description ?? ''
                });
                setShowPicker(false);
              }}
            >
              <span className="jp-afdag-var-pick-key">
                {entry.conn_id}
                {entry.conn_type && (
                  <em className="jp-afdag-conn-type"> · {entry.conn_type}</em>
                )}
              </span>
              {entry.owner && (
                <span className="jp-afdag-var-pick-owner">
                  owned by {entry.owner}
                </span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
