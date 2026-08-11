import * as React from 'react';

import { inspectVariables } from '../handler';
import { IVariableStatus, IVariablesInspectRes } from '../interfaces';
import { IAfdagIR, IAfdagVariable } from '../ir';

export interface IVariablesTabProps {
  ir: IAfdagIR;
  variables: IAfdagVariable[];
  onVariablesChange: (next: IAfdagVariable[]) => void;
}

/** A blank flow-owned declaration. */
function emptyVariable(): IAfdagVariable {
  return {
    key: '',
    scope: 'local',
    value: '',
    description: '',
    var_type: 'string'
  };
}

/** The reference snippet to paste into an operator field. Airflow resolves it at
 * task run time; `var.json` deserializes, `var.value` returns the raw string.
 * A key that is not a plain identifier can't use attribute access, so it falls
 * back to the `.get('…')` form. */
export function referenceSnippet(entry: IAfdagVariable): string {
  const accessor = entry.var_type === 'json' ? 'var.json' : 'var.value';
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(entry.key)
    ? `{{ ${accessor}.${entry.key} }}`
    : `{{ ${accessor}.get(${JSON.stringify(entry.key)}) }}`;
}

/** The Python equivalent, for a code-node body. Airflow 3's SDK parameter is
 * `default` — NOT the old ORM's `default_var`. */
export function codeSnippet(entry: IAfdagVariable): string {
  const args = [JSON.stringify(entry.key)];
  if (entry.var_type === 'json') {
    args.push('deserialize_json=True');
  }
  if (entry.default) {
    args.push(`default=${JSON.stringify(entry.default)}`);
  }
  return `Variable.get(${args.join(', ')})`;
}

function StatusBadge(props: {
  entry: IAfdagVariable;
  status?: IVariableStatus;
}) {
  const { entry, status } = props;
  const remote = entry.scope === 'remote';
  const missing = status && !status.exists;
  const cls = remote
    ? 'jp-afdag-var-badge jp-afdag-var-badge-remote'
    : 'jp-afdag-var-badge jp-afdag-var-badge-local';
  return (
    <>
      <span
        className={cls}
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

function VariableRow(props: {
  entry: IAfdagVariable;
  status?: IVariableStatus;
  onChange: (patch: Partial<IAfdagVariable>) => void;
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
          value={entry.key}
          placeholder="variable_key"
          aria-label="Variable key"
          onChange={e => onChange({ key: e.target.value })}
        />
        <StatusBadge entry={entry} status={status} />
        <button
          className="jp-afdag-var-remove"
          onClick={onRemove}
          disabled={inUse}
          title={
            inUse
              ? `Still used by ${usedBy.join(', ')} — remove those references first.`
              : 'Remove this variable from the flow'
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
              onChange({ scope: e.target.value as IAfdagVariable['scope'] })
            }
          >
            <option value="local">Flow variable (this flow creates it)</option>
            <option value="remote">Airflow variable (already exists)</option>
          </select>
        </label>

        <label>
          <span>Type</span>
          <select
            value={entry.var_type ?? 'string'}
            onChange={e =>
              onChange({
                var_type: e.target.value as IAfdagVariable['var_type']
              })
            }
          >
            <option value="string">Text</option>
            <option value="json">JSON</option>
          </select>
        </label>

        {!remote && (
          <label className="jp-afdag-var-wide">
            <span>Value</span>
            <textarea
              rows={entry.var_type === 'json' ? 3 : 1}
              value={entry.value ?? ''}
              placeholder={
                entry.var_type === 'json' ? '{"key": "value"}' : 'value'
              }
              onChange={e => onChange({ value: e.target.value })}
            />
          </label>
        )}

        <label className="jp-afdag-var-wide">
          <span>Default (optional)</span>
          <input
            value={entry.default ?? ''}
            placeholder="used when the variable is missing at run time"
            onChange={e => onChange({ default: e.target.value })}
          />
        </label>

        <label className="jp-afdag-var-wide">
          <span>Description</span>
          <input
            value={entry.description ?? ''}
            onChange={e => onChange({ description: e.target.value })}
          />
        </label>
      </div>

      <div className="jp-afdag-var-refs">
        <code title="Paste into any operator field">
          {referenceSnippet(entry)}
        </code>
        <code title="Use inside a Python code task">{codeSnippet(entry)}</code>
      </div>

      {inUse && (
        <div className="jp-afdag-var-usage">Used by {usedBy.join(', ')}</div>
      )}
      {status?.redacted && (
        <div className="jp-afdag-var-usage">
          Airflow hides this value because the key looks sensitive; it cannot be
          read back here.
        </div>
      )}
    </div>
  );
}

/**
 * VARIABLES tab (PRD §6.10 / §15.15): declare the Airflow variables this flow
 * depends on.
 *
 * Two scopes, and the distinction is the whole feature. A **flow** variable is
 * owned here — its value ships to Airflow on deploy and is removed again on
 * undeploy. An **Airflow** variable already exists in the target (another flow,
 * an operator, a secrets backend); the flow may reference it but never create,
 * change or delete it, and deploy verifies it still exists.
 *
 * Declarations live on `ir.variables` — outside `nodes`/`edges`, like notes, so
 * the task graph, cycle check and codegen are untouched.
 */
export function VariablesTab(props: IVariablesTabProps): JSX.Element {
  const { ir, variables, onVariablesChange } = props;
  const [inspect, setInspect] = React.useState<IVariablesInspectRes | null>(
    null
  );
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [showPicker, setShowPicker] = React.useState(false);

  // Reconcile against the live Airflow: which keys exist, who owns them, and
  // where each is referenced. Debounced because it re-runs as the user types.
  React.useEffect(() => {
    let cancelled = false;
    setLoading(true);
    const timer = window.setTimeout(() => {
      void inspectVariables(ir).then(res => {
        if (cancelled) {
          return;
        }
        setLoading(false);
        if (res.status === 'OK' && res.data) {
          setInspect(res.data);
          setError(null);
        } else {
          setError(res.error ?? 'Could not check variables against Airflow.');
        }
      });
    }, 400);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [ir]);

  const statusFor = React.useCallback(
    (key: string): IVariableStatus | undefined =>
      inspect?.variables.find(entry => entry.key === key),
    [inspect]
  );

  const update = (index: number, patch: Partial<IAfdagVariable>): void => {
    const next = variables.map((entry, i) =>
      i === index ? { ...entry, ...patch } : entry
    );
    onVariablesChange(next);
  };

  const remove = (index: number): void => {
    onVariablesChange(variables.filter((_, i) => i !== index));
  };

  const add = (entry: IAfdagVariable): void => {
    onVariablesChange([...variables, entry]);
  };

  const undefinedKeys = inspect?.undefined ?? [];
  const available = (inspect?.available ?? []).filter(v => !v.declared);

  return (
    <div className="jp-afdag-tabpanel">
      <p className="jp-afdag-var-intro">
        Airflow variables this flow depends on. A <strong>flow variable</strong>{' '}
        is created in Airflow when you deploy and removed when you undeploy. An{' '}
        <strong>Airflow variable</strong> already exists there — this flow can
        read it, but never change or delete it.
      </p>
      <p className="jp-afdag-var-intro jp-afdag-var-secrets">
        Keep secrets out of flow variables: their values are stored in this
        <code>.afdag</code> file in plain text. Create the secret directly in
        Airflow and reference it as an Airflow variable instead.
      </p>

      {undefinedKeys.length > 0 && (
        <div className="jp-afdag-var-alert jp-afdag-var-alert-error">
          Used but not defined: {undefinedKeys.join(', ')}. Add each one below —
          the flow will not deploy until you do.
        </div>
      )}
      {inspect && !inspect.airflow_reachable && (
        <div className="jp-afdag-var-alert">
          Airflow is unreachable, so “does it exist?” could not be checked. The
          deploy will re-check.
        </div>
      )}
      {error && <div className="jp-afdag-var-alert">{error}</div>}

      {variables.length === 0 && (
        <p className="jp-afdag-var-empty">
          No variables yet. Add one to avoid hard-coding values in your tasks.
        </p>
      )}

      {variables.map((entry, index) => (
        <VariableRow
          key={index}
          entry={entry}
          status={statusFor(entry.key)}
          onChange={patch => update(index, patch)}
          onRemove={() => remove(index)}
        />
      ))}

      <div className="jp-afdag-var-actions">
        <button onClick={() => add(emptyVariable())}>＋ Flow variable</button>
        <button
          onClick={() => setShowPicker(v => !v)}
          disabled={available.length === 0}
          title={
            available.length === 0
              ? 'No other variables found in Airflow'
              : 'Reference a variable that already exists in Airflow'
          }
        >
          ＋ Airflow variable
          {available.length > 0 ? ` (${available.length})` : ''}
        </button>
        {loading && <span className="jp-afdag-var-loading">checking…</span>}
      </div>

      {showPicker && (
        <div className="jp-afdag-var-picker">
          {available.map(entry => (
            <button
              key={entry.key}
              className="jp-afdag-var-pick"
              onClick={() => {
                add({
                  key: entry.key,
                  scope: 'remote',
                  description: entry.description ?? '',
                  var_type: 'string'
                });
                setShowPicker(false);
              }}
            >
              <span className="jp-afdag-var-pick-key">{entry.key}</span>
              {entry.owner && (
                <span className="jp-afdag-var-pick-owner">
                  owned by {entry.owner}
                </span>
              )}
            </button>
          ))}
        </div>
      )}

      {undefinedKeys.length > 0 && (
        <div className="jp-afdag-var-actions">
          {undefinedKeys.map(key => (
            <button
              key={key}
              onClick={() => add({ ...emptyVariable(), key })}
              title={`Declare '${key}' as a flow variable`}
            >
              ＋ define “{key}”
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
