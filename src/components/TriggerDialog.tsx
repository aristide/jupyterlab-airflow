import { ITranslator } from '@jupyterlab/translation';
import * as React from 'react';

import { IDagParam } from '../interfaces';
import {
  ConfDraft,
  IConfField,
  buildConf,
  classifyParams,
  initialDraft
} from '../triggerForm';

type Trans = ReturnType<ITranslator['load']>;
type LogicalMode = 'now' | 'pick';

export interface ITriggerDialogProps {
  dagId: string;
  params: Record<string, IDagParam>;
  trans: Trans;
  /** Close the dialog without triggering. */
  onClose: () => void;
  /**
   * Fire the run. Resolves to an error message (the dialog stays open and shows
   * it, so the user's conf isn't lost) or `null` on success (the dialog closes).
   */
  onSubmit: (
    conf: Record<string, unknown>,
    logicalDate: string | null
  ) => Promise<string | null>;
}

/**
 * Trigger-with-conf dialog (PRD §6.6 / §15.10). Renders a field-per-param form
 * derived from the DAG's serialized `params` and posts the populated `conf`. A
 * null `logical_date` means "run now" (Airflow 3); the user can instead pin the
 * run to a chosen datetime. No-params DAGs never reach here — the manager keeps
 * the instant bare trigger for them.
 */
export function TriggerDialog(props: ITriggerDialogProps): JSX.Element {
  const { trans } = props;
  const fields = React.useMemo(
    () => classifyParams(props.params),
    [props.params]
  );
  const [draft, setDraft] = React.useState<ConfDraft>(() =>
    initialDraft(fields)
  );
  const [fieldErrors, setFieldErrors] = React.useState<Record<string, string>>(
    {}
  );
  const [logicalMode, setLogicalMode] = React.useState<LogicalMode>('now');
  const [pickValue, setPickValue] = React.useState('');
  const [serverError, setServerError] = React.useState<string | null>(null);
  const [submitting, setSubmitting] = React.useState(false);

  // Editing any input clears its stale validation error + any server error so
  // the feedback tracks the live form.
  const set = (name: string, value: string | boolean): void => {
    setDraft(d => ({ ...d, [name]: value }));
    setServerError(null);
    setFieldErrors(e => {
      if (!(name in e)) {
        return e;
      }
      const next = { ...e };
      delete next[name];
      return next;
    });
  };

  const submit = async (): Promise<void> => {
    const { conf, errors } = buildConf(fields, draft);
    if (Object.keys(errors).length > 0) {
      setFieldErrors(errors);
      return;
    }
    if (logicalMode === 'pick' && !pickValue) {
      setServerError(trans.__('Pick a date, or choose “now”.'));
      return;
    }
    const logicalDate =
      logicalMode === 'pick' ? new Date(pickValue).toISOString() : null;
    setServerError(null);
    setSubmitting(true);
    const err = await props.onSubmit(conf, logicalDate);
    if (err) {
      // Keep the dialog open with the user's conf intact so they can fix it.
      setSubmitting(false);
      setServerError(err);
    } else {
      props.onClose();
    }
  };

  return (
    <div className="jp-airflow-modal jp-airflow-trigger">
      <div className="jp-airflow-modal-head">
        <span>{trans.__('Trigger %1', props.dagId)}</span>
        <button
          className="jp-airflow-iconbtn"
          onClick={props.onClose}
          disabled={submitting}
        >
          ✕
        </button>
      </div>

      <div className="jp-airflow-modal-body jp-airflow-conf-body">
        <div className="jp-airflow-conf-intro">
          {trans.__('This DAG accepts parameters:')}
        </div>

        {fields.map(field => (
          <ConfField
            key={field.name}
            field={field}
            value={draft[field.name]}
            error={fieldErrors[field.name]}
            trans={trans}
            onChange={value => set(field.name, value)}
          />
        ))}

        <div className="jp-airflow-conf-sep" />

        <div className="jp-airflow-logicaldate">
          <span className="jp-airflow-conf-label">
            {trans.__('Logical date')}
          </span>
          <label className="jp-airflow-radio">
            <input
              type="radio"
              name="jp-airflow-logical-date"
              checked={logicalMode === 'now'}
              onChange={() => {
                setLogicalMode('now');
                setServerError(null);
              }}
            />
            {trans.__('now')}
          </label>
          <label className="jp-airflow-radio">
            <input
              type="radio"
              name="jp-airflow-logical-date"
              checked={logicalMode === 'pick'}
              onChange={() => {
                setLogicalMode('pick');
                setServerError(null);
              }}
            />
            {trans.__('pick…')}
          </label>
          <input
            type="datetime-local"
            className="jp-airflow-conf-input"
            aria-label={trans.__('Logical date')}
            disabled={logicalMode !== 'pick'}
            value={pickValue}
            onChange={e => {
              setPickValue(e.target.value);
              setLogicalMode('pick');
              setServerError(null);
            }}
          />
        </div>
      </div>

      {serverError && (
        <div className="jp-airflow-conf-servererror">{serverError}</div>
      )}

      <div className="jp-airflow-modal-actions">
        <button
          className="jp-airflow-btn"
          onClick={props.onClose}
          disabled={submitting}
        >
          {trans.__('Cancel')}
        </button>
        <button
          className="jp-airflow-btn jp-mod-accent"
          onClick={() => void submit()}
          disabled={submitting}
        >
          {submitting ? trans.__('Triggering…') : trans.__('▶ Trigger')}
        </button>
      </div>
    </div>
  );
}

interface IConfFieldProps {
  field: IConfField;
  value: string | boolean;
  error?: string;
  trans: Trans;
  onChange: (value: string | boolean) => void;
}

function ConfField(props: IConfFieldProps): JSX.Element {
  const { field, trans } = props;
  return (
    <div className="jp-airflow-conf-field">
      <label className="jp-airflow-conf-label" htmlFor={`conf-${field.name}`}>
        {field.name}
      </label>
      <ConfControl {...props} />
      {field.description && (
        <div className="jp-airflow-field-help">{field.description}</div>
      )}
      {props.error && (
        <div className="jp-airflow-field-err">{trans.__(props.error)}</div>
      )}
    </div>
  );
}

function ConfControl(props: IConfFieldProps): JSX.Element {
  const { field, value, onChange } = props;
  const id = `conf-${field.name}`;

  switch (field.kind) {
    case 'bool':
      return (
        <input
          id={id}
          type="checkbox"
          checked={value === true}
          onChange={e => onChange(e.target.checked)}
        />
      );
    case 'enum':
      return (
        <select
          id={id}
          className="jp-airflow-conf-input"
          value={String(value)}
          onChange={e => onChange(e.target.value)}
        >
          {field.nullable && (
            <option value="-1">{props.trans.__('— none —')}</option>
          )}
          {(field.enumValues ?? []).map((v, i) => (
            <option key={i} value={String(i)}>
              {String(v)}
            </option>
          ))}
        </select>
      );
    case 'int':
    case 'number':
      return (
        <input
          id={id}
          type="number"
          className="jp-airflow-conf-input"
          step={field.kind === 'int' ? '1' : 'any'}
          min={field.min}
          max={field.max}
          value={String(value)}
          onChange={e => onChange(e.target.value)}
        />
      );
    case 'date':
      return (
        <input
          id={id}
          type="date"
          className="jp-airflow-conf-input"
          value={String(value)}
          onChange={e => onChange(e.target.value)}
        />
      );
    case 'datetime':
      return (
        <input
          id={id}
          type="datetime-local"
          className="jp-airflow-conf-input"
          value={String(value)}
          onChange={e => onChange(e.target.value)}
        />
      );
    case 'json':
      return (
        <textarea
          id={id}
          className="jp-airflow-conf-input jp-airflow-conf-json"
          rows={3}
          value={String(value)}
          onChange={e => onChange(e.target.value)}
        />
      );
    default:
      return (
        <input
          id={id}
          type="text"
          className="jp-airflow-conf-input"
          value={String(value)}
          onChange={e => onChange(e.target.value)}
        />
      );
  }
}
