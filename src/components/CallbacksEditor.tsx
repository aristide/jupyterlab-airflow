import * as React from 'react';

import {
  formDataToNotifierParams,
  notifierForm,
  notifierToFormData
} from '../forms';
import { AfdagCallbacksValue, IAfdagCallbackEntry } from '../ir';
import { INotifierDef } from '../interfaces';
import {
  getNotifier,
  getNotifiers,
  validateNotifierParams
} from '../notifiers';
import { AfdagForm } from './AfdagForm';

/** One lifecycle event the editor can attach notifiers to. */
export interface ICallbackEvent {
  id: string;
  label: string;
  hint: string;
}

/** A callbacks block keyed by event id — the shape of both `dag.callbacks`
 * (PRD §6.8, DAG scope) and `node.callbacks` (per-task scope). */
export type CallbacksValue = AfdagCallbacksValue;

export interface ICallbacksEditorProps {
  /** The events to expose (DAG: on_failure/on_success; task: +on_retry). */
  events: ICallbackEvent[];
  /** The current callbacks block (from the IR); undefined when none are set. */
  value: CallbacksValue | undefined;
  /** Commit a cleaned callbacks block (undefined when nothing is set). */
  onChange: (next: CallbacksValue | undefined) => void;
  /** Lead-in copy explaining the scope (DAG vs this task). */
  intro: React.ReactNode;
}

// Local entries carry a stable client-only uid so add/remove reconciles by
// identity (not array index — index keys reuse a form instance for a different
// entry on mid-list removal). The uid is stripped at the IR boundary.
interface ILocalEntry extends IAfdagCallbackEntry {
  __uid: string;
}
type LocalCallbacks = Record<string, ILocalEntry[]>;

let uidCounter = 0;
const nextUid = (): string => `cb${(uidCounter += 1)}`;

function seedLocal(
  events: ICallbackEvent[],
  cbs: CallbacksValue | undefined
): LocalCallbacks {
  const out: LocalCallbacks = {};
  if (!cbs) {
    return out;
  }
  for (const { id } of events) {
    const list = cbs[id];
    if (list) {
      out[id] = list.map(entry => ({ ...entry, __uid: nextUid() }));
    }
  }
  return out;
}

/** Strip the client-only uid + drop empty event arrays; undefined when nothing
 * is set so the IR (and the deployed `.py`) stays clean and back-compatible. */
function cleanCallbacks(
  events: ICallbackEvent[],
  cbs: LocalCallbacks
): CallbacksValue | undefined {
  const out: CallbacksValue = {};
  let any = false;
  for (const { id } of events) {
    const list = cbs[id];
    if (list && list.length > 0) {
      out[id] = list.map(entry => ({
        notifier_id: entry.notifier_id,
        params: entry.params
      }));
      any = true;
    }
  }
  return any ? out : undefined;
}

/** The right "this channel isn't available" copy for the target Airflow — pip
 * hint for a missing provider, version note for too-old (mirrors the palette). */
export function availabilityNote(def: INotifierDef): string | null {
  if (def.availability === 'missing-provider') {
    return `Needs ${def.pipInstall ?? `the ${def.provider ?? ''} provider`} in your Airflow.`;
  }
  if (def.availability === 'version-too-old') {
    return `Needs Airflow ${def.airflowMinVersion ?? ''}+ — your Airflow is older.`;
  }
  return null;
}

/**
 * Shared callbacks editor (PRD §6.8): attach notifiers (email / Slack / …) to
 * lifecycle events. Notifiers run as Airflow callbacks — not graph tasks. Holds
 * local state (seeded once from the IR; the parent remounts via `key` on an
 * external reload or a node switch) and commits each change back through
 * `onChange`. Drives both the DAG-level NOTIFY tab and the per-task NODE-tab
 * section; the caller picks the events and scope.
 */
export function CallbacksEditor(props: ICallbacksEditorProps): JSX.Element {
  const { events, value, onChange, intro } = props;
  const notifiers = getNotifiers();
  const [callbacks, setCallbacks] = React.useState<LocalCallbacks>(() =>
    seedLocal(events, value)
  );

  const commit = React.useCallback(
    (next: LocalCallbacks) => {
      setCallbacks(next);
      onChange(cleanCallbacks(events, next));
    },
    [events, onChange]
  );

  const addEntry = (event: string, notifierId: string): void => {
    const entry: ILocalEntry = {
      notifier_id: notifierId,
      params: {},
      __uid: nextUid()
    };
    commit({ ...callbacks, [event]: [...(callbacks[event] ?? []), entry] });
  };

  const removeEntry = (event: string, uid: string): void => {
    const list = (callbacks[event] ?? []).filter(entry => entry.__uid !== uid);
    commit({ ...callbacks, [event]: list });
  };

  const updateParams = (
    event: string,
    uid: string,
    params: Record<string, unknown>
  ): void => {
    const list = (callbacks[event] ?? []).map(entry =>
      entry.__uid === uid ? { ...entry, params } : entry
    );
    commit({ ...callbacks, [event]: list });
  };

  if (notifiers.length === 0) {
    return (
      <p className="jp-afdag-notify-intro">
        No notification channels are available yet — the notifier registry
        couldn’t be loaded.
      </p>
    );
  }

  return (
    <div className="jp-afdag-notify">
      <p className="jp-afdag-notify-intro">{intro}</p>
      {events.map(({ id, label, hint }) => {
        const entries = callbacks[id] ?? [];
        return (
          <section key={id} className="jp-afdag-notify-event">
            <h3 className="jp-afdag-notify-event-title">
              {label}{' '}
              <span className="jp-afdag-notify-hint">— when {hint}</span>
            </h3>
            {entries.length === 0 && (
              <p className="jp-afdag-notify-none">No notifications.</p>
            )}
            {entries.map(entry => (
              <NotifierEntry
                key={entry.__uid}
                entry={entry}
                onChangeParams={params => updateParams(id, entry.__uid, params)}
                onRemove={() => removeEntry(id, entry.__uid)}
              />
            ))}
            <AddNotifier
              notifiers={notifiers}
              onAdd={notifierId => addEntry(id, notifierId)}
            />
          </section>
        );
      })}
    </div>
  );
}

function NotifierEntry(props: {
  entry: IAfdagCallbackEntry;
  onChangeParams: (params: Record<string, unknown>) => void;
  onRemove: () => void;
}): JSX.Element {
  const def = getNotifier(props.entry.notifier_id);
  const form = React.useMemo(() => (def ? notifierForm(def) : null), [def]);
  // Local form state (seeded once) so a json field isn't reformatted mid-edit by
  // the IR round-trip — the DagTab pattern; the stable uid key keeps it ours.
  const [formData, setFormData] = React.useState<Record<string, unknown>>(() =>
    def ? notifierToFormData(def, props.entry.params) : {}
  );
  const handleChange = (next: Record<string, unknown>): void => {
    setFormData(next);
    if (def) {
      props.onChangeParams(formDataToNotifierParams(def, next));
    }
  };
  const note = def ? availabilityNote(def) : null;
  const missing = def
    ? validateNotifierParams(def.id, props.entry.params).missing
    : [];
  return (
    <div className="jp-afdag-notify-entry">
      <div className="jp-afdag-notify-entry-head">
        <span className="jp-afdag-notify-channel">
          {def ? def.label : props.entry.notifier_id}
        </span>
        <button
          className="jp-afdag-notify-remove"
          aria-label="Remove notification"
          title="Remove notification"
          onClick={props.onRemove}
        >
          ✕
        </button>
      </div>
      {note && <p className="jp-afdag-notify-warn">ⓘ {note}</p>}
      {def && form && (
        <AfdagForm
          schema={form.schema}
          uiSchema={form.uiSchema}
          formData={formData}
          onChange={handleChange}
        />
      )}
      {missing.length > 0 && (
        <p className="jp-afdag-notify-error">
          ⚠ Required: {missing.join(', ')}
        </p>
      )}
    </div>
  );
}

function AddNotifier(props: {
  notifiers: INotifierDef[];
  onAdd: (id: string) => void;
}): JSX.Element {
  return (
    <div className="jp-afdag-notify-add">
      <span className="jp-afdag-notify-add-label">＋ Add</span>
      {props.notifiers.map(notifier => {
        const note = availabilityNote(notifier);
        return (
          <button
            key={notifier.id}
            className={
              note
                ? 'jp-afdag-notify-add-btn jp-mod-unavailable'
                : 'jp-afdag-notify-add-btn'
            }
            title={note ?? notifier.description}
            onClick={() => props.onAdd(notifier.id)}
          >
            {notifier.label}
            {note ? ' ⓘ' : ''}
          </button>
        );
      })}
    </div>
  );
}
