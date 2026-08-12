import * as React from 'react';

import { validateDagId } from '../ir';

export interface IDagIdFieldProps {
  dagId: string;
  /** Called with a validated, actually-changed id. For a deployed DAG this is
   * the §6.1.8(B) migration (preflight → confirm → redeploy → retire), not a
   * field edit, so it stays in the parent. */
  onCommit: (next: string) => void;
}

/**
 * The `dag_id` in the top bar, edited in place (PRD §6.1.8(B) / §15.1).
 *
 * Click it to edit; **Enter** or moving focus away commits, **Escape** reverts.
 * Committing an unchanged id is a no-op, so simply clicking and clicking away
 * never triggers the rename migration.
 *
 * A `dag_id` must be a Python identifier, and changing a *deployed* one is a
 * real migration rather than a rename (Airflow has none — a new id is a new DAG
 * with no history). So the value is validated before it can commit: Enter on an
 * invalid id keeps the editor open with the reason, while blurring reverts
 * rather than trapping focus in a field the user is trying to leave.
 */
export function DagIdField(props: IDagIdFieldProps): JSX.Element {
  const { dagId, onCommit } = props;
  const [editing, setEditing] = React.useState(false);
  const [draft, setDraft] = React.useState(dagId);
  const [error, setError] = React.useState<string | null>(null);
  const inputRef = React.useRef<HTMLInputElement>(null);
  // Blur fires after Enter/Escape have already resolved the edit; this stops it
  // committing (or reverting) a second time.
  const settledRef = React.useRef(false);

  const start = (): void => {
    setDraft(dagId);
    setError(null);
    settledRef.current = false;
    setEditing(true);
  };

  React.useEffect(() => {
    if (editing) {
      inputRef.current?.focus();
      inputRef.current?.select();
    }
  }, [editing]);

  const cancel = (): void => {
    settledRef.current = true;
    setError(null);
    setEditing(false);
  };

  /** Returns false when the value is invalid, so Enter can stay in the editor. */
  const commit = (): boolean => {
    const checked = validateDagId(draft);
    if ('error' in checked) {
      setError(checked.error);
      return false;
    }
    settledRef.current = true;
    setEditing(false);
    setError(null);
    if (checked.id !== dagId) {
      onCommit(checked.id);
    }
    return true;
  };

  if (!editing) {
    return (
      <button
        type="button"
        className="jp-afdag-dagid jp-afdag-dagid-btn"
        title="Click to rename the dag_id (a guided migration for a deployed DAG)"
        onClick={start}
      >
        {dagId || 'untitled'}
      </button>
    );
  }

  return (
    <span className="jp-afdag-dagid-edit">
      <input
        ref={inputRef}
        className="jp-afdag-dagid-input"
        aria-label="DAG id"
        aria-invalid={Boolean(error)}
        value={draft}
        size={Math.max(draft.length + 1, 8)}
        onChange={event => {
          setDraft(event.target.value);
          setError(null);
        }}
        onKeyDown={event => {
          if (event.key === 'Enter') {
            event.preventDefault();
            commit();
          } else if (event.key === 'Escape') {
            event.preventDefault();
            cancel();
          }
        }}
        onBlur={() => {
          if (settledRef.current) {
            return;
          }
          // Leaving the field commits a valid change and discards an invalid
          // one — keeping focus hostage in a field the user is leaving would be
          // worse than losing a short, retypeable id.
          if (!commit()) {
            cancel();
          }
        }}
      />
      {error && (
        <span className="jp-afdag-dagid-error" role="alert">
          {error}
        </span>
      )}
    </span>
  );
}
