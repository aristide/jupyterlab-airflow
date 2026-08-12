import { IAfdagVariable } from './ir';

/**
 * How a declared variable is referenced from a task (PRD §6.10).
 *
 * Shared by the VARIABLES tab (which shows the snippets) and the per-field
 * variable picker (which inserts them), so the two can never disagree about
 * what a reference looks like — the server's usage scanner recognises exactly
 * these forms, and a mismatch would make a real reference invisible to the
 * "is this variable still used?" check.
 */

/** Airflow renders `{{ var.value.k }}` at task run time, in any templated
 * operator field. `var.json` deserializes a JSON-typed variable. A key that is
 * not a plain identifier can't use attribute access, so it falls back to the
 * `.get('…')` form. */
export function referenceSnippet(entry: IAfdagVariable): string {
  const accessor = entry.var_type === 'json' ? 'var.json' : 'var.value';
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(entry.key)
    ? `{{ ${accessor}.${entry.key} }}`
    : `{{ ${accessor}.get(${JSON.stringify(entry.key)}) }}`;
}

/** The Python equivalent, for a code-node body. Airflow 3's SDK parameter is
 * `default` — NOT the old ORM's `default_var`, which raises TypeError. */
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

/**
 * Splice `snippet` into `value` at the field's caret, replacing any selection.
 *
 * The element is only *read* (for `selectionStart`/`selectionEnd`) — the new
 * string is handed back so the caller can route it through React's own
 * `onChange`, rather than mutating the DOM behind React's back. Falls back to
 * appending when there is no live element or no caret (an unfocused field).
 */
export function insertAtCaret(
  value: string,
  snippet: string,
  el: HTMLInputElement | HTMLTextAreaElement | null
): string {
  const current = value ?? '';
  const start = el?.selectionStart;
  const end = el?.selectionEnd;
  if (typeof start !== 'number' || typeof end !== 'number') {
    return current
      ? `${current}${current.endsWith(' ') ? '' : ' '}${snippet}`
      : snippet;
  }
  return current.slice(0, start) + snippet + current.slice(end);
}
