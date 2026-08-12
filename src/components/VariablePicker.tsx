import * as React from 'react';

import { IAfdagVariable } from '../ir';
import { codeSnippet, referenceSnippet } from '../variableRefs';

export interface IVariablePickerProps {
  /** The flow's declared variables (PRD §6.10). Empty → a pointer to the tab. */
  variables: IAfdagVariable[];
  /** `jinja` for a templated operator field, `python` for a code body. */
  flavour: 'jinja' | 'python';
  /** Receives the snippet to splice in at the caret. */
  onInsert: (snippet: string) => void;
  /** Labels the trigger for screen readers (the field it belongs to). */
  fieldLabel?: string;
}

/**
 * Insert a reference to a declared variable into the field it sits beside
 * (PRD §6.10).
 *
 * Airflow's own syntax is easy to get subtly wrong — `var.value` vs `var.json`,
 * attribute access vs `.get()` for a dotted key, `default=` vs the Airflow-2
 * `default_var=` — and a malformed reference fails at run time inside the task,
 * not at edit time. The snippets come from the same module the VARIABLES tab
 * uses, which is also exactly what the server's usage scanner recognises, so an
 * inserted reference is guaranteed to be seen as a real usage.
 */
export function VariablePicker(
  props: IVariablePickerProps
): JSX.Element | null {
  const { variables, flavour, onInsert, fieldLabel } = props;
  const [open, setOpen] = React.useState(false);
  const rootRef = React.useRef<HTMLSpanElement>(null);

  // Close on an outside click or Escape, like the other popovers in the editor.
  React.useEffect(() => {
    if (!open) {
      return;
    }
    const onDown = (event: MouseEvent): void => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    const onKey = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  if (variables.length === 0) {
    return null;
  }

  const snippetFor = (entry: IAfdagVariable): string =>
    flavour === 'python' ? codeSnippet(entry) : referenceSnippet(entry);

  return (
    <span className="jp-afdag-varpick" ref={rootRef}>
      <button
        type="button"
        className="jp-afdag-varpick-btn"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={
          fieldLabel
            ? `Insert a variable into ${fieldLabel}`
            : 'Insert a variable'
        }
        title="Insert a reference to one of this flow's variables"
        onClick={() => setOpen(value => !value)}
      >
        {'{ }'}
      </button>
      {open && (
        <span className="jp-afdag-varpick-menu" role="listbox">
          {variables.map(entry => (
            <button
              type="button"
              key={entry.key}
              role="option"
              aria-selected={false}
              className="jp-afdag-varpick-item"
              // The field loses focus on mousedown, taking the caret with it;
              // insert here so the reference lands where the user was typing.
              onMouseDown={event => {
                event.preventDefault();
                onInsert(snippetFor(entry));
                setOpen(false);
              }}
            >
              <span className="jp-afdag-varpick-key">{entry.key}</span>
              <span className="jp-afdag-varpick-snippet">
                {snippetFor(entry)}
              </span>
            </button>
          ))}
        </span>
      )}
    </span>
  );
}
