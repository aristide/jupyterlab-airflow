import { getDefaultRegistry } from '@rjsf/core';
import {
  BaseInputTemplateProps,
  RegistryWidgetsType,
  WidgetProps
} from '@rjsf/utils';
import * as React from 'react';

import { SCHEDULE_PRESETS } from '../forms';
import { IAfdagVariable } from '../ir';
import { insertAtCaret } from '../variableRefs';
import { CodeMirrorField, ICodeMirrorHandle } from './CodeMirrorField';
import { VariablePicker } from './VariablePicker';

// RJSF's own defaults, composed with rather than reimplemented: the variable
// picker is additive, so the underlying inputs keep every behaviour (number
// coercion, required/disabled, ids, aria wiring) exactly as upstream defines it.
const defaultRegistry = getDefaultRegistry();
const DefaultBaseInput = defaultRegistry.templates.BaseInputTemplate;
const DefaultTextarea = defaultRegistry.widgets.TextareaWidget;

/** The flow's variables, threaded to every field through RJSF's `formContext`
 * (set once in AfdagForm) so no widget needs its own prop drilling. */
function contextVariables(formContext: unknown): IAfdagVariable[] {
  const list = (formContext as { variables?: unknown } | undefined)?.variables;
  return Array.isArray(list) ? (list as IAfdagVariable[]) : [];
}

/**
 * Whether this field can meaningfully hold a Jinja variable reference.
 *
 * Airflow renders `{{ … }}` only in an operator's *templated* fields at task
 * run time, so the picker is deliberately narrow: a reference dropped anywhere
 * else is stored as a literal and silently never renders, which is exactly the
 * kind of quiet breakage the variables feature exists to prevent. Excluded:
 * non-strings and enums (Airflow could never render a template there), and any
 * field that opts out via `ui:options.variablePicker: false` — used for
 * `task_id`, which must be a Python identifier. The DAG-level scalars
 * (owner/start_date/tags/…) are excluded wholesale by simply not handing the
 * DAG form any variables.
 */
function acceptsTemplate(schema: unknown, options: unknown): boolean {
  if (
    (options as { variablePicker?: unknown } | undefined)?.variablePicker ===
    false
  ) {
    return false;
  }
  const shape = (schema ?? {}) as { type?: unknown; enum?: unknown };
  return shape.type === 'string' && !Array.isArray(shape.enum);
}

/** RJSF `code` widget: a CodeMirror Python editor for `@task`/branch bodies. */
function CodeWidget(props: WidgetProps): JSX.Element {
  const variables = contextVariables(props.formContext);
  const handleRef = React.useRef<ICodeMirrorHandle>(null);
  return (
    <div className="jp-afdag-field-withpick">
      <CodeMirrorField
        handleRef={handleRef}
        language="python"
        value={String(props.value ?? '')}
        readOnly={props.readonly}
        onChange={value => props.onChange(value === '' ? undefined : value)}
      />
      {!props.readonly && (
        <VariablePicker
          variables={variables}
          // A code body is Python, so the reference is `Variable.get(...)`
          // rather than a Jinja template.
          flavour="python"
          fieldLabel={props.label}
          onInsert={snippet => handleRef.current?.insert(snippet)}
        />
      )}
    </div>
  );
}

/**
 * Every plain text input in every form (PRD §6.10): RJSF funnels string, number
 * and date widgets through `BaseInputTemplate`, so wrapping it once puts the
 * variable picker on all of them — no per-operator YAML change, the same
 * "one seam, every operator" approach per-task callbacks and assets use.
 */
function AfdagBaseInput(props: BaseInputTemplateProps): JSX.Element {
  const variables = contextVariables(props.formContext);
  if (
    !acceptsTemplate(props.schema, props.options) ||
    props.readonly ||
    variables.length === 0
  ) {
    return <DefaultBaseInput {...props} />;
  }
  return (
    <div className="jp-afdag-field-withpick">
      <DefaultBaseInput {...props} />
      <VariablePicker
        variables={variables}
        flavour="jinja"
        fieldLabel={props.label}
        onInsert={snippet =>
          props.onChange(
            insertAtCaret(
              String(props.value ?? ''),
              snippet,
              document.getElementById(props.id) as HTMLInputElement | null
            )
          )
        }
      />
    </div>
  );
}

/** The multi-line sibling: `TextareaWidget` renders its own element rather than
 * going through `BaseInputTemplate`, so it needs the picker wired separately. */
function AfdagTextarea(props: WidgetProps): JSX.Element {
  const variables = contextVariables(props.formContext);
  if (
    !acceptsTemplate(props.schema, props.options) ||
    props.readonly ||
    variables.length === 0
  ) {
    return <DefaultTextarea {...props} />;
  }
  return (
    <div className="jp-afdag-field-withpick">
      <DefaultTextarea {...props} />
      <VariablePicker
        variables={variables}
        flavour="jinja"
        fieldLabel={props.label}
        onInsert={snippet =>
          props.onChange(
            insertAtCaret(
              String(props.value ?? ''),
              snippet,
              document.getElementById(props.id) as HTMLTextAreaElement | null
            )
          )
        }
      />
    </div>
  );
}

/** RJSF `json` widget: a CodeMirror JSON editor for dict params (env, params). */
function JsonWidget(props: WidgetProps): JSX.Element {
  return (
    <CodeMirrorField
      language="json"
      value={String(props.value ?? '')}
      placeholder="{ }"
      readOnly={props.readonly}
      onChange={value => props.onChange(value === '' ? undefined : value)}
    />
  );
}

/** RJSF `schedule` widget: preset dropdown + a custom cron/timedelta input. */
function ScheduleWidget(props: WidgetProps): JSX.Element {
  const value = String(props.value ?? 'None');
  const isPreset = SCHEDULE_PRESETS.includes(value);
  const [custom, setCustom] = React.useState(!isPreset);

  return (
    <div className="jp-afdag-schedule">
      <select
        value={custom ? '__custom__' : value}
        onChange={event => {
          const next = event.target.value;
          if (next === '__custom__') {
            setCustom(true);
            props.onChange('');
          } else {
            setCustom(false);
            props.onChange(next);
          }
        }}
      >
        {SCHEDULE_PRESETS.map(preset => (
          <option key={preset} value={preset}>
            {preset}
          </option>
        ))}
        <option value="__custom__">Custom (cron…)</option>
      </select>
      {custom && (
        <input
          className="jp-afdag-schedule-cron"
          placeholder="0 9 * * *"
          value={isPreset ? '' : value}
          onChange={event => props.onChange(event.target.value)}
        />
      )}
    </div>
  );
}

export const afdagWidgets: RegistryWidgetsType = {
  code: CodeWidget,
  json: JsonWidget,
  schedule: ScheduleWidget,
  // Overrides RJSF's built-in, so every multi-line string field gets the picker.
  TextareaWidget: AfdagTextarea
};

/** Templates registered on every Studio form. `BaseInputTemplate` is the single
 * seam that reaches every plain input across all operators. */
export const afdagBaseInput = AfdagBaseInput;
