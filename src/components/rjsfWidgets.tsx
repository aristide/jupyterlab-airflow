import { RegistryWidgetsType, WidgetProps } from '@rjsf/utils';
import * as React from 'react';

import { SCHEDULE_PRESETS } from '../forms';
import { CodeMirrorField } from './CodeMirrorField';

/** RJSF `code` widget: a CodeMirror Python editor for `@task`/branch bodies. */
function CodeWidget(props: WidgetProps): JSX.Element {
  return (
    <CodeMirrorField
      language="python"
      value={String(props.value ?? '')}
      readOnly={props.readonly}
      onChange={value => props.onChange(value === '' ? undefined : value)}
    />
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
  schedule: ScheduleWidget
};
