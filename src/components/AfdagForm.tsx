import Form from '@rjsf/core';
import { DescriptionFieldProps, RJSFSchema, UiSchema } from '@rjsf/utils';
import validator from '@rjsf/validator-ajv8';
import * as React from 'react';

import { afdagWidgets } from './rjsfWidgets';
import { InfoBubble } from './InfoBubble';

export interface IAfdagFormProps {
  schema: RJSFSchema;
  uiSchema: UiSchema;
  formData: Record<string, unknown>;
  onChange: (formData: Record<string, unknown>) => void;
}

/**
 * Render a field's schema `description` as a hoverable `ⓘ` info bubble instead
 * of always-on inline text (PRD §6.1.3). Wired once here, so both the DAG form
 * and the registry-driven NODE form pick it up. Empty descriptions render
 * nothing (e.g. the root/`Common settings` objects).
 */
function AfdagDescriptionField(
  props: DescriptionFieldProps
): JSX.Element | null {
  const { description, id } = props;
  if (!description) {
    return null;
  }
  if (typeof description !== 'string') {
    return <>{description}</>;
  }
  return <InfoBubble text={description} id={id} />;
}

const afdagTemplates = { DescriptionFieldTemplate: AfdagDescriptionField };

/**
 * A thin RJSF wrapper with the Airflow Studio custom widgets, live validation,
 * and the default submit button removed (the IR is committed on change, there is
 * no explicit submit). The empty-fragment child suppresses RJSF's submit button.
 */
export function AfdagForm(props: IAfdagFormProps): JSX.Element {
  return (
    <Form
      className="jp-afdag-rjsf"
      schema={props.schema}
      uiSchema={props.uiSchema}
      formData={props.formData}
      validator={validator}
      widgets={afdagWidgets}
      templates={afdagTemplates}
      liveValidate
      showErrorList={false}
      onChange={event =>
        props.onChange(event.formData as Record<string, unknown>)
      }
    >
      <></>
    </Form>
  );
}
