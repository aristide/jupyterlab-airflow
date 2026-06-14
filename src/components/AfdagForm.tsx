import Form from '@rjsf/core';
import { RJSFSchema, UiSchema } from '@rjsf/utils';
import validator from '@rjsf/validator-ajv8';
import * as React from 'react';

import { afdagWidgets } from './rjsfWidgets';

export interface IAfdagFormProps {
  schema: RJSFSchema;
  uiSchema: UiSchema;
  formData: Record<string, unknown>;
  onChange: (formData: Record<string, unknown>) => void;
}

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
