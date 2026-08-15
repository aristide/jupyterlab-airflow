import Form from '@rjsf/core';
import { DescriptionFieldProps, RJSFSchema, UiSchema } from '@rjsf/utils';
import validator from '@rjsf/validator-ajv8';
import * as React from 'react';

import { afdagBaseInput, afdagWidgets } from './rjsfWidgets';
import { InfoBubble } from './InfoBubble';
import { useCanEdit } from './capabilitiesContext';
import { IAfdagVariable } from '../ir';

export interface IAfdagFormProps {
  schema: RJSFSchema;
  uiSchema: UiSchema;
  formData: Record<string, unknown>;
  onChange: (formData: Record<string, unknown>) => void;
  /** The flow's declared variables (PRD §6.10), handed to every field through
   * RJSF's `formContext` so the per-field variable picker can offer them. */
  variables?: IAfdagVariable[];
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

const afdagTemplates = {
  DescriptionFieldTemplate: AfdagDescriptionField,
  // One seam that reaches every plain input in every operator form.
  BaseInputTemplate: afdagBaseInput
};

/**
 * A thin RJSF wrapper with the Airflow Studio custom widgets, live validation,
 * and the default submit button removed (the IR is committed on change, there is
 * no explicit submit). The empty-fragment child suppresses RJSF's submit button.
 */
export function AfdagForm(props: IAfdagFormProps): JSX.Element {
  // Read the capability here rather than threading a `readonly` prop through
  // DagTab / NodeTab / every future tab: this is the single place every Studio
  // form is constructed, and RJSF's own `readonly` on <Form> propagates to
  // every field and widget beneath it (rjsfWidgets already honours
  // `props.readonly`). One line covers forms that do not exist yet.
  const canEdit = useCanEdit();

  return (
    <Form
      className="jp-afdag-rjsf"
      schema={props.schema}
      uiSchema={props.uiSchema}
      formData={props.formData}
      validator={validator}
      widgets={afdagWidgets}
      templates={afdagTemplates}
      formContext={{ variables: props.variables ?? [] }}
      readonly={!canEdit}
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
