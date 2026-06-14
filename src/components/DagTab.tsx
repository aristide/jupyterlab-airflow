import * as React from 'react';

import { dagForm, dagToFormData, formDataToDag } from '../forms';
import { IAfdagIR } from '../ir';
import { AfdagForm } from './AfdagForm';

export interface IDagTabProps {
  dag: IAfdagIR['dag'];
  onDagChange: (patch: Partial<IAfdagIR['dag']>) => void;
}

/**
 * DAG tab: the registry-independent DAG-config form (dag_id, schedule preset,
 * start_date, catchup, retries, tags, owner, params/default_args). Holds local
 * form state so JSON-text fields aren't reformatted mid-edit by the IR
 * round-trip; the parent remounts this (via `key`) on an external reload.
 */
export function DagTab(props: IDagTabProps): JSX.Element {
  const { dag, onDagChange } = props;
  const { schema, uiSchema } = React.useMemo(() => dagForm(), []);
  const [formData, setFormData] = React.useState(() => dagToFormData(dag));

  const handleChange = React.useCallback(
    (next: Record<string, unknown>) => {
      setFormData(next);
      onDagChange(formDataToDag(next));
    },
    [onDagChange]
  );

  return (
    <div className="jp-afdag-tabpanel">
      <AfdagForm
        schema={schema}
        uiSchema={uiSchema}
        formData={formData}
        onChange={handleChange}
      />
    </div>
  );
}
