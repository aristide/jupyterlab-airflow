import * as React from 'react';

import { generateDag } from '../handler';
import { IAfdagIR } from '../ir';

export interface ICodePanelProps {
  ir: IAfdagIR;
}

/**
 * The CODE tab: a read-only preview of the server-generated Airflow 3.x Python.
 * Codegen is authoritative server-side; this debounces a `POST generate` on each
 * IR change and shows the result (or the validation errors that block it).
 */
export function CodePanel(props: ICodePanelProps): JSX.Element {
  const { ir } = props;
  const [code, setCode] = React.useState('');
  const [errors, setErrors] = React.useState<string[]>([]);
  const [status, setStatus] = React.useState<'idle' | 'loading' | 'error'>(
    'idle'
  );

  React.useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    const handle = window.setTimeout(() => {
      void generateDag(ir).then(res => {
        if (cancelled) {
          return;
        }
        if (res.status === 'OK' && res.data) {
          setCode(res.data.code);
          setErrors(res.data.errors);
          setStatus(res.data.valid ? 'idle' : 'error');
        } else {
          setCode('');
          setErrors([res.error ?? 'Code generation failed']);
          setStatus('error');
        }
      });
    }, 400);
    return () => {
      cancelled = true;
      window.clearTimeout(handle);
    };
  }, [ir]);

  return (
    <div className="jp-afdag-code">
      {errors.length > 0 && (
        <ul className="jp-afdag-code-errors">
          {errors.map((message, index) => (
            <li key={index}>{message}</li>
          ))}
        </ul>
      )}
      {status === 'loading' && !code && (
        <div className="jp-afdag-hint">Generating…</div>
      )}
      {code && (
        <pre className="jp-afdag-code-pre">
          <code>{code}</code>
        </pre>
      )}
      {!code && status !== 'loading' && errors.length === 0 && (
        <div className="jp-afdag-hint">Add tasks to generate a DAG.</div>
      )}
    </div>
  );
}
