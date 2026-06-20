import * as React from 'react';

import { generateDag } from '../handler';
import { IAfdagIR } from '../ir';
import { CodeMirrorField } from './CodeMirrorField';

export interface ICodePanelProps {
  ir: IAfdagIR;
  /** Instant client-side issues (cycle, missing required fields). */
  clientErrors: string[];
}

/**
 * CODE tab: a read-only preview of the server-generated Airflow 3.x Python with
 * a Generate DAG button and a validation panel. Codegen is authoritative
 * server-side; this debounces a `POST generate` on each IR change (and on demand)
 * and shows the result plus both client-side and server-side validation messages.
 */
export function CodePanel(props: ICodePanelProps): JSX.Element {
  const { ir, clientErrors } = props;
  const [code, setCode] = React.useState('');
  const [serverErrors, setServerErrors] = React.useState<string[]>([]);
  const [status, setStatus] = React.useState<'idle' | 'loading' | 'error'>(
    'idle'
  );
  const [nonce, setNonce] = React.useState(0);

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
          setServerErrors(res.data.errors);
          setStatus(res.data.valid ? 'idle' : 'error');
        } else {
          setCode('');
          setServerErrors([res.error ?? 'Code generation failed']);
          setStatus('error');
        }
      });
    }, 400);
    return () => {
      cancelled = true;
      window.clearTimeout(handle);
    };
  }, [ir, nonce]);

  const messages = [...clientErrors, ...serverErrors];

  return (
    <div className="jp-afdag-tabpanel jp-afdag-code">
      <div className="jp-afdag-saved-head">
        <span>
          Generated DAG (
          {ir.syntax_style === 'traditional' ? 'Traditional' : 'TaskFlow'})
        </span>
        <button
          className="jp-afdag-btn"
          onClick={() => setNonce(n => n + 1)}
          title="Regenerate the DAG from the current graph"
        >
          Generate DAG
        </button>
      </div>
      {messages.length > 0 && (
        <ul className="jp-afdag-code-errors">
          {messages.map((message, index) => (
            <li key={index}>{message}</li>
          ))}
        </ul>
      )}
      {status === 'loading' && !code && (
        <div className="jp-afdag-hint">Generating…</div>
      )}
      {code && <CodeMirrorField language="python" value={code} readOnly />}
      {!code && status !== 'loading' && messages.length === 0 && (
        <div className="jp-afdag-hint">Add tasks to generate a DAG.</div>
      )}
    </div>
  );
}
