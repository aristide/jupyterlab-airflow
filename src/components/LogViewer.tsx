import { ITranslator } from '@jupyterlab/translation';
import * as React from 'react';

type Trans = ReturnType<ITranslator['load']>;

export interface ILogViewerData {
  dagId: string;
  runId: string;
  taskId: string;
  /** The try currently shown. */
  tryNumber: number;
  /** Highest available try (the task instance's try_number). */
  maxTry: number;
  /** Log text, or null while loading. */
  text: string | null;
  /** Non-null when the fetch failed (kept distinct from log content). */
  error: string | null;
}

export interface ILogViewerProps {
  data: ILogViewerData;
  trans: Trans;
  onSelectTry: (tryNumber: number) => void;
  onClose: () => void;
}

export type Level =
  | 'critical'
  | 'error'
  | 'warning'
  | 'info'
  | 'debug'
  | 'plain';

const PROBLEM: ReadonlySet<Level> = new Set<Level>([
  'critical',
  'error',
  'warning'
]);

// Best-effort per-line level classification from the line text — robust to
// Airflow's log formatting without depending on a structured-event API. A
// Python traceback (no level token) is treated as an error so it stands out.
export function classifyLine(line: string): Level {
  if (/\b(?:CRITICAL|FATAL)\b/.test(line)) {
    return 'critical';
  }
  if (
    /\bERROR\b/.test(line) ||
    /^\s*Traceback \(most recent call last\)/.test(line) ||
    /^\s*File ".*", line \d+/.test(line) ||
    /^\w*(?:Error|Exception):/.test(line)
  ) {
    return 'error';
  }
  if (/\b(?:WARNING|WARN)\b/.test(line)) {
    return 'warning';
  }
  if (/\bINFO\b/.test(line)) {
    return 'info';
  }
  if (/\bDEBUG\b/.test(line)) {
    return 'debug';
  }
  return 'plain';
}

/**
 * A friendly task-log viewer (PRD §6.6 / §15.9): per-level colouring + glyph,
 * traceback emphasis, autoscroll to the first error, an attempt selector,
 * search / errors-only filter, Copy / Download, and a wrap toggle. Replaces the
 * raw <pre> dump. Level/timestamp are derived from the line text client-side.
 */
export function LogViewer(props: ILogViewerProps): JSX.Element {
  const { data, trans } = props;
  const [search, setSearch] = React.useState('');
  const [errorsOnly, setErrorsOnly] = React.useState(false);
  const [wrap, setWrap] = React.useState(true);
  const bodyRef = React.useRef<HTMLDivElement>(null);

  const lines = React.useMemo(() => {
    const text = data.text ?? '';
    return text.split('\n').map((raw, i) => ({
      n: i + 1,
      text: raw,
      level: classifyLine(raw)
    }));
  }, [data.text]);

  const needle = search.trim().toLowerCase();
  const visible = lines.filter(line => {
    if (errorsOnly && !PROBLEM.has(line.level)) {
      return false;
    }
    if (needle && !line.text.toLowerCase().includes(needle)) {
      return false;
    }
    return true;
  });

  const errorCount = lines.filter(
    line => line.level === 'error' || line.level === 'critical'
  ).length;
  const isEmptyLog =
    data.text !== null && lines.length <= 1 && (lines[0]?.text ?? '') === '';

  // Autoscroll to the first error each time a log loads, so a failure buried in
  // hundreds of INFO lines is visible immediately.
  React.useEffect(() => {
    if (data.text === null) {
      return;
    }
    const el = bodyRef.current?.querySelector('.jp-airflow-logline.jp-mod-err');
    if (el) {
      el.scrollIntoView({ block: 'center' });
    } else if (bodyRef.current) {
      bodyRef.current.scrollTop = 0;
    }
  }, [data.text]);

  const copy = (): void => {
    void navigator.clipboard?.writeText(data.text ?? '');
  };

  const download = (): void => {
    const blob = new Blob([data.text ?? ''], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${data.taskId}.try${data.tryNumber}.log`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const tries: number[] = [];
  for (let t = 1; t <= Math.max(1, data.maxTry); t++) {
    tries.push(t);
  }

  return (
    <div className="jp-airflow-modal jp-airflow-logs">
      <div className="jp-airflow-modal-head">
        <span className="jp-airflow-logtitle">
          {data.taskId}
          {errorCount > 0 && (
            <span className="jp-airflow-logerrcount">
              {' '}
              {trans.__('· %1 error(s)', errorCount)}
            </span>
          )}
        </span>
        <button
          className="jp-airflow-iconbtn"
          aria-label={trans.__('Close')}
          onClick={props.onClose}
        >
          ✕
        </button>
      </div>

      <div className="jp-airflow-logtoolbar">
        {data.maxTry > 1 && (
          <label className="jp-airflow-logtry">
            {trans.__('Attempt')}
            <select
              value={data.tryNumber}
              onChange={event => props.onSelectTry(Number(event.target.value))}
            >
              {tries.map(t => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </label>
        )}
        <input
          className="jp-airflow-logsearch"
          type="search"
          placeholder={trans.__('Search…')}
          value={search}
          onChange={event => setSearch(event.target.value)}
        />
        <label className="jp-airflow-logchk">
          <input
            type="checkbox"
            checked={errorsOnly}
            onChange={event => setErrorsOnly(event.target.checked)}
          />
          {trans.__('Errors only')}
        </label>
        <button className="jp-airflow-linkbtn" onClick={() => setWrap(w => !w)}>
          {wrap ? trans.__('No wrap') : trans.__('Wrap')}
        </button>
        <button
          className="jp-airflow-linkbtn"
          onClick={copy}
          disabled={data.text === null}
        >
          {trans.__('Copy')}
        </button>
        <button
          className="jp-airflow-linkbtn"
          onClick={download}
          disabled={data.text === null}
        >
          {trans.__('Download')}
        </button>
      </div>

      <div
        ref={bodyRef}
        className={`jp-airflow-logbody${wrap ? '' : ' jp-mod-nowrap'}`}
      >
        {data.error !== null ? (
          <div className="jp-airflow-logstate jp-mod-err">{data.error}</div>
        ) : data.text === null ? (
          <div className="jp-airflow-logstate">{trans.__('Loading…')}</div>
        ) : isEmptyLog ? (
          <div className="jp-airflow-logstate">{trans.__('(empty log)')}</div>
        ) : visible.length === 0 ? (
          <div className="jp-airflow-logstate">
            {trans.__('No matching lines')}
          </div>
        ) : (
          visible.map(line => (
            <div
              key={line.n}
              className={`jp-airflow-logline jp-airflow-log-${line.level}${
                line.level === 'error' || line.level === 'critical'
                  ? ' jp-mod-err'
                  : ''
              }`}
            >
              <span className="jp-airflow-logln">{line.n}</span>
              <span className="jp-airflow-logln-text">{line.text || ' '}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
