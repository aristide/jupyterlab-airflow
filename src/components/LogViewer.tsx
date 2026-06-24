import { ITranslator } from '@jupyterlab/translation';
import * as React from 'react';

import { IStructuredLogEvent } from '../interfaces';

type Trans = ReturnType<ITranslator['load']>;

export interface ILogViewerData {
  dagId: string;
  runId: string;
  taskId: string;
  /** The try currently shown. */
  tryNumber: number;
  /** Highest available try (the task instance's try_number). */
  maxTry: number;
  /** Flattened log text (Copy/Download + the plain-text fallback), or null while
   * loading. */
  text: string | null;
  /** Structured events when Airflow returned them (PRD §6.6) — the viewer then
   * colours by the server-provided `level` instead of guessing from text.
   * Absent for plain-text logs (falls back to {@link classifyLine}). */
  events?: IStructuredLogEvent[];
  /** True when the log was capped while still streaming (a running task) — shown
   * as an "incomplete, refresh for more" note. */
  truncated?: boolean;
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

const LEVEL_TOKEN: Record<string, Level> = {
  CRITICAL: 'critical',
  FATAL: 'critical',
  ERROR: 'error',
  WARNING: 'warning',
  WARN: 'warning',
  INFO: 'info',
  DEBUG: 'debug'
};

// Best-effort per-line level classification from the line text — robust to
// Airflow's log formatting without a structured-event API. Airflow formats a
// line as `[ts] {file:lineno} LEVEL - message`, so match the level token *in
// its position* (right after the `{file}` marker, or at the start of a bare
// line) — NOT anywhere in the line. Matching anywhere would upgrade a benign
// INFO line that merely mentions "ERROR" and, worse, mis-target the
// autoscroll-to-first-error. A Python traceback (no level token) → error.
export function classifyLine(line: string): Level {
  const m =
    /\}\s+(CRITICAL|FATAL|ERROR|WARNING|WARN|INFO|DEBUG)\b/.exec(line) ??
    /^\s*(CRITICAL|FATAL|ERROR|WARNING|WARN|INFO|DEBUG)\b/.exec(line);
  if (m) {
    return LEVEL_TOKEN[m[1]];
  }
  if (
    /^\s*Traceback \(most recent call last\)/.test(line) ||
    /^\s*File ".*", line \d+/.test(line) ||
    /^\w*(?:Error|Exception):/.test(line)
  ) {
    return 'error';
  }
  return 'plain';
}

/** Map a structured event's `level` (e.g. "INFO" / "warning") to a {@link Level},
 * or null when absent/unknown so the caller falls back to {@link classifyLine}. */
export function levelFromStructured(level: string | undefined): Level | null {
  if (!level) {
    return null;
  }
  return LEVEL_TOKEN[level.toUpperCase()] ?? null;
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
  const [copied, setCopied] = React.useState<'' | 'ok' | 'fail'>('');
  const bodyRef = React.useRef<HTMLDivElement>(null);

  const lines = React.useMemo(() => {
    // Prefer the server's structured events (accurate per-event level); fall back
    // to client-side text classification for plain-text logs (PRD §6.6).
    if (data.events && data.events.length > 0) {
      return data.events.map((event, i) => ({
        n: i + 1,
        // Compose timestamp + LEVEL + message (skipping absent parts), mirroring
        // the server's flattened line — so search/errors-only match the level and
        // an event with metadata but an empty message still renders a real line.
        text: [
          event.timestamp,
          event.level ? event.level.toUpperCase() : '',
          event.event
        ]
          .filter(Boolean)
          .join(' '),
        level: levelFromStructured(event.level) ?? classifyLine(event.event)
      }));
    }
    const text = data.text ?? '';
    return text.split('\n').map((raw, i) => ({
      n: i + 1,
      text: raw,
      level: classifyLine(raw)
    }));
  }, [data.events, data.text]);

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
  // "(empty log)" = no log content at all — whether that's the plain-text empty
  // case (one blank line) or structured events that carry no visible text. Any
  // non-blank line (incl. an event whose timestamp/level renders) → not empty.
  const isEmptyLog =
    data.text !== null &&
    lines.length > 0 &&
    lines.every(line => (line.text ?? '').trim() === '');

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
  }, [data.text, data.events]);

  const copy = async (): Promise<void> => {
    const text = data.text ?? '';
    let ok = false;
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        ok = true;
      } else {
        // Non-secure context (plain HTTP, a realistic dev/JupyterHub setup) has
        // no async Clipboard API — fall back to a hidden textarea + execCommand.
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        ok = document.execCommand('copy');
        document.body.removeChild(ta);
      }
    } catch {
      ok = false;
    }
    setCopied(ok ? 'ok' : 'fail');
    window.setTimeout(() => setCopied(''), 1500);
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
          onClick={() => void copy()}
          disabled={data.text === null}
        >
          {copied === 'ok'
            ? trans.__('Copied')
            : copied === 'fail'
              ? trans.__('Copy failed')
              : trans.__('Copy')}
        </button>
        <button
          className="jp-airflow-linkbtn"
          onClick={download}
          disabled={data.text === null}
        >
          {trans.__('Download')}
        </button>
      </div>

      {data.truncated && data.text !== null && data.error === null && (
        <div className="jp-airflow-logtruncated">
          {trans.__(
            '⚠ Showing the log so far — the task is still producing output. Reselect the attempt to refresh.'
          )}
        </div>
      )}

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
