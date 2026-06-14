import { ITranslator, nullTranslator } from '@jupyterlab/translation';
import {
  ReactWidget,
  UseSignal,
  refreshIcon,
  runIcon
} from '@jupyterlab/ui-components';
import { Signal } from '@lumino/signaling';
import * as React from 'react';

import { listDagRuns, listDags, setDagPaused, triggerDag } from './handler';
import { IDag, IDagRun } from './interfaces';

interface IPanelState {
  loading: boolean;
  error: string | null;
  dags: IDag[];
  expanded: { [dagId: string]: IDagRun[] | 'loading' };
}

/**
 * A Lumino widget hosting the Airflow DAG browser. The React tree is driven by
 * a single mutable state object pushed through a Lumino signal so the widget
 * can be re-rendered imperatively (refresh, trigger, pause…).
 */
export class AirflowPanel extends ReactWidget {
  private _state: IPanelState = {
    loading: false,
    error: null,
    dags: [],
    expanded: {}
  };
  private _changed = new Signal<this, IPanelState>(this);
  private _trans: ReturnType<ITranslator['load']>;

  constructor(translator?: ITranslator) {
    super();
    this.addClass('jp-airflow-panel');
    this._trans = (translator ?? nullTranslator).load('jupyterlab_airflow');
    void this.refresh();
  }

  private _emit(patch: Partial<IPanelState>): void {
    this._state = { ...this._state, ...patch };
    this._changed.emit(this._state);
  }

  async refresh(): Promise<void> {
    this._emit({ loading: true, error: null });
    const res = await listDags();
    if (res.status === 'ERR') {
      this._emit({ loading: false, error: res.error ?? 'Unknown error' });
      return;
    }
    this._emit({
      loading: false,
      error: null,
      dags: res.data?.dags ?? [],
      expanded: {}
    });
  }

  private async _toggleExpand(dag: IDag): Promise<void> {
    const expanded = { ...this._state.expanded };
    if (dag.dag_id in expanded) {
      delete expanded[dag.dag_id];
      this._emit({ expanded });
      return;
    }
    expanded[dag.dag_id] = 'loading';
    this._emit({ expanded });
    const res = await listDagRuns(dag.dag_id);
    const next = { ...this._state.expanded };
    next[dag.dag_id] = res.status === 'OK' ? res.data?.dag_runs ?? [] : [];
    this._emit({ expanded: next });
  }

  private async _togglePause(dag: IDag): Promise<void> {
    const res = await setDagPaused(dag.dag_id, !dag.is_paused);
    if (res.status === 'OK') {
      const dags = this._state.dags.map(d =>
        d.dag_id === dag.dag_id ? { ...d, is_paused: !d.is_paused } : d
      );
      this._emit({ dags });
    } else {
      this._emit({ error: res.error ?? 'Failed to update DAG' });
    }
  }

  private async _trigger(dag: IDag): Promise<void> {
    const res = await triggerDag(dag.dag_id);
    if (res.status === 'ERR') {
      this._emit({ error: res.error ?? 'Failed to trigger DAG' });
      return;
    }
    // Refresh the run list if the DAG is expanded.
    if (dag.dag_id in this._state.expanded) {
      const runs = await listDagRuns(dag.dag_id);
      const next = { ...this._state.expanded };
      next[dag.dag_id] = runs.status === 'OK' ? runs.data?.dag_runs ?? [] : [];
      this._emit({ expanded: next });
    }
  }

  render(): JSX.Element {
    return (
      <UseSignal signal={this._changed} initialArgs={this._state}>
        {(_, state) => this._renderBody(state ?? this._state)}
      </UseSignal>
    );
  }

  private _renderBody(state: IPanelState): JSX.Element {
    const trans = this._trans;
    return (
      <div className="jp-airflow-root">
        <div className="jp-airflow-header">
          <span className="jp-airflow-title">{trans.__('Airflow DAGs')}</span>
          <button
            className="jp-airflow-iconbtn"
            title={trans.__('Refresh')}
            onClick={() => void this.refresh()}
          >
            <refreshIcon.react tag="span" width="16px" height="16px" />
          </button>
        </div>

        {state.loading && (
          <div className="jp-airflow-status">{trans.__('Loading…')}</div>
        )}

        {state.error && (
          <div className="jp-airflow-error">
            {state.error}
            <div className="jp-airflow-hint">
              {trans.__(
                'Check the AIRFLOW_API_URL / AIRFLOW_USERNAME / AIRFLOW_PASSWORD environment variables on the Jupyter server.'
              )}
            </div>
          </div>
        )}

        {!state.loading && !state.error && state.dags.length === 0 && (
          <div className="jp-airflow-status">{trans.__('No DAGs found.')}</div>
        )}

        <ul className="jp-airflow-list">
          {state.dags.map(dag => this._renderDag(dag, state))}
        </ul>
      </div>
    );
  }

  private _renderDag(dag: IDag, state: IPanelState): JSX.Element {
    const trans = this._trans;
    const runs = state.expanded[dag.dag_id];
    const schedule =
      dag.timetable_summary ||
      (typeof dag.schedule_interval === 'string'
        ? dag.schedule_interval
        : dag.schedule_interval?.value) ||
      '—';
    return (
      <li key={dag.dag_id} className="jp-airflow-dag">
        <div className="jp-airflow-dagrow">
          <button
            className="jp-airflow-expand"
            onClick={() => void this._toggleExpand(dag)}
            title={trans.__('Show recent runs')}
          >
            {dag.dag_id in state.expanded ? '▾' : '▸'}
          </button>
          <span className="jp-airflow-dagname" title={dag.description ?? ''}>
            {dag.dag_display_name || dag.dag_id}
          </span>
          <span className="jp-airflow-schedule">{schedule}</span>
          <label
            className="jp-airflow-pause"
            title={dag.is_paused ? trans.__('Paused') : trans.__('Active')}
          >
            <input
              type="checkbox"
              checked={!dag.is_paused}
              onChange={() => void this._togglePause(dag)}
            />
          </label>
          <button
            className="jp-airflow-iconbtn"
            title={trans.__('Trigger DAG')}
            onClick={() => void this._trigger(dag)}
          >
            <runIcon.react tag="span" width="16px" height="16px" />
          </button>
        </div>
        {dag.dag_id in state.expanded && (
          <ul className="jp-airflow-runs">
            {runs === 'loading' ? (
              <li className="jp-airflow-status">{trans.__('Loading runs…')}</li>
            ) : (runs ?? []).length === 0 ? (
              <li className="jp-airflow-status">{trans.__('No runs yet.')}</li>
            ) : (
              (runs as IDagRun[]).map(run => (
                <li key={run.dag_run_id} className="jp-airflow-run">
                  <span
                    className={`jp-airflow-state jp-airflow-state-${run.state}`}
                  >
                    {run.state}
                  </span>
                  <span className="jp-airflow-runid">{run.dag_run_id}</span>
                </li>
              ))
            )}
          </ul>
        )}
      </li>
    );
  }
}
