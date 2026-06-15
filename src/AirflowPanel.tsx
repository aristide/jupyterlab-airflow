import { ITranslator, nullTranslator } from '@jupyterlab/translation';
import { ReactWidget } from '@jupyterlab/ui-components';
import { Signal } from '@lumino/signaling';
import * as React from 'react';

import { ManagerApp } from './components/ManagerApp';

/**
 * The left-sidebar Airflow Resource Manager. A thin Lumino shell that hosts the
 * React {@link ManagerApp}; `refresh()` (wired to the command palette) re-fetches
 * by emitting a signal the app subscribes to.
 */
export class AirflowPanel extends ReactWidget {
  private _refresh = new Signal<this, void>(this);
  private _trans: ReturnType<ITranslator['load']>;

  constructor(translator?: ITranslator) {
    super();
    this.addClass('jp-airflow-panel');
    this._trans = (translator ?? nullTranslator).load('jupyterlab_airflow');
  }

  refresh(): void {
    this._refresh.emit();
  }

  render(): JSX.Element {
    return <ManagerApp trans={this._trans} refreshSignal={this._refresh} />;
  }
}
