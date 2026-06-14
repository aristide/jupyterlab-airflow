import { DocumentRegistry } from '@jupyterlab/docregistry';
import { ReactWidget } from '@jupyterlab/ui-components';
import { Message } from '@lumino/messaging';
import { ISignal, Signal } from '@lumino/signaling';
import { Widget } from '@lumino/widgets';
import * as React from 'react';

import { AfdagModel } from './model';
import { StudioApp } from './components/StudioApp';

import '@xyflow/react/dist/style.css';

/**
 * The Lumino/React content of the `.afdag` document widget. It hosts the
 * ReactFlow-based Airflow Studio canvas and re-fits the view whenever the
 * widget is shown or resized (ReactFlow renders 0x0 if it never remeasures).
 */
export class AfdagEditorPanel extends ReactWidget {
  constructor(context: DocumentRegistry.IContext<AfdagModel>) {
    super();
    this.addClass('jp-afdag-editor');
    this._context = context;
  }

  get resized(): ISignal<this, void> {
    return this._resized;
  }

  protected onResize(msg: Widget.ResizeMessage): void {
    super.onResize(msg);
    this._resized.emit();
  }

  protected onAfterShow(msg: Message): void {
    super.onAfterShow(msg);
    this._resized.emit();
  }

  render(): JSX.Element {
    return <StudioApp context={this._context} resized={this._resized} />;
  }

  private _context: DocumentRegistry.IContext<AfdagModel>;
  private _resized = new Signal<this, void>(this);
}
