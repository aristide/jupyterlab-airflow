import {
  ABCWidgetFactory,
  DocumentRegistry,
  DocumentWidget
} from '@jupyterlab/docregistry';

import { AfdagModel } from './model';
import { IStudioServices } from './services';
import { AfdagEditorPanel } from './widget';

/**
 * The document widget wrapping the visual editor content.
 */
export class AfdagDocWidget extends DocumentWidget<
  AfdagEditorPanel,
  AfdagModel
> {}

/**
 * Widget factory that opens `.afdag` documents in the Airflow Studio editor.
 * The optional app services (Contents + open-by-path) are forwarded to the
 * editor for the SAVED tab.
 */
export class AfdagWidgetFactory extends ABCWidgetFactory<
  AfdagDocWidget,
  AfdagModel
> {
  constructor(
    options: DocumentRegistry.IWidgetFactoryOptions<AfdagDocWidget>,
    services: IStudioServices | null = null
  ) {
    super(options);
    this._services = services;
  }

  protected createNewWidget(
    context: DocumentRegistry.IContext<AfdagModel>
  ): AfdagDocWidget {
    return new AfdagDocWidget({
      context,
      content: new AfdagEditorPanel(context, this._services)
    });
  }

  private _services: IStudioServices | null;
}
