import {
  ABCWidgetFactory,
  DocumentRegistry,
  DocumentWidget
} from '@jupyterlab/docregistry';

import { AfdagModel } from './model';
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
 */
export class AfdagWidgetFactory extends ABCWidgetFactory<
  AfdagDocWidget,
  AfdagModel
> {
  protected createNewWidget(
    context: DocumentRegistry.IContext<AfdagModel>
  ): AfdagDocWidget {
    return new AfdagDocWidget({
      context,
      content: new AfdagEditorPanel(context)
    });
  }
}
