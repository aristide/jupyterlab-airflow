import {
  ILayoutRestorer,
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { ICommandPalette, WidgetTracker } from '@jupyterlab/apputils';
import { IFileBrowserFactory } from '@jupyterlab/filebrowser';
import { ILauncher } from '@jupyterlab/launcher';
import { ISettingRegistry } from '@jupyterlab/settingregistry';
import { ITranslator, nullTranslator } from '@jupyterlab/translation';

import { AirflowPanel } from './AirflowPanel';
import { AfdagDocWidget, AfdagWidgetFactory } from './factory';
import { airflowIcon } from './icons';
import { AfdagModelFactory } from './model';

namespace CommandIDs {
  export const refresh = 'jupyterlab-airflow:refresh';
  export const createDag = 'jupyterlab-airflow:create-dag';
}

const AFDAG_FILE_TYPE = 'afdag';
const FACTORY = 'Airflow Studio';

/**
 * The manager plugin: a left-sidebar panel that lists DAGs and lets you
 * pause/unpause, trigger runs, and inspect recent run states.
 */
const managerPlugin: JupyterFrontEndPlugin<void> = {
  id: 'jupyterlab-airflow:plugin',
  description: 'A JupyterLab extension for Apache Airflow 3.x',
  autoStart: true,
  optional: [ILayoutRestorer, ICommandPalette, ISettingRegistry, ITranslator],
  activate: (
    app: JupyterFrontEnd,
    restorer: ILayoutRestorer | null,
    palette: ICommandPalette | null,
    settingRegistry: ISettingRegistry | null,
    translator: ITranslator | null
  ) => {
    const trans = (translator ?? nullTranslator).load('jupyterlab_airflow');

    if (settingRegistry) {
      settingRegistry
        .load(managerPlugin.id)
        .then(settings =>
          console.log('jupyterlab-airflow settings loaded:', settings.composite)
        )
        .catch(reason =>
          console.error('Failed to load jupyterlab-airflow settings.', reason)
        );
    }

    const panel = new AirflowPanel(translator ?? undefined);
    panel.id = 'jp-airflow-panel';
    panel.title.icon = airflowIcon;
    panel.title.caption = trans.__('Apache Airflow');

    app.shell.add(panel, 'left', { rank: 300 });

    app.commands.addCommand(CommandIDs.refresh, {
      label: trans.__('Airflow: Refresh DAGs'),
      execute: () => panel.refresh()
    });
    if (palette) {
      palette.addItem({
        command: CommandIDs.refresh,
        category: trans.__('Airflow')
      });
    }

    if (restorer) {
      restorer.add(panel, 'jupyterlab-airflow');
    }

    console.log('JupyterLab extension jupyterlab-airflow is activated!');
  }
};

/**
 * The editor plugin: a main-area visual DAG editor (Airflow Studio) bound to
 * `.afdag` documents. JupyterLab owns open/save/dirty/restore via the document
 * registry; the editor reads and writes the IR graph JSON.
 */
const editorPlugin: JupyterFrontEndPlugin<void> = {
  id: 'jupyterlab-airflow:editor',
  description:
    'Visual no-code DAG editor (Airflow Studio) for .afdag documents',
  autoStart: true,
  optional: [
    ILayoutRestorer,
    ICommandPalette,
    ILauncher,
    IFileBrowserFactory,
    ITranslator
  ],
  activate: (
    app: JupyterFrontEnd,
    restorer: ILayoutRestorer | null,
    palette: ICommandPalette | null,
    launcher: ILauncher | null,
    browserFactory: IFileBrowserFactory | null,
    translator: ITranslator | null
  ) => {
    const trans = (translator ?? nullTranslator).load('jupyterlab_airflow');

    app.docRegistry.addFileType({
      name: AFDAG_FILE_TYPE,
      displayName: trans.__('Airflow DAG'),
      extensions: ['.afdag'],
      mimeTypes: ['application/json'],
      fileFormat: 'text',
      contentType: 'file',
      icon: airflowIcon
    });

    app.docRegistry.addModelFactory(new AfdagModelFactory());

    const services = {
      contents: app.serviceManager.contents,
      openPath: (path: string) =>
        void app.commands.execute('docmanager:open', {
          path,
          factory: FACTORY
        })
    };

    const widgetFactory = new AfdagWidgetFactory(
      {
        name: FACTORY,
        label: trans.__('Airflow Studio'),
        modelName: 'afdag-model',
        fileTypes: [AFDAG_FILE_TYPE],
        defaultFor: [AFDAG_FILE_TYPE]
      },
      services
    );
    app.docRegistry.addWidgetFactory(widgetFactory);

    const tracker = new WidgetTracker<AfdagDocWidget>({
      namespace: 'airflow-studio'
    });
    widgetFactory.widgetCreated.connect((_, widget) => {
      widget.title.icon = airflowIcon;
      void tracker.add(widget);
      widget.context.pathChanged.connect(() => void tracker.save(widget));
    });

    if (restorer) {
      void restorer.restore(tracker, {
        command: 'docmanager:open',
        args: widget => ({ path: widget.context.path, factory: FACTORY }),
        name: widget => widget.context.path
      });
    }

    app.commands.addCommand(CommandIDs.createDag, {
      label: trans.__('Airflow DAG'),
      caption: trans.__('Create a new Airflow DAG in Airflow Studio'),
      icon: airflowIcon,
      execute: async args => {
        const cwd =
          (args['cwd'] as string) ??
          browserFactory?.tracker.currentWidget?.model.path ??
          '';
        const model = await app.commands.execute('docmanager:new-untitled', {
          path: cwd,
          type: 'file',
          ext: 'afdag'
        });
        return app.commands.execute('docmanager:open', {
          path: model.path,
          factory: FACTORY
        });
      }
    });

    if (palette) {
      palette.addItem({
        command: CommandIDs.createDag,
        category: trans.__('Airflow')
      });
    }
    if (launcher) {
      launcher.add({
        command: CommandIDs.createDag,
        category: trans.__('Airflow'),
        rank: 1
      });
    }
    app.contextMenu.addItem({
      command: CommandIDs.createDag,
      selector: '.jp-DirListing-content',
      rank: 100
    });

    console.log('JupyterLab extension jupyterlab-airflow editor is activated!');
  }
};

export default [managerPlugin, editorPlugin];
