import { Contents } from '@jupyterlab/services';

/**
 * App-level services the editor needs beyond the document context: the Contents
 * manager (for the SAVED tab's workspace `.afdag` listing) and a way to open a
 * document by path. Threaded from the editor plugin through the widget factory
 * so the React app stays decoupled from `JupyterFrontEnd`. Optional everywhere —
 * when absent (e.g. tests), the SAVED tab degrades to a hint.
 */
export interface IStudioServices {
  contents: Contents.IManager;
  openPath: (path: string) => void;
}
