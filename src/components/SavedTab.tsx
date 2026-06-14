import { Contents } from '@jupyterlab/services';
import * as React from 'react';

import { IStudioServices } from '../services';

export interface ISavedTabProps {
  services: IStudioServices | null;
  currentPath: string;
}

const MAX_DEPTH = 4;

async function listAfdag(
  contents: Contents.IManager,
  path = '',
  depth = 0,
  acc: string[] = []
): Promise<string[]> {
  if (depth > MAX_DEPTH) {
    return acc;
  }
  const model = await contents.get(path, { content: true });
  for (const item of (model.content as Contents.IModel[]) ?? []) {
    if (item.type === 'directory') {
      if (!item.name.startsWith('.')) {
        await listAfdag(contents, item.path, depth + 1, acc);
      }
    } else if (item.path.endsWith('.afdag')) {
      acc.push(item.path);
    }
  }
  return acc;
}

/**
 * SAVED tab: lists `.afdag` documents in the workspace (Contents API) so they
 * can be reopened; highlights the document currently open in this editor.
 * Deploy-status marking is deferred until the manager correlates DAGs by
 * `afdag_id`.
 */
export function SavedTab(props: ISavedTabProps): JSX.Element {
  const { services, currentPath } = props;
  const [files, setFiles] = React.useState<string[] | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const refresh = React.useCallback(() => {
    if (!services) {
      return;
    }
    setError(null);
    setFiles(null);
    listAfdag(services.contents)
      .then(found => setFiles(found.sort()))
      .catch(err => setError(String((err && err.message) || err)));
  }, [services]);

  React.useEffect(() => {
    refresh();
  }, [refresh]);

  if (!services) {
    return (
      <div className="jp-afdag-tabpanel">
        <div className="jp-afdag-hint">
          Saved documents are unavailable in this context.
        </div>
      </div>
    );
  }

  return (
    <div className="jp-afdag-tabpanel">
      <div className="jp-afdag-saved-head">
        <span>Workspace .afdag documents</span>
        <button className="jp-afdag-btn" onClick={refresh}>
          Refresh
        </button>
      </div>
      {error && <div className="jp-afdag-hint jp-mod-error">{error}</div>}
      {files === null && !error && (
        <div className="jp-afdag-hint">Loading…</div>
      )}
      {files !== null && files.length === 0 && (
        <div className="jp-afdag-hint">No .afdag documents found.</div>
      )}
      {files !== null && files.length > 0 && (
        <ul className="jp-afdag-saved-list">
          {files.map(path => (
            <li key={path}>
              <button
                className={
                  path === currentPath
                    ? 'jp-afdag-saved-item jp-mod-current'
                    : 'jp-afdag-saved-item'
                }
                title={`Open ${path}`}
                onClick={() => services.openPath(path)}
              >
                {path}
                {path === currentPath && (
                  <span className="jp-afdag-saved-tag"> open</span>
                )}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
