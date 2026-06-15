import * as React from 'react';

// Editor actions shared with the ReactFlow custom node/edge components (which
// are registered at module scope and so cannot receive StudioApp callbacks as
// props). StudioApp provides stable `deleteNode`/`deleteEdge` here; AfdagNode,
// AfdagEdge, and the NODE tab consume them. Keeping the value stable (a memoized
// object of `useCallback`s) means hovering/selecting does not churn consumers.

export interface IEditorActions {
  /** Remove a node (task or note) and any incident edges from the live graph. */
  deleteNode: (id: string) => void;
  /** Remove a single dependency edge, leaving both nodes in place. */
  deleteEdge: (id: string) => void;
  /** Update an annotation note card's text. */
  updateNoteText: (id: string, text: string) => void;
}

const NOOP_ACTIONS: IEditorActions = {
  deleteNode: () => undefined,
  deleteEdge: () => undefined,
  updateNoteText: () => undefined
};

export const EditorActionsContext =
  React.createContext<IEditorActions>(NOOP_ACTIONS);

/** Read the editor actions; falls back to no-ops so components render in isolation. */
export function useEditorActions(): IEditorActions {
  return React.useContext(EditorActionsContext);
}
