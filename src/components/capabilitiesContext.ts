import * as React from 'react';

import { getCapabilities } from '../handler';

// Whether this user may run privileged actions (PRD §9), shared with every
// component that offers one — including the ReactFlow custom node/edge
// components, which are registered at module scope and so cannot receive
// StudioApp props. Consumers read it with `useCanEdit()` rather than having the
// flag threaded through every intermediate component.
//
// This is a PRESENTATION concern only. The server rejects a privileged request
// from a viewer with a 403 whatever the client believes, so nothing here is a
// security boundary — it exists so the UI reads as coherently restricted
// instead of offering buttons that are guaranteed to fail.

/**
 * Default `true` — edit allowed.
 *
 * Deliberately optimistic. The fetch is asynchronous, so a pessimistic default
 * would blank out the toolbar on every load until it resolved, and a failed
 * request would lock a legitimate editor out of their own work for a reason
 * they cannot see or fix. Being wrong in this direction costs a 403 with a
 * clear message; being wrong in the other direction looks like the extension
 * is broken.
 */
export const CanEditContext = React.createContext<boolean>(true);

/** Read whether the current user may run privileged actions. */
export function useCanEdit(): boolean {
  return React.useContext(CanEditContext);
}

/**
 * Fetch the capability once on mount. Used by the two React roots (the Studio
 * editor and the manager sidebar), each of which then provides the context to
 * its own subtree.
 */
export function useFetchCanEdit(): boolean {
  const [canEdit, setCanEdit] = React.useState(true);

  React.useEffect(() => {
    let cancelled = false;
    void getCapabilities().then(res => {
      // Only ever narrow on a definite answer. A failed request leaves the
      // optimistic default in place — see the note on CanEditContext.
      if (!cancelled && res.status === 'OK' && res.data) {
        setCanEdit(res.data.can_edit);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  return canEdit;
}

/** The one explanation shown wherever an action is unavailable, so the reason
 * is phrased identically in every tooltip and notice. */
export const VIEW_ONLY_HINT =
  'You have view-only access — ask an administrator for edit access.';
