// The notifier registry (PRD §6.8) — served by `GET notifiers` (backed by the
// YAML notifier registry) and fetched once at editor activation via
// `loadNotifiers`. Cached in a module-level list so the Notifications tab can
// read it synchronously, mirroring `operators.ts`.

import { listNotifiers } from './handler';
import { INotifierDef } from './interfaces';

let notifiers: INotifierDef[] = [];
let pending: Promise<INotifierDef[]> | null = null;

/**
 * Fetch the notifier registry from the server extension and cache it. Concurrent
 * calls share one request; later calls return the cached list unless `force` is
 * set. Rejects if it can't be loaded (the cache isn't poisoned, so a retry is
 * allowed).
 */
export function loadNotifiers(force = false): Promise<INotifierDef[]> {
  if (pending && !force) {
    return pending;
  }
  pending = listNotifiers(force).then(res => {
    if (res.status !== 'OK' || !res.data) {
      pending = null; // allow a retry
      throw new Error(res.error || 'Failed to load the notifier registry');
    }
    notifiers = res.data;
    return notifiers;
  });
  return pending;
}

/** The cached notifier registry (empty until {@link loadNotifiers} resolves). */
export function getNotifiers(): INotifierDef[] {
  return notifiers;
}

export function getNotifier(id: string): INotifierDef | undefined {
  return notifiers.find(notifier => notifier.id === id);
}

/** Client-side required-field check for a notifier's params (PRD §6.8), the
 * notifier analogue of `validateNodeParams`. Feeds the editor error badge so a
 * notifier missing a required field (e.g. Slack `text`) blocks Deploy. */
export function validateNotifierParams(
  notifierId: string,
  params: Record<string, unknown>
): { valid: boolean; missing: string[] } {
  const def = getNotifier(notifierId);
  if (!def) {
    return { valid: false, missing: ['unknown notifier'] };
  }
  const missing = def.params
    .filter(param => param.required)
    .filter(param => {
      const value = params[param.name];
      return (
        value === undefined || value === null || String(value).trim() === ''
      );
    })
    .map(param => param.label);
  return { valid: missing.length === 0, missing };
}
