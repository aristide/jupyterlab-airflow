// The operator catalogue is served by the Jupyter server extension
// (`GET operators`, backed by the YAML operator registry) and fetched once at
// editor activation via `loadOperators`. The result is cached in a module-level
// index so `getOperator` / `validateNodeParams` stay synchronous for the hot
// render/validation paths. The authoritative validation (parse / DagBag) still
// happens server-side before deploy.

import { listOperators } from './handler';
import { IOperatorDef, IOperatorParam, OperatorWidget } from './interfaces';

// Re-export the registry types so existing `../operators` importers keep working.
export type { IOperatorDef, IOperatorParam, OperatorWidget };

let operators: IOperatorDef[] = [];
let operatorIndex: Record<string, IOperatorDef> = {};
let pending: Promise<IOperatorDef[]> | null = null;

function reindex(list: IOperatorDef[]): void {
  operators = list;
  operatorIndex = {};
  for (const operator of list) {
    operatorIndex[operator.id] = operator;
  }
}

/**
 * Fetch the operator registry from the server extension and cache it. Safe to
 * call repeatedly: concurrent calls share one request and later calls return the
 * cached list unless `force` is set. Rejects if the registry can't be loaded
 * (the caller surfaces this; a retry is allowed because the cache isn't poisoned).
 */
export function loadOperators(force = false): Promise<IOperatorDef[]> {
  if (pending && !force) {
    return pending;
  }
  pending = listOperators(force).then(res => {
    if (res.status !== 'OK' || !res.data) {
      pending = null; // allow a retry
      throw new Error(res.error || 'Failed to load the operator registry');
    }
    reindex(res.data);
    return operators;
  });
  return pending;
}

/** The cached registry (empty until {@link loadOperators} resolves). */
export function getOperators(): IOperatorDef[] {
  return operators;
}

export function getOperator(id: string): IOperatorDef | undefined {
  return operatorIndex[id];
}

export interface IValidationResult {
  valid: boolean;
  missing: string[];
}

/**
 * Client-side, instant required-field validation for a node. The authoritative
 * check (parse / DagBag) happens server-side before deploy.
 */
export function validateNodeParams(
  opId: string,
  params: Record<string, unknown>
): IValidationResult {
  const def = getOperator(opId);
  if (!def) {
    return { valid: false, missing: ['unknown operator'] };
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
