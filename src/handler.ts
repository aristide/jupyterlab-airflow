import { URLExt } from '@jupyterlab/coreutils';
import { ServerConnection } from '@jupyterlab/services';

import {
  IApiRes,
  ICapabilities,
  IClearRes,
  IDagDetails,
  IDagListRes,
  IDagRunsRes,
  IDagRun,
  IDeployLifecycleRes,
  IDeployRes,
  IDeployStatusRes,
  IGenerateRes,
  IHealth,
  IImportErrorsRes,
  INotifierDef,
  IOperatorDef,
  IOrphansRes,
  IDagSourceRes,
  IPurgeRes,
  IRenamePreflightRes,
  IRollbackRes,
  IRetireRes,
  ITaskInstancesRes,
  ITaskLogsRes,
  IValidateRes,
  IConnectionDeleteRes,
  IConnectionSetRes,
  IConnectionsInspectRes,
  IConnectionsListRes,
  IVariableDeleteRes,
  IVariableSetRes,
  IVariablesInspectRes,
  IVariablesListRes
} from './interfaces';
import { IAfdagIR } from './ir';

const NAMESPACE = 'jupyterlab-airflow';

/**
 * Call the jupyterlab-airflow server extension.
 *
 * The server replies with `{ data }` on success or `{ error, detail }` on
 * failure; both are normalised into an {@link IApiRes}.
 */
export async function requestAPI<T>(
  endPoint = '',
  init: RequestInit = {}
): Promise<IApiRes<T>> {
  const settings = ServerConnection.makeSettings();
  const requestUrl = URLExt.join(settings.baseUrl, NAMESPACE, endPoint);

  let response: Response;
  try {
    response = await ServerConnection.makeRequest(requestUrl, init, settings);
  } catch (error: any) {
    throw new ServerConnection.NetworkError(error);
  }

  let data: any = await response.text();
  if (data.length > 0) {
    try {
      data = JSON.parse(data);
    } catch (error) {
      console.log('Not a JSON response body.', response);
    }
  }

  if (!response.ok || (data && data.error)) {
    return {
      status: 'ERR',
      error: (data && data.error) || response.statusText,
      detail: data && data.detail
    };
  }

  return { status: 'OK', data: data.data as T };
}

async function GET<T>(
  act: string,
  params: { [key: string]: string } = {}
): Promise<IApiRes<T>> {
  const query = new URLSearchParams(params).toString();
  return requestAPI<T>(query ? `${act}?${query}` : act);
}

async function POST<T>(
  act: string,
  body: Record<string, unknown>
): Promise<IApiRes<T>> {
  return requestAPI<T>(act, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
}

/**
 * One human-readable message from a failed response.
 *
 * The server splits a failure into `error` (what happened) and `detail` (what
 * to do about it), but every call site used to show only the first half. That
 * is worst exactly where it matters most: a 403 reads "You have view-only
 * access to Airflow Studio." with the actionable "Ask an administrator for
 * edit access." silently dropped.
 */
export function apiError<T>(res: IApiRes<T>, fallback: string): string {
  const head = res.error ?? fallback;
  return res.detail ? `${head} ${res.detail}` : head;
}

export const getHealth = (): Promise<IApiRes<IHealth>> =>
  GET<IHealth>('health');

// Whether this user may run privileged actions (PRD §9). Advisory — the server
// enforces it on every mutating endpoint regardless of the answer here.
export const getCapabilities = (): Promise<IApiRes<ICapabilities>> =>
  GET<ICapabilities>('capabilities');

// `refresh` forces a fresh read of the target Airflow's installed providers
// (the availability annotations); otherwise the server serves its short-TTL cache.
export const listOperators = (
  refresh = false
): Promise<IApiRes<IOperatorDef[]>> =>
  GET<IOperatorDef[]>('operators', refresh ? { refresh: '1' } : {});

export const listNotifiers = (
  refresh = false
): Promise<IApiRes<INotifierDef[]>> =>
  GET<INotifierDef[]>('notifiers', refresh ? { refresh: '1' } : {});

export const generateDag = (ir: IAfdagIR): Promise<IApiRes<IGenerateRes>> =>
  POST<IGenerateRes>('generate', ir as unknown as Record<string, unknown>);

export const validateDag = (ir: IAfdagIR): Promise<IApiRes<IValidateRes>> =>
  POST<IValidateRes>('validate', ir as unknown as Record<string, unknown>);

/** What the editor would otherwise perform after the write (PRD §6.5.4): the
 * rename migration's retire intent, and whether to run the DAG on deploy. Sent
 * WITH the deploy so the server owns the whole lifecycle — a retire intent held
 * in the browser dies with the tab. */
export interface IDeployLifecycleReq {
  retire?: { dag_id: string; purge: boolean } | null;
  run_on_deploy?: boolean;
}

// The `{ir, lifecycle}` envelope is also how the server knows this client
// observes rather than performs; a bare-IR body keeps the old behaviour.
export const deployDag = (
  ir: IAfdagIR,
  lifecycle: IDeployLifecycleReq = {}
): Promise<IApiRes<IDeployRes>> =>
  POST<IDeployRes>('deploy', { ir, lifecycle } as unknown as Record<
    string,
    unknown
  >);

/** Observe a deploy the server is completing. By `deployId` for the deploy this
 * session started; by `afdagId` to re-attach after a page reload. */
export const deployLifecycle = (opts: {
  deployId?: string;
  afdagId?: string;
}): Promise<IApiRes<IDeployLifecycleRes | null>> =>
  GET<IDeployLifecycleRes | null>('deploy/lifecycle', {
    ...(opts.deployId ? { deploy_id: opts.deployId } : {}),
    ...(opts.afdagId ? { afdag_id: opts.afdagId } : {})
  });

/** Stop the server finishing a deploy — the escape hatch closing the tab used to
 * provide, now that closing it no longer stops anything. */
export const cancelDeployLifecycle = (
  deployId: string
): Promise<
  IApiRes<{ deploy_id: string; cancelled: boolean; pending_step?: boolean }>
> => POST('deploy/lifecycle/cancel', { deploy_id: deployId });

/** Re-arm a deploy whose server-side budget ran out ("Keep waiting"). The server
 * owns the remaining steps — notably a rename's retire, whose intent lives only
 * in the journal — so waiting longer has to be asked of it, not re-run here. */
export const resumeDeployLifecycle = (
  deployId: string
): Promise<IApiRes<{ deploy_id: string; resumed: boolean; reason: string }>> =>
  POST('deploy/lifecycle/resume', { deploy_id: deployId });

export const deployStatus = (
  dagId: string,
  filename: string
): Promise<IApiRes<IDeployStatusRes>> =>
  GET<IDeployStatusRes>('deploy/status', { dag_id: dagId, filename });

export const listImportErrors = (): Promise<IApiRes<IImportErrorsRes>> =>
  GET<IImportErrorsRes>('importerrors');

export const listDags = (
  limit = 100,
  dagIdPattern = ''
): Promise<IApiRes<IDagListRes>> =>
  GET<IDagListRes>('dags', {
    limit: String(limit),
    ...(dagIdPattern ? { dag_id_pattern: dagIdPattern } : {})
  });

export const setDagPaused = (
  dagId: string,
  isPaused: boolean
): Promise<IApiRes<unknown>> =>
  POST('dags/pause', { dag_id: dagId, is_paused: isPaused });

// Full DAG detail incl. the serialized `params` — drives the manager's
// trigger-with-conf form (PRD §6.6/§15.10).
export const getDagDetails = (dagId: string): Promise<IApiRes<IDagDetails>> =>
  GET<IDagDetails>('dags/details', { dag_id: dagId });

// Trigger a DAG run. `conf` populates the run's params; a null `logical_date`
// (the default) means "run now" (Airflow 3), or pass an ISO datetime to pin it.
export const triggerDag = (
  dagId: string,
  conf: Record<string, unknown> = {},
  logicalDate: string | null = null
): Promise<IApiRes<IDagRun>> =>
  POST('dags/trigger', { dag_id: dagId, conf, logical_date: logicalDate });

export const deleteDag = (dagId: string): Promise<IApiRes<IPurgeRes>> =>
  POST<IPurgeRes>('dags/delete', { dag_id: dagId });

// Roll a deployed DAG back to its previous version (PRD §6.5.5 / §7): restore the
// `.bak` saved on the last overwrite-deploy.
export const rollbackDag = (dagId: string): Promise<IApiRes<IRollbackRes>> =>
  POST<IRollbackRes>('dags/rollback', { dag_id: dagId });

// Deployed Studio DAGs whose source `.afdag` was deleted (PRD §6.5.6).
export const findOrphans = (): Promise<IApiRes<IOrphansRes>> =>
  GET<IOrphansRes>('dags/orphans');

// Resolve a deployed DAG back to its source `.afdag` Contents path for the
// manager's "Open in Studio to fix" recovery action (PRD §7). `path` is null
// when the source can't be located (pre-provenance deploy / source deleted).
export const findDagSource = (opts: {
  filename?: string;
  dagId?: string;
}): Promise<IApiRes<IDagSourceRes>> =>
  GET<IDagSourceRes>('dags/source', {
    ...(opts.filename ? { filename: opts.filename } : {}),
    ...(opts.dagId ? { dag_id: opts.dagId } : {})
  });

// One DagRun's current state — polled by the editor's run-on-deploy banner.
export const getDagRun = (
  dagId: string,
  runId: string
): Promise<IApiRes<IDagRun>> =>
  GET<IDagRun>('dagruns/get', { dag_id: dagId, run_id: runId });

// Stop an in-flight run (PRD §6.6): Airflow has no cancel endpoint, so this
// PATCHes the run to a terminal state (`failed`) and the scheduler kills its
// running tasks.
export const setDagRunState = (
  dagId: string,
  runId: string,
  state = 'failed'
): Promise<IApiRes<IDagRun>> =>
  POST<IDagRun>('dagruns/state', {
    dag_id: dagId,
    run_id: runId,
    state
  });

// Rename migration (PRD §6.1.8(B)): check the old dag_id's deploy state, then
// (after the new DAG registers) retire the old one — pause+remove, or purge.
export const renamePreflight = (
  dagId: string
): Promise<IApiRes<IRenamePreflightRes>> =>
  GET<IRenamePreflightRes>('dags/rename/preflight', { dag_id: dagId });

export const retireOldDag = (
  dagId: string,
  purge: boolean
): Promise<IApiRes<IRetireRes>> =>
  POST<IRetireRes>('dags/retire', { dag_id: dagId, purge });

export const listDagRuns = (
  dagId: string,
  limit = 10
): Promise<IApiRes<IDagRunsRes>> =>
  GET<IDagRunsRes>('dagruns', { dag_id: dagId, limit: String(limit) });

export const listTaskInstances = (
  dagId: string,
  runId: string
): Promise<IApiRes<ITaskInstancesRes>> =>
  GET<ITaskInstancesRes>('taskinstances', { dag_id: dagId, run_id: runId });

export const getTaskLogs = (
  dagId: string,
  runId: string,
  taskId: string,
  tryNumber = 1
): Promise<IApiRes<ITaskLogsRes>> =>
  GET<ITaskLogsRes>('taskinstances/logs', {
    dag_id: dagId,
    run_id: runId,
    task_id: taskId,
    try_number: String(tryNumber)
  });

export const clearTasks = (
  dagId: string,
  runId: string,
  taskIds: string[],
  dryRun = true
): Promise<IApiRes<IClearRes>> =>
  POST<IClearRes>('taskinstances/clear', {
    dag_id: dagId,
    run_id: runId,
    task_ids: taskIds,
    dry_run: dryRun
  });

/** Every variable in the target Airflow (PRD §6.10) — the picker's source. */
export const listVariables = (): Promise<IApiRes<IVariablesListRes>> =>
  GET<IVariablesListRes>('variables');

/** Reconcile a flow's variable declarations against the live Airflow. Takes the
 * IR because it inspects an unsaved document, not a deployed one. */
export const inspectVariables = (
  ir: IAfdagIR
): Promise<IApiRes<IVariablesInspectRes>> =>
  POST<IVariablesInspectRes>(
    'variables/inspect',
    ir as unknown as Record<string, unknown>
  );

/** Create/update one variable owned by this flow. The server refuses to touch a
 * variable the flow does not own (409). */
export const setVariable = (
  dagId: string,
  key: string,
  value: string,
  description = ''
): Promise<IApiRes<IVariableSetRes>> =>
  POST<IVariableSetRes>('variables/set', {
    dag_id: dagId,
    key,
    value,
    description
  });

/** Delete one variable, only when this flow owns it. */
export const deleteVariable = (
  dagId: string,
  key: string
): Promise<IApiRes<IVariableDeleteRes>> =>
  POST<IVariableDeleteRes>('variables/delete', { dag_id: dagId, key });

/** Every connection in the target Airflow (PRD §6.11) — the picker's source. */
export const listConnections = (): Promise<IApiRes<IConnectionsListRes>> =>
  GET<IConnectionsListRes>('connections');

/** Reconcile a flow's connection declarations + actual conn_id usage against
 * the live Airflow. Takes the IR because it inspects an unsaved document. */
export const inspectConnections = (
  ir: IAfdagIR
): Promise<IApiRes<IConnectionsInspectRes>> =>
  POST<IConnectionsInspectRes>(
    'connections/inspect',
    ir as unknown as Record<string, unknown>
  );

/** Create/update one connection owned by this flow (409 when not owned). */
export const setConnection = (
  dagId: string,
  conn: Record<string, unknown>
): Promise<IApiRes<IConnectionSetRes>> =>
  POST<IConnectionSetRes>('connections/set', { dag_id: dagId, ...conn });

/** Delete one connection, only when this flow owns it. */
export const deleteConnection = (
  dagId: string,
  connId: string
): Promise<IApiRes<IConnectionDeleteRes>> =>
  POST<IConnectionDeleteRes>('connections/delete', {
    dag_id: dagId,
    conn_id: connId
  });
