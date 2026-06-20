import { URLExt } from '@jupyterlab/coreutils';
import { ServerConnection } from '@jupyterlab/services';

import {
  IApiRes,
  IClearRes,
  IDagDetails,
  IDagListRes,
  IDagRunsRes,
  IDagRun,
  IDeployRes,
  IDeployStatusRes,
  IGenerateRes,
  IHealth,
  IImportErrorsRes,
  IOperatorDef,
  IOrphansRes,
  IPurgeRes,
  IRenamePreflightRes,
  IRetireRes,
  ITaskInstancesRes,
  ITaskLogsRes,
  IValidateRes
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

export const getHealth = (): Promise<IApiRes<IHealth>> =>
  GET<IHealth>('health');

// `refresh` forces a fresh read of the target Airflow's installed providers
// (the availability annotations); otherwise the server serves its short-TTL cache.
export const listOperators = (
  refresh = false
): Promise<IApiRes<IOperatorDef[]>> =>
  GET<IOperatorDef[]>('operators', refresh ? { refresh: '1' } : {});

export const generateDag = (ir: IAfdagIR): Promise<IApiRes<IGenerateRes>> =>
  POST<IGenerateRes>('generate', ir as unknown as Record<string, unknown>);

export const validateDag = (ir: IAfdagIR): Promise<IApiRes<IValidateRes>> =>
  POST<IValidateRes>('validate', ir as unknown as Record<string, unknown>);

export const deployDag = (ir: IAfdagIR): Promise<IApiRes<IDeployRes>> =>
  POST<IDeployRes>('deploy', ir as unknown as Record<string, unknown>);

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

// Deployed Studio DAGs whose source `.afdag` was deleted (PRD §6.5.6).
export const findOrphans = (): Promise<IApiRes<IOrphansRes>> =>
  GET<IOrphansRes>('dags/orphans');

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
