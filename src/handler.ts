import { URLExt } from '@jupyterlab/coreutils';
import { ServerConnection } from '@jupyterlab/services';

import {
  IApiRes,
  IDagListRes,
  IDagRunsRes,
  IDagRun,
  IHealth
} from './interfaces';

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

export const listDags = (limit = 100): Promise<IApiRes<IDagListRes>> =>
  GET<IDagListRes>('dags', { limit: String(limit) });

export const setDagPaused = (
  dagId: string,
  isPaused: boolean
): Promise<IApiRes<unknown>> =>
  POST('dags/pause', { dag_id: dagId, is_paused: isPaused });

export const triggerDag = (
  dagId: string,
  conf: Record<string, unknown> = {}
): Promise<IApiRes<IDagRun>> => POST('dags/trigger', { dag_id: dagId, conf });

export const listDagRuns = (
  dagId: string,
  limit = 10
): Promise<IApiRes<IDagRunsRes>> =>
  GET<IDagRunsRes>('dagruns', { dag_id: dagId, limit: String(limit) });
