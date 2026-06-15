// Shapes returned by the jupyterlab-airflow server extension. The server proxies
// the Airflow REST API (/api/v2) and wraps every response as { data } or
// { error, detail }.

export interface IApiRes<T> {
  status: 'OK' | 'ERR';
  data?: T;
  error?: string;
  detail?: unknown;
}

export interface IDag {
  dag_id: string;
  dag_display_name?: string;
  is_paused: boolean;
  description?: string | null;
  timetable_summary?: string | null;
  schedule_interval?: { value?: string } | string | null;
  tags?: Array<{ name: string }>;
  owners?: string[];
  next_dagrun_logical_date?: string | null;
  last_parsed_time?: string | null;
}

export interface IDagListRes {
  dags: IDag[];
  total_entries: number;
}

export interface IDagRun {
  dag_run_id: string;
  dag_id: string;
  state: string;
  run_type?: string;
  logical_date?: string | null;
  start_date?: string | null;
  end_date?: string | null;
}

export interface IDagRunsRes {
  dag_runs: IDagRun[];
  total_entries: number;
}

export interface IHealth {
  ok: boolean;
  base_url: string;
  username: string;
}

// Operator registry, served by `GET operators` (the server reads the bundled
// + optional user operator YAML files). Drives the palette and node forms.
export type OperatorWidget = 'text' | 'textarea' | 'code' | 'json';

export interface IOperatorParam {
  name: string;
  label: string;
  required?: boolean;
  widget?: OperatorWidget;
  type?: string;
  default?: unknown;
}

export interface IOperatorDef {
  id: string;
  label: string;
  category: string;
  taskIdPrefix: string;
  params: IOperatorParam[];
  taskflow?: 'native' | 'operator';
  handles?: { in?: boolean; out?: boolean };
}

// Result of `POST generate` (IR → Airflow 3.x Python). Validation failures come
// back in `errors` with `valid: false`; `code` is best-effort.
export interface IGenerateRes {
  code: string;
  valid: boolean;
  errors: string[];
}

// Stage 7 (DagBag import) outcome. `skipped` when Airflow isn't importable in
// the Jupyter env (the authoritative check then comes from /importErrors).
export interface IDagBagResult {
  status: 'ok' | 'skipped' | 'error';
  detail?: unknown;
  dags?: string[];
}

// Result of `POST validate` — the full Appendix E pipeline, no write.
export interface IValidateRes {
  valid: boolean;
  code: string;
  errors: string[];
  dagbag: IDagBagResult;
}

// Result of `POST deploy` — validate then atomic shared-volume write.
export interface IDeployRes {
  deployed: boolean;
  path?: string;
  filename?: string;
  dag_id: string;
  warnings: string[];
  errors: string[];
  dagbag: IDagBagResult;
}
