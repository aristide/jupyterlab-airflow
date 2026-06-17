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
  has_import_errors?: boolean;
}

export interface ITaskInstance {
  task_id: string;
  dag_run_id?: string;
  dag_id?: string;
  state?: string | null;
  try_number?: number;
  start_date?: string | null;
  end_date?: string | null;
  duration?: number | null;
}

export interface ITaskInstancesRes {
  task_instances: ITaskInstance[];
  total_entries: number;
}

export interface ITaskLogsRes {
  content: string;
}

// `clearTaskInstances` returns the affected set (used as a dry-run preview).
export interface IClearRes {
  task_instances: ITaskInstance[];
  total_entries: number;
}

// Result of deleting a DAG (file removed, history purged).
export interface IPurgeRes {
  dag_id: string;
  removed_file: boolean;
  purged_history: boolean;
}

export interface IDagListRes {
  dags: IDag[];
  total_entries: number;
}

// A deployed Studio DAG whose source `.afdag` was deleted (PRD §6.5.6). The
// reconciliation sweep matches it by the `afdag_id` provenance join.
export interface IOrphan {
  dag_id: string;
  filename: string;
  afdag_id?: string;
}

export interface IOrphansRes {
  orphans: IOrphan[];
  // True when a `.afdag` could not be read/parsed during the sweep — its
  // identity is unknown, so the manager suppresses the destructive prompt that
  // sweep rather than risk falsely flagging a present-but-unreadable source.
  degraded?: boolean;
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
  /** Plain-language contextual help (inline under the field + in the INFO tab). */
  help?: string;
}

export interface IOperatorDef {
  id: string;
  label: string;
  category: string;
  taskIdPrefix: string;
  params: IOperatorParam[];
  taskflow?: 'native' | 'operator';
  handles?: { in?: boolean; out?: boolean };
  // INFO-tab documentation fields (data-only; see the operator registry YAML).
  description?: string;
  docsUrl?: string;
  example?: string;
  provider?: string;
  airflowMinVersion?: string;
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

// A DAG-file import error from `GET /api/v2/importErrors`.
export interface IImportError {
  import_error_id?: number;
  timestamp?: string;
  filename?: string;
  bundle_name?: string;
  stack_trace?: string;
}

export interface IImportErrorsRes {
  import_errors: IImportError[];
  total_entries: number;
}

// One observation of a deploy's tri-state (PRD §6.5.4).
export interface IDeployStatusRes {
  state: 'registered' | 'failed' | 'processing';
  import_error?: IImportError;
  dag?: { dag_id: string; is_paused: boolean };
}

// Deploy state of a dag_id, for choosing the rename path (PRD §6.1.8(B)).
export interface IRenamePreflightRes {
  dag_id: string;
  file_exists: boolean;
  /** The deployed file was edited out of band since Studio wrote it (§6.5.3). */
  drifted: boolean;
  registered: boolean;
  active_runs: number;
}

// Result of retiring the OLD dag_id after a rename migration (PRD §6.1.8(B)).
export interface IRetireRes {
  dag_id: string;
  removed_file: boolean;
  paused?: boolean;
  purged_history: boolean;
}
