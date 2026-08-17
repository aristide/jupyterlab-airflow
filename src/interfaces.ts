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

// One DAG param from `GET /dags/{id}/details`. Airflow serializes each param as
// `{value (default), description, schema}` where `schema` is a JSON-Schema
// fragment (type/enum/format/minimum/maximum). Drives the trigger conf form.
export interface IDagParamSchema {
  type?: string | string[];
  enum?: unknown[];
  format?: string;
  minimum?: number;
  maximum?: number;
  [key: string]: unknown;
}

export interface IDagParam {
  value: unknown;
  description?: string | null;
  schema?: IDagParamSchema;
}

// Subset of `GET /dags/{id}/details` the trigger conf form needs (PRD §15.10).
export interface IDagDetails {
  dag_id: string;
  params?: Record<string, IDagParam> | null;
  [key: string]: unknown;
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

// One structured log event from Airflow 3's logs endpoint (PRD §6.6). `event` is
// the message; `level` (when the handler emits it) is authoritative — the viewer
// colours by it instead of guessing from the line text. Verified against
// apache-airflow-core 3.0.2 `StructuredLogMessage` (event*, timestamp?, + extras).
export interface IStructuredLogEvent {
  event: string;
  timestamp?: string;
  level?: string;
  logger?: string;
}

export interface ITaskLogsRes {
  /** Flattened log text (Copy/Download + the plain-text fallback). */
  content: string;
  /** Structured events when Airflow returned them; absent for plain-text logs. */
  events?: IStructuredLogEvent[];
  /** True when the log was capped while more remained (a still-running task that
   * keeps streaming past the chunk budget) — the viewer flags it as incomplete. */
  truncated?: boolean;
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

/** A deployed `.py` whose source `.afdag` is alive but has since been renamed,
 * so it still deploys the flow's OLD dag_id (PRD §6.1.8(B) / §15.11). Distinct
 * from an orphan: the source exists, so the remedy is a keep-history retire,
 * not the destructive purge an orphan invites. */
export interface ISupersededDag {
  /** The stale dag_id this file still deploys. */
  dag_id: string;
  filename: string;
  afdag_id?: string;
  /** What the source flow is called now. */
  current_dag_id: string;
  /** Contents-relative path of the source `.afdag`, for "open in Studio". */
  source_path?: string;
}

export interface IOrphansRes {
  orphans: IOrphan[];
  /** Renamed-but-not-retired leftovers; see {@link ISupersededDag}. Optional so
   * a response from an older server still typechecks. */
  superseded?: ISupersededDag[];
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

/**
 * What this user may do (PRD §9), served by `GET capabilities`.
 *
 * Advisory only. Authorization is enforced server-side in every mutating
 * handler, which answers 403 regardless of what the client believes — this
 * exists so the UI can present a coherent view-only mode instead of offering
 * actions that are guaranteed to fail.
 */
export interface ICapabilities {
  role: 'editor' | 'viewer';
  can_edit: boolean;
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
  /** Per-task common settings this op supports (the NODE "Common settings"
   * section): retries/retry_delay/depends_on_past + sensor mode/poke_interval/
   * timeout. See `COMMON_PARAM_DEFS` in forms.ts (PRD §6.1.3). */
  commonParams?: string[];
  // INFO-tab documentation fields (data-only; see the operator registry YAML).
  description?: string;
  docsUrl?: string;
  example?: string;
  provider?: string;
  airflowMinVersion?: string;
  /** Off the Airflow constraints file (Great Expectations / OpenMetadata; PRD
   * §6.2.2 ¹ / §13 Q13). Such ops are shown with a pinned-install note but never
   * gate-blocked — `/importErrors` is the deploy verdict. */
  thirdParty?: boolean;
  /** The third-party package's own version pin (used in the install hint). */
  version?: string;
  // Provider-availability against the *target* Airflow (PRD §6.2.1). `unknown`
  // when the target couldn't be read; `third-party` for off-constraints ops —
  // both are still shown and never blocked.
  availability?:
    | 'available'
    | 'missing-provider'
    | 'version-too-old'
    | 'unknown'
    | 'third-party';
  /** `pip install …` hint; present for `missing-provider` or `third-party`. */
  pipInstall?: string;
}

/**
 * A notifier (PRD §6.8): a callback channel (email / Slack / …) attachable to a
 * DAG (or task) lifecycle event in the Notifications tab. Its data shape mirrors
 * `IOperatorDef` (the `import`/`template` stay server-side). Served by
 * `GET notifiers`, annotated with provider-availability like operators.
 */
export interface INotifierDef {
  id: string;
  label: string;
  params: IOperatorParam[];
  description?: string;
  docsUrl?: string;
  example?: string;
  provider?: string;
  airflowMinVersion?: string;
  thirdParty?: boolean;
  version?: string;
  availability?:
    | 'available'
    | 'missing-provider'
    | 'version-too-old'
    | 'unknown'
    | 'third-party';
  pipInstall?: string;
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
  /** Per-deploy trace id, also stamped into the `.py` provenance header and the
   * deploy's audit record — links a deployed DAG (and a later import error) back
   * to the deploy session (PRD §8.9/§10). */
  correlation_id?: string;
  /** This deploy overwrote a prior version, so a rollback target exists (§7). */
  backed_up?: boolean;
  /** The post-write lifecycle (PRD §6.5.4). `reconciled` means the SERVER will
   * wait for registration, retire the renamed-away dag_id, unpause and trigger —
   * so the editor only observes. When it is false (an older/disabled server, or
   * a journal that could not be written) the editor performs those steps itself,
   * exactly as before. */
  lifecycle?: { deploy_id: string; reconciled: boolean; run_id: string };
  warnings: string[];
  errors: string[];
  dagbag: IDagBagResult;
}

// -- Durable deploy lifecycle (PRD §6.5.4) ---------------------------------- //

export type LifecyclePhase =
  | 'awaiting_registration'
  | 'retiring'
  | 'unpausing'
  | 'triggering'
  | 'terminal';

export type LifecycleOutcome =
  | 'completed'
  | 'import_failed'
  | 'expired'
  | 'failed'
  | 'denied'
  | 'superseded'
  | 'cancelled';

export interface ILifecycleStep {
  state: 'pending' | 'done' | 'skipped' | 'failed';
  attempts: number;
  last_error: string | null;
  skipped_reason?: string | null;
}

/** One observation of a deploy the server is completing. Deliberately carries no
 * user, no paths and no IR — it is a read-only, ungated projection. */
export interface IDeployLifecycleRes {
  deploy_id: string;
  dag_id: string;
  filename: string;
  afdag_id: string;
  phase: LifecyclePhase;
  outcome: LifecycleOutcome | null;
  steps: Record<
    'registered' | 'retire' | 'unpause' | 'trigger',
    ILifecycleStep
  >;
  retire_dag_id: string | null;
  run_id: string | null;
  run_state: string | null;
  import_error?: IImportError;
  message: string;
  updated_at: string;
  action_deadline_at: string;
}

// Result of `POST dags/rollback` — restore the previous deployed version (§7).
export interface IRollbackRes {
  dag_id: string;
  rolled_back: boolean;
  filename: string;
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

// Source `.afdag` Contents path for a deployed DAG (PRD §7, "Open in Studio to
// fix"). `path` is null when the source can't be located.
export interface IDagSourceRes {
  path: string | null;
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

// -- Variables (PRD §6.10) --------------------------------------------------

// One variable in the target Airflow, as offered by the "reference an existing
// Airflow variable" picker. `owner` is the Studio flow that created it (null
// when it was made outside Studio) — a variable owned by another flow is
// referenceable but never editable from here.
export interface IAirflowVariable {
  key: string;
  description?: string;
  owner?: string | null;
  /** Already declared by the open flow (so the picker can grey it out). */
  declared?: boolean;
}

// A flow's declaration reconciled against the live Airflow.
export interface IVariableStatus {
  key: string;
  scope: 'local' | 'remote';
  value: string;
  description: string;
  var_type: 'string' | 'json';
  default?: string;
  /** Where the key is referenced ("task 'x'", "DAG params"). Non-empty means
   * removing it would break the flow, so the UI refuses. */
  used_by: string[];
  /** Present in the target Airflow right now. */
  exists: boolean;
  /** Present AND created by this flow (so it is safe to modify/delete). */
  owned: boolean;
  /** Airflow withheld the value (the key looks sensitive). It cannot be read
   * back, and must never be written back — that would overwrite the real
   * secret with three asterisks. */
  redacted?: boolean;
  airflow_value?: string | null;
}

// The VARIABLES tab payload: declarations + what Airflow actually has.
export interface IVariablesInspectRes {
  variables: IVariableStatus[];
  available: IAirflowVariable[];
  /** Keys used by a task but declared nowhere — the blocking error case. */
  undefined: string[];
  /** Declared but referenced nowhere — a hint, never an error. */
  unused: string[];
  /** False when Airflow was unreachable; existence checks are then unknown. */
  airflow_reachable: boolean;
}

export interface IVariablesListRes {
  variables: IAirflowVariable[];
}

export interface IVariableSetRes {
  key: string;
  created: boolean;
}

export interface IVariableDeleteRes {
  key: string;
  deleted: boolean;
}

// -- Connections (PRD §6.11) ------------------------------------------------

// One connection in the target Airflow, as offered by the picker. Only
// non-secret identity fields — never the password or extra.
export interface IAirflowConnection {
  conn_id: string;
  conn_type?: string | null;
  description?: string;
  owner?: string | null;
  declared?: boolean;
}

// A flow's declaration reconciled against the live Airflow.
export interface IConnectionStatus {
  conn_id: string;
  scope: 'local' | 'remote';
  conn_type?: string;
  description?: string;
  host?: string;
  login?: string;
  password?: string;
  schema?: string;
  port?: string;
  extra?: string;
  /** Where the id is referenced ("task 'x' (aws_conn_id)"). Non-empty means
   * removing it would break the flow, so the UI refuses. */
  used_by: string[];
  exists: boolean;
  owned: boolean;
  /** Airflow withheld the password (it masks secrets on read). */
  redacted?: boolean;
  airflow_conn_type?: string | null;
  /** Advisory notes about this connection's `extra` (never blocking). */
  hints?: string[];
}

// A conn_id a task uses that the flow does not declare. A warning, never a
// deploy blocker — flows predating §6.11 reference conn_ids that exist fine.
export interface IUndeclaredConnection {
  conn_id: string;
  where: string[];
  /** Came from the operator's registry default, not typed by the author. */
  implicit: boolean;
  exists_in_airflow: boolean;
}

export interface IConnectionsInspectRes {
  connections: IConnectionStatus[];
  available: IAirflowConnection[];
  undeclared: IUndeclaredConnection[];
  unused: string[];
  airflow_reachable: boolean;
  /** Curated `extra` metadata for the declared connection types (PRD §6.13). */
  type_hints?: Record<string, IConnTypeHints>;
}

export interface IConnectionsListRes {
  connections: IAirflowConnection[];
}

export interface IConnectionSetRes {
  conn_id: string;
  created: boolean;
}

export interface IConnectionDeleteRes {
  conn_id: string;
  deleted: boolean;
}

// -- Connection-type extra hints (PRD §6.13) --------------------------------

// One `extra` key a known connection type understands. Curated server-side:
// Airflow's REST API does not expose the per-provider connection form metadata,
// so without this an `extra` is an opaque JSON box whose keys fail silently.
export interface IConnExtraField {
  key: string;
  type: 'boolean' | 'integer' | 'string';
  label: string;
  help: string;
  default?: unknown;
  /** Allowed values; anything else is rejected by the provider at run time. */
  enum?: string[];
}

// A one-click starting point for a common setup of this connection type.
export interface IConnPreset {
  id: string;
  label: string;
  help: string;
  port?: number;
  extra: Record<string, unknown>;
}

export interface IConnTypeHints {
  conn_type: string;
  label: string;
  intro: string;
  fields: IConnExtraField[];
  presets: IConnPreset[];
}
