// Wire shapes for /admin/api/v1, mirroring dev/admin-ui-spec.md Part 4 exactly.

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue }

// Error body for all non-2xx responses; `rejection` only on 422 enqueue rejections.
export interface ApiErrorBody {
  error: string
  code: string
  rejection?: EnqueueRejection
}

// Stats frame: SSE `stats` event payload, also `frame` in /overview and the hello event.
export interface QueueTotals {
  enqueued: number
  reserved: number
  acked: number
  nacked: number
  dead_lettered: number
}

export interface QueueRates {
  enqueued: number
  acked: number
  nacked: number
  dead_lettered: number
}

export interface QueueFrame {
  ready: number
  scheduled: number
  inflight: number
  dead_lettered: number
  totals: QueueTotals
  rates: QueueRates
}

export interface StatsFrame {
  seq: number
  ts_ms: number
  server: { command_queue_len: number }
  queues: Record<string, QueueFrame>
}

export interface RateSample {
  ts_ms: number
  enqueued: number
  acked: number
  nacked: number
  dead_lettered: number
}

export type RateHistory = Record<string, RateSample[]>

// SSE `hello` event
export interface HelloEvent {
  history: RateHistory
  frame: StatsFrame
}

// GET /overview
export interface OverviewServer {
  version: string
  started_at_ms: number
  now_ms: number
  strict_queues: boolean
  dead_letter_retention_ms: number
  command_queue_len: number
}

export interface Overview {
  server: OverviewServer
  frame: StatsFrame
  history: RateHistory
}

// GET /queues, GET /queues/{name}
export interface QueueDepths {
  ready: number
  scheduled: number
  inflight: number
  dead_lettered: number
}

export interface QueueConfig {
  name: string
  max_lease_duration_ms: number | null
  default_max_attempts: number | null
  max_attempts_ceiling: number | null
  default_priority: number | null
  max_payload_bytes: number | null
  allowed_encodings: string[] | null
  allowed_job_types: string[] | null
  max_schedule_horizon_ms: number | null
  max_custom_entries: number | null
  max_custom_total_bytes: number | null
  max_custom_key_bytes: number | null
  dedup_window_ms: number | null
  max_queue_depth: number | null
}

export interface EffectiveLimits {
  max_lease_duration_ms: number
  default_max_attempts: number
  max_attempts_ceiling: number
  default_priority: number
  max_payload_bytes: number
  allowed_encodings: string[] | null
  allowed_job_types: string[] | null
  max_schedule_horizon_ms: number
  max_custom_entries: number
  max_custom_total_bytes: number
  max_custom_key_bytes: number
  dedup_window_ms: number
  max_queue_depth: number | null
}

export interface QueueInfo {
  name: string
  declared: boolean
  depths: QueueDepths
  overrides: QueueConfig | null
  effective: EffectiveLimits
}

// PUT /queues/{name}
export interface QueueOverridesPatch {
  max_lease_duration_ms?: number | null
  default_max_attempts?: number | null
  max_attempts_ceiling?: number | null
  default_priority?: number | null
  max_payload_bytes?: number | null
  allowed_encodings?: string[] | null
  allowed_job_types?: string[] | null
  max_schedule_horizon_ms?: number | null
  max_custom_entries?: number | null
  max_custom_total_bytes?: number | null
  max_custom_key_bytes?: number | null
  dedup_window_ms?: number | null
  max_queue_depth?: number | null
}

export interface QueueUpdateRequest {
  etag: string
  overrides: QueueOverridesPatch
}

// PUT /queues/{name} and PUT /config response
export interface ConfigWriteResult {
  applied: boolean
  requires_restart: string[]
  etag: string
}

// DELETE /queues/{name}
export interface QueueDeleteResponse {
  purged: number
  etag: string
}

// GET /queues/{name}/jobs
export type JobState = 'ready' | 'scheduled' | 'inflight' | 'dead_letter'

export interface JobPayload {
  encoding: string
  size_bytes: number
  data_b64?: string
}

export interface JobSummary {
  id: string
  key_b64: string
  job_type: string
  priority: number
  attempt: number
  max_attempts: number
  enqueued_at_ms: number
  scheduled_at_ms?: number
  lease_expires_at_ms?: number
  failed_at_ms?: number
  cause?: string
  last_reason?: string
  custom: Record<string, string>
  payload: JobPayload
}

export interface JobsPage {
  jobs: JobSummary[]
  next_cursor: string | null
  truncated: boolean
}

// GET /jobs/{id}, GET /queues/{name}/dead-letters/{key_b64}
export interface JobDetail extends JobSummary {
  payload: JobPayload & { data_b64: string }
}

// POST /queues/{name}/jobs
export interface EnqueuePayload {
  encoding: string
  data_b64?: string
  data_text?: string
}

export interface EnqueueJobRequest {
  job_type: string
  payload?: EnqueuePayload
  priority?: number
  max_attempts?: number
  scheduled_at_ms?: number
  idempotency_key?: string
  custom?: Record<string, string>
}

export interface EnqueueJobResponse {
  job_id: string
}

export interface EnqueueRejection {
  reason: string
  detail: string
}

// POST /queues/{name}/jobs:dead-letter
export interface DeadLetterJobsResult {
  dead_lettered: number
  missing: number
}

// POST /queues/{name}/dead-letters:requeue and :delete
export interface DeadLetterKeysRequest {
  keys_b64: string[]
}

export interface RequeueResult {
  requeued: number
  missing: number
}

export interface DeleteResult {
  deleted: number
  missing: number
}

// GET/POST/DELETE /session
export type Role = 'viewer' | 'operator' | 'admin'

export interface SessionInfo {
  name: string | null
  role: Role | null
  auth_enabled: boolean
}

export interface SessionLoginResponse {
  name: string
  role: Role
  expires_at_ms: number
}

// GET /config: running Config as JSON with secrets redacted.
export type PersistMode = 'sync_all' | 'sync_data' | 'buffer'

export type LogFormat = 'text' | 'json'

export interface ServerConfigView {
  listen_addr: string
  db_path: string
  tls_cert_path: string | null
  tls_key_path: string | null
  strict_queues: boolean
}

// Redacted: the server never serves key material, only the count.
export interface AuthConfigView {
  api_keys: { count: number } | null
}

export interface LimitsConfigView {
  max_lease_duration_ms: number
  default_max_attempts: number
  max_attempts_ceiling: number
  default_priority: number
  max_reserve_batch: number
  max_reserve_queues: number
  max_wait_timeout_ms: number
  max_enqueue_batch: number
  max_queue_depth: number | null
  max_payload_bytes: number
  max_message_bytes: number
  max_custom_entries: number
  max_custom_total_bytes: number
  max_custom_key_bytes: number
  max_queue_name_bytes: number
  max_job_type_bytes: number
  max_idempotency_key_bytes: number
  max_schedule_horizon_ms: number
  allowed_encodings: string[] | null
}

export interface StorageConfigView {
  persist_mode: PersistMode
  sweep_interval_ms: number
  sweep_limit: number
  dedup_window_ms: number
  dead_letter_retention_ms: number
  command_queue_capacity: number
  cache_size_bytes: number | null
  max_journaling_size_bytes: number | null
  max_cached_files: number | null
  worker_threads: number | null
}

export interface LoggingConfigView {
  level: string
  format: LogFormat
}

export interface TracingConfigView {
  enabled: boolean
  otlp_endpoint: string
  service_name: string
  sample_ratio: number
}

export interface MetricsConfigView {
  enabled: boolean
  otlp_endpoint: string
  export_interval_ms: number
  prometheus_enabled: boolean
  prometheus_listen_addr: string
}

export interface AdminConfigView {
  enabled: boolean
  listen_addr: string
  tls_cert_path: string | null
  tls_key_path: string | null
  // Redacted: names and roles only.
  keys: { name: string; role: Role }[] | null
  session_ttl_ms: number
}

export interface EffectiveConfig {
  server: ServerConfigView
  auth: AuthConfigView
  limits: LimitsConfigView
  storage: StorageConfigView
  logging: LoggingConfigView
  tracing: TracingConfigView
  metrics: MetricsConfigView
  admin: AdminConfigView
  queues: QueueConfig[]
}

export interface ConfigResponse {
  effective: EffectiveConfig
  etag: string
  env_pinned: string[]
  restart_only: string[]
  // Restart-only paths whose on-disk value differs from the running (boot)
  // value; they take effect on the next restart.
  pending_restart: string[]
}

// PUT /config
export interface ConfigChange {
  path: string
  value: JsonValue | null
}

export interface ConfigUpdateRequest {
  etag: string
  changes: ConfigChange[]
}

// GET /server-info
export interface ServerInfo {
  version: string
  started_at_ms: number
  listen_addr: string
  tls: boolean
  auth_enforcing: boolean
  strict_queues: boolean
  db_path: string
}
