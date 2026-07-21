import type {
  ApiErrorBody,
  AuditPage,
  ConfigResponse,
  ConfigUpdateRequest,
  ConfigWriteResult,
  DeadLetterJobsResult,
  DeleteResult,
  EnqueueJobRequest,
  EnqueueJobResponse,
  EnqueueRejection,
  JobDetail,
  JobLookup,
  JobState,
  JobsPage,
  Overview,
  QueueDeleteResponse,
  QueueInfo,
  QueueUpdateRequest,
  RequeueResult,
  ServerInfo,
  SessionInfo,
  SessionLoginResponse,
} from './types'

export const API_BASE = '/admin/api/v1'

export class AdminApiError extends Error {
  status: number
  code: string
  rejection?: EnqueueRejection

  constructor(status: number, body: ApiErrorBody) {
    super(body.error)
    this.name = 'AdminApiError'
    this.status = status
    this.code = body.code
    this.rejection = body.rejection
  }
}

async function parseError(res: Response): Promise<ApiErrorBody> {
  try {
    return (await res.json()) as ApiErrorBody
  } catch {
    return { error: `${res.status} ${res.statusText}`, code: `http_${res.status}` }
  }
}

// Fires on any 401 outside the session endpoints, so an expired or rotated-out
// session lands on the login screen instead of surfacing per-view errors.
let onUnauthorized: (() => void) | null = null

export function setUnauthorizedHandler(handler: () => void) {
  onUnauthorized = handler
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  headers?: Record<string, string>,
): Promise<T> {
  const init: RequestInit = { method, headers: { ...headers } }
  if (body !== undefined) {
    init.headers = { 'Content-Type': 'application/json', ...headers }
    init.body = JSON.stringify(body)
  }
  const res = await fetch(API_BASE + path, init)
  if (!res.ok) {
    if (res.status === 401 && !path.startsWith('/session')) onUnauthorized?.()
    throw new AdminApiError(res.status, await parseError(res))
  }
  if (res.status === 204) return undefined as unknown as T
  return (await res.json()) as T
}

function seg(part: string): string {
  return encodeURIComponent(part)
}

export const api = {
  overview: (history = true) =>
    request<Overview>('GET', history ? '/overview' : '/overview?history=false'),
  serverInfo: () => request<ServerInfo>('GET', '/server-info'),

  queues: () => request<QueueInfo[]>('GET', '/queues'),
  queue: (name: string) => request<QueueInfo>('GET', `/queues/${seg(name)}`),
  updateQueue: (name: string, body: QueueUpdateRequest) =>
    request<ConfigWriteResult>('PUT', `/queues/${seg(name)}`, body),
  deleteQueue: (name: string, etag: string, purge: boolean) =>
    request<QueueDeleteResponse>(
      'DELETE',
      `/queues/${seg(name)}${purge ? '?purge=true' : ''}`,
      undefined,
      { 'If-Match': etag },
    ),

  jobs: (name: string, state: JobState, cursor?: string, limit?: number) => {
    const params = new URLSearchParams({ state })
    if (cursor) params.set('cursor', cursor)
    if (limit !== undefined) params.set('limit', String(limit))
    return request<JobsPage>('GET', `/queues/${seg(name)}/jobs?${params}`)
  },
  job: (id: string) => request<JobLookup>('GET', `/jobs/${seg(id)}`),
  deadLetter: (name: string, keyB64: string) =>
    request<JobDetail>('GET', `/queues/${seg(name)}/dead-letters/${seg(keyB64)}`),
  enqueue: (name: string, body: EnqueueJobRequest) =>
    request<EnqueueJobResponse>('POST', `/queues/${seg(name)}/jobs`, body),
  deadLetterJobs: (
    name: string,
    state: 'ready' | 'scheduled',
    keysB64: string[],
    reason?: string,
  ) =>
    request<DeadLetterJobsResult>('POST', `/queues/${seg(name)}/jobs:dead-letter`, {
      state,
      keys_b64: keysB64,
      reason,
    }),
  requeueDeadLetters: (name: string, keysB64: string[], reason?: string) =>
    request<RequeueResult>('POST', `/queues/${seg(name)}/dead-letters:requeue`, {
      keys_b64: keysB64,
      reason,
    }),
  deleteDeadLetters: (name: string, keysB64: string[], reason?: string) =>
    request<DeleteResult>('POST', `/queues/${seg(name)}/dead-letters:delete`, {
      keys_b64: keysB64,
      reason,
    }),

  audit: (opts: { before?: number; limit?: number; actor?: string; actionPrefix?: string } = {}) => {
    const params = new URLSearchParams()
    if (opts.before !== undefined) params.set('before', String(opts.before))
    if (opts.limit !== undefined) params.set('limit', String(opts.limit))
    if (opts.actor) params.set('actor', opts.actor)
    if (opts.actionPrefix) params.set('action_prefix', opts.actionPrefix)
    const qs = params.toString()
    return request<AuditPage>('GET', qs ? `/audit?${qs}` : '/audit')
  },

  config: () => request<ConfigResponse>('GET', '/config'),
  updateConfig: (body: ConfigUpdateRequest) => request<ConfigWriteResult>('PUT', '/config', body),
  addAuthKey: (body: { etag: string; name: string; key: string }) =>
    request<ConfigWriteResult>('POST', '/auth/keys', body),
  deleteAuthKey: (name: string, etag: string) =>
    request<ConfigWriteResult>('DELETE', `/auth/keys/${seg(name)}`, undefined, {
      'If-Match': etag,
    }),
  disableAuth: (etag: string) =>
    request<ConfigWriteResult>('DELETE', '/auth/keys', undefined, { 'If-Match': etag }),

  session: () => request<SessionInfo>('GET', '/session'),
  login: (name: string, key: string) =>
    request<SessionLoginResponse>('POST', '/session', { name, key }),
  logout: () => request<void>('DELETE', '/session'),
}
