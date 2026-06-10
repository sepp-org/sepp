import { useQueryClient, type QueryClient } from '@tanstack/vue-query'
import { reactive, ref } from 'vue'
import { API_BASE, api } from '../api/client'
import type { HelloEvent, OverviewServer, RateHistory, RateSample, StatsFrame } from '../api/types'

export type StreamStatus = 'live' | 'polling' | 'connecting'

const HISTORY_CAP = 60
const STALE_MS = 5_000
const POLL_INTERVAL_MS = 5_000
const SSE_RETRY_MS = 30_000

const status = ref<StreamStatus>('connecting')
const frame = ref<StatsFrame | null>(null)
const history = reactive<RateHistory>({})
const server = ref<OverviewServer | null>(null)

let queryClient: QueryClient | null = null
let es: EventSource | null = null
let lastFrameAt = 0
let started = false
let watchdogTimer: number | undefined
let pollTimer: number | undefined
let retryTimer: number | undefined

function appendSample(name: string, sample: RateSample) {
  const ring = history[name] ?? (history[name] = [])
  if (ring.length > 0 && ring[ring.length - 1].ts_ms === sample.ts_ms) return
  ring.push(sample)
  if (ring.length > HISTORY_CAP) ring.splice(0, ring.length - HISTORY_CAP)
}

function applyFrame(f: StatsFrame) {
  frame.value = f
  lastFrameAt = Date.now()
  // Frames carry the authoritative queue set; drop history for queues that
  // vanished (deleted/evicted) so a recreated queue starts a fresh sparkline.
  for (const name of Object.keys(history)) {
    if (!(name in f.queues)) delete history[name]
  }
  for (const [name, q] of Object.entries(f.queues)) {
    appendSample(name, {
      ts_ms: f.ts_ms,
      enqueued: q.rates.enqueued,
      acked: q.rates.acked,
      nacked: q.rates.nacked,
      dead_lettered: q.rates.dead_lettered,
    })
  }
}

function replaceHistory(h: RateHistory) {
  for (const name of Object.keys(history)) delete history[name]
  for (const [name, samples] of Object.entries(h)) history[name] = samples.slice(-HISTORY_CAP)
}

function invalidateConfigQueries() {
  queryClient?.invalidateQueries({ queryKey: ['config'] })
  queryClient?.invalidateQueries({ queryKey: ['queues'] })
}

function closeSse() {
  es?.close()
  es = null
}

function clearTimers() {
  if (pollTimer !== undefined) {
    clearInterval(pollTimer)
    pollTimer = undefined
  }
  if (retryTimer !== undefined) {
    clearInterval(retryTimer)
    retryTimer = undefined
  }
}

function connectSse() {
  closeSse()
  lastFrameAt = Date.now()
  es = new EventSource(`${API_BASE}/events`)
  es.addEventListener('hello', (ev) => {
    const hello = JSON.parse((ev as MessageEvent).data) as HelloEvent
    replaceHistory(hello.history)
    frame.value = hello.frame
    lastFrameAt = Date.now()
    status.value = 'live'
    clearTimers()
  })
  es.addEventListener('stats', (ev) => {
    applyFrame(JSON.parse((ev as MessageEvent).data) as StatsFrame)
    status.value = 'live'
  })
  es.addEventListener('config', () => invalidateConfigQueries())
}

async function pollOnce() {
  try {
    const o = await api.overview()
    server.value = o.server
    frame.value = o.frame
    replaceHistory(o.history)
  } catch {
    // Transient fetch failure; the poll timer or SSE retry recovers.
  }
}

function enterPolling() {
  closeSse()
  status.value = 'polling'
  void pollOnce()
  pollTimer = window.setInterval(() => void pollOnce(), POLL_INTERVAL_MS)
  retryTimer = window.setInterval(connectSse, SSE_RETRY_MS)
}

function checkStale() {
  if (status.value !== 'polling' && Date.now() - lastFrameAt > STALE_MS) enterPolling()
}

function start() {
  if (started) return
  started = true
  status.value = 'connecting'
  connectSse()
  // Hello frames carry no server info; one overview fetch seeds it for the header.
  void pollOnce()
  watchdogTimer = window.setInterval(checkStale, 1_000)
}

function stop() {
  closeSse()
  clearTimers()
  if (watchdogTimer !== undefined) {
    clearInterval(watchdogTimer)
    watchdogTimer = undefined
  }
  started = false
  status.value = 'connecting'
}

export function useStatsStream() {
  if (!queryClient) {
    // Capture once in a setup context; later start() calls run outside setup.
    try {
      queryClient = useQueryClient()
    } catch {
      queryClient = null
    }
  }
  return { status, frame, history, server, start, stop }
}
