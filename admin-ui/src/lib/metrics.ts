import type { QueueDepths, QueueRates } from '../api/types'

// Single source for the rate metric order and colors, so the big per-second
// numbers double as the sparkline legend.
export interface RateMetric {
  key: keyof QueueRates
  label: string
  stroke: string
  fill?: string
}

// Steps validated for the ink-900 surface (colorblind separation, >= 3:1
// contrast, dark lightness band) in this order; keep order and hues together.
export const RATE_METRICS: RateMetric[] = [
  { key: 'enqueued', label: 'enqueued', stroke: '#d95926', fill: 'rgba(217, 89, 38, 0.10)' },
  { key: 'acked', label: 'acked', stroke: '#199e70', fill: 'rgba(25, 158, 112, 0.10)' },
  { key: 'nacked', label: 'nacked', stroke: '#c98500' },
  { key: 'dead_lettered', label: 'dead-lettered', stroke: '#d03b3b' },
]

export function formatRate(n: number): string {
  if (n >= 10_000) return `${(n / 1000).toFixed(1)}k`
  if (n >= 100) return n.toFixed(0)
  return n.toFixed(1)
}

// Depth (queue state) counters, in display order. `alert` renders red when
// the count is non-zero.
export interface DepthMetric {
  key: keyof QueueDepths
  label: string
  alert?: boolean
}

export const DEPTH_METRICS: DepthMetric[] = [
  { key: 'ready', label: 'ready' },
  { key: 'scheduled', label: 'scheduled' },
  { key: 'inflight', label: 'in-flight' },
  { key: 'dead_lettered', label: 'dead letters', alert: true },
]
