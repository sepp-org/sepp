import { reactive, ref, watch } from 'vue'

export interface SparkRange {
  key: string
  ms: number
}

// 6h matches the admin.stats_history_ms ceiling; the default retention is 1h,
// so longer picks simply show what the server kept.
export const SPARK_RANGES: SparkRange[] = [
  { key: '1m', ms: 60_000 },
  { key: '5m', ms: 300_000 },
  { key: '15m', ms: 900_000 },
  { key: '1h', ms: 3_600_000 },
  { key: '6h', ms: 21_600_000 },
]

const STORAGE_KEY = 'sepp-spark-range'

const stored = localStorage.getItem(STORAGE_KEY)
const globalRange = ref(SPARK_RANGES.some((r) => r.key === stored) ? (stored as string) : '5m')
watch(globalRange, (key) => localStorage.setItem(STORAGE_KEY, key))

// Per-queue overrides are a drill-down, deliberately not persisted.
const overrides = reactive<Record<string, string>>({})

export function rangeMs(key: string): number {
  return SPARK_RANGES.find((r) => r.key === key)?.ms ?? 300_000
}

export function useSparkRange() {
  function effectiveKey(queue: string): string {
    return overrides[queue] ?? globalRange.value
  }
  return { globalRange, overrides, effectiveKey }
}
