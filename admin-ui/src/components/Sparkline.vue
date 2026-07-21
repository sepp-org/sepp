<script setup lang="ts">
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import type { RateSample } from '../api/types'
import { RATE_METRICS, formatRate } from '../lib/metrics'

const props = withDefaults(
  defineProps<{ samples: RateSample[]; rangeMs: number; height?: number }>(),
  { height: 64 },
)

// Past this much silence the window keeps sliding on wall clock and a gap
// opens at the right edge, instead of the chart silently freezing. Above the
// 5s polling cadence so a degraded-but-working connection stays smooth.
const STALL_MS = 6_500

const host = ref<HTMLDivElement | null>(null)
let chart: uPlot | null = null
let resize: ResizeObserver | null = null
let ticker: number | undefined
let lastArrivalWall = Date.now()
// End of the plotted window in chart-x seconds; the newest sample, pushed
// further right by silence once the stream stalls.
let windowEndSec = 0

interface Tip {
  left: number
  time: string
  values: number[]
}
const tip = ref<Tip | null>(null)
const flip = ref(false)

function windowed(cutoffMs: number): RateSample[] {
  const all = props.samples
  let lo = 0
  let hi = all.length - 1
  while (lo < hi) {
    const mid = (lo + hi) >> 1
    if (all[mid].ts_ms < cutoffMs) lo = mid + 1
    else hi = mid
  }
  return all[lo].ts_ms < cutoffMs ? [] : all.slice(lo)
}

function toData(samples: RateSample[]): uPlot.AlignedData {
  return [
    samples.map((s) => s.ts_ms / 1000),
    ...RATE_METRICS.map((m) => samples.map((s) => s[m.key])),
  ]
}

function hostWidth(): number {
  return Math.max(host.value?.clientWidth ?? 0, 40)
}

function onCursor(u: uPlot) {
  const { idx, left } = u.cursor
  if (idx == null || left == null || left < 0) {
    tip.value = null
    return
  }
  const ts = u.data[0][idx]
  // The cursor snaps to the nearest sample no matter how far; don't tooltip
  // from across the empty part of an underfilled window.
  if (Math.abs(left - u.valToPos(ts, 'x')) > 40) {
    tip.value = null
    return
  }
  flip.value = left > u.width / 2
  tip.value = {
    left,
    time: new Date(ts * 1000).toTimeString().slice(0, 8),
    values: RATE_METRICS.map((_m, i) => u.data[i + 1][idx] ?? 0),
  }
}

function teardown() {
  chart?.destroy()
  chart = null
  tip.value = null
}

function render() {
  if (!host.value) return
  const all = props.samples
  if (all.length === 0) {
    teardown()
    return
  }
  const silentMs = Date.now() - lastArrivalWall
  const endMs = all[all.length - 1].ts_ms + (silentMs > STALL_MS ? silentMs : 0)
  windowEndSec = endMs / 1000
  const samples = windowed(endMs - props.rangeMs)
  if (samples.length === 0) {
    teardown()
    return
  }
  const data = toData(samples)
  if (chart) {
    chart.setData(data)
    return
  }
  chart = new uPlot(
    {
      width: hostWidth(),
      height: props.height,
      cursor: { y: false, points: { size: 5 } },
      legend: { show: false },
      scales: {
        x: {
          time: false,
          // Pin the window to the picked range: a shorter history hugs the
          // right edge instead of stretching to fill the width.
          range: () => [windowEndSec - props.rangeMs / 1000, windowEndSec],
        },
        y: { range: (_u, _min, max) => [0, Math.max(max, 1)] },
      },
      axes: [{ show: false }, { show: false }],
      series: [
        {},
        ...RATE_METRICS.map((m) => ({
          stroke: m.stroke,
          fill: m.fill,
          width: 1.5,
          // A lone sample draws no path; give it a dot so a brand-new queue
          // isn't an empty chart.
          points: { show: (u: uPlot) => u.data[0].length === 1, size: 4 },
        })),
      ],
      hooks: { setCursor: [onCursor] },
    },
    data,
    host.value,
  )
}

onMounted(() => {
  render()
  resize = new ResizeObserver(() => {
    if (chart) chart.setSize({ width: hostWidth(), height: props.height })
  })
  if (host.value) resize.observe(host.value)
  ticker = window.setInterval(() => {
    if (Date.now() - lastArrivalWall > STALL_MS) render()
  }, 1_000)
})

// Length + newest timestamp instead of a deep watch: rings run to 21,600
// samples and a deep watch re-traverses every one each second.
watch(
  () => [props.samples.length, props.samples[props.samples.length - 1]?.ts_ms ?? 0] as const,
  ([, ts], [, prevTs]) => {
    if (ts !== prevTs) lastArrivalWall = Date.now()
    render()
  },
)
watch(() => props.rangeMs, render)

onBeforeUnmount(() => {
  resize?.disconnect()
  resize = null
  if (ticker !== undefined) {
    clearInterval(ticker)
    ticker = undefined
  }
  teardown()
})
</script>

<template>
  <div class="relative w-full" :style="{ height: `${height}px` }">
    <div ref="host" class="absolute inset-0"></div>
    <div
      v-if="tip"
      class="pointer-events-none absolute -top-1 z-10 min-w-44 rounded border border-ink-700 bg-ink-950/95 px-2.5 py-1.5"
      :style="{
        left: `${tip.left}px`,
        transform: flip ? 'translate(calc(-100% - 10px), -100%)' : 'translate(10px, -100%)',
      }"
    >
      <div class="text-[10px] text-ink-400 tabular-nums">{{ tip.time }}</div>
      <div
        v-for="(m, i) in RATE_METRICS"
        :key="m.key"
        class="flex items-center gap-1.5 text-[11px]"
      >
        <span class="h-0.5 w-3 rounded-full" :style="{ background: m.stroke }" />
        <span class="whitespace-nowrap text-ink-400">{{ m.label }}</span>
        <span class="ml-auto pl-3 text-ink-100 tabular-nums">{{ formatRate(tip.values[i]) }}/s</span>
      </div>
    </div>
  </div>
</template>
