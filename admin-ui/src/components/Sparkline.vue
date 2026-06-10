<script setup lang="ts">
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import type { RateSample } from '../api/types'
import { RATE_METRICS } from '../lib/metrics'

const props = withDefaults(defineProps<{ samples: RateSample[]; height?: number }>(), {
  height: 64,
})

const host = ref<HTMLDivElement | null>(null)
let chart: uPlot | null = null
let resize: ResizeObserver | null = null

function toData(samples: RateSample[]): uPlot.AlignedData {
  return [
    samples.map((s) => s.ts_ms / 1000),
    ...RATE_METRICS.map((m) => samples.map((s) => s[m.key])),
  ]
}

function hostWidth(): number {
  return Math.max(host.value?.clientWidth ?? 0, 40)
}

function render() {
  if (!host.value) return
  if (props.samples.length === 0) {
    chart?.destroy()
    chart = null
    return
  }
  const data = toData(props.samples)
  if (chart) {
    chart.setData(data)
    return
  }
  chart = new uPlot(
    {
      width: hostWidth(),
      height: props.height,
      cursor: { show: false },
      legend: { show: false },
      scales: {
        x: { time: false },
        y: { range: (_u, _min, max) => [0, Math.max(max, 1)] },
      },
      axes: [{ show: false }, { show: false }],
      series: [
        {},
        ...RATE_METRICS.map((m) => ({
          stroke: m.stroke,
          fill: m.fill,
          width: 1.5,
          points: { show: false },
        })),
      ],
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
})

watch(() => props.samples, render, { deep: true })

onBeforeUnmount(() => {
  resize?.disconnect()
  resize = null
  chart?.destroy()
  chart = null
})
</script>

<template>
  <!-- min-w floor makes a crowded flex-wrap row push the chart onto its own
       full-width line instead of crushing it to zero. -->
  <div ref="host" class="min-w-48 flex-1" :style="{ height: `${height}px` }"></div>
</template>
