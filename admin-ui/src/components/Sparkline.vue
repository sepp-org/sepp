<script setup lang="ts">
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import type { RateSample } from '../api/types'

const props = defineProps<{ samples: RateSample[] }>()

const host = ref<HTMLDivElement | null>(null)
let chart: uPlot | null = null

function toData(samples: RateSample[]): uPlot.AlignedData {
  return [
    samples.map((s) => s.ts_ms / 1000),
    samples.map((s) => s.enqueued),
    samples.map((s) => s.acked),
  ]
}

function series(stroke: string): uPlot.Series {
  return { stroke, width: 1.5, points: { show: false } }
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
      width: 120,
      height: 36,
      cursor: { show: false },
      legend: { show: false },
      scales: {
        x: { time: false },
        y: { range: (_u, _min, max) => [0, Math.max(max, 1)] },
      },
      axes: [{ show: false }, { show: false }],
      series: [{}, series('#ec6a2e'), series('#f5854d')],
    },
    data,
    host.value,
  )
}

onMounted(render)
watch(() => props.samples, render, { deep: true })

onBeforeUnmount(() => {
  chart?.destroy()
  chart = null
})
</script>

<template>
  <div ref="host" class="h-9 w-30 shrink-0"></div>
</template>
