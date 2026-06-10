<script setup lang="ts">
import type { QueueFrame, RateSample } from '../api/types'
import { DEPTH_METRICS, RATE_METRICS, formatRate } from '../lib/metrics'
import Sparkline from './Sparkline.vue'
import StatBadge from './StatBadge.vue'

defineProps<{
  name: string
  frame: QueueFrame
  samples: RateSample[]
  declared: boolean
}>()
</script>

<template>
  <div
    class="cursor-pointer rounded-lg border border-ink-800 bg-ink-900 px-5 py-4 transition-colors hover:border-ink-600"
  >
    <div class="flex items-center gap-2">
      <span class="truncate text-base font-medium">{{ name }}</span>
      <span
        v-if="declared"
        class="rounded-full bg-ink-800 px-2 py-0.5 text-[10px] tracking-wider text-ink-300 uppercase"
      >
        declared
      </span>
    </div>
    <div class="mt-4 flex flex-wrap items-start gap-x-8 gap-y-4">
      <StatBadge
        v-for="d in DEPTH_METRICS"
        :key="d.key"
        :label="d.label"
        :value="frame[d.key]"
        :alert="d.alert"
      />
      <div class="w-px self-stretch bg-ink-800" />
      <div v-for="m in RATE_METRICS" :key="m.key" class="flex min-w-20 flex-col">
        <span class="text-2xl leading-7 font-semibold tabular-nums" :style="{ color: m.stroke }">
          {{ formatRate(frame.rates[m.key]) }}<span class="text-sm font-normal text-ink-500"
            >/s</span
          >
        </span>
        <span
          class="mt-1 flex items-center gap-1.5 text-[10px] tracking-wider text-ink-400 uppercase"
        >
          <span class="size-1.5 rounded-full" :style="{ background: m.stroke }" />
          {{ m.label }}
        </span>
        <span class="text-xs text-ink-500 tabular-nums">
          {{ frame.totals[m.key].toLocaleString() }} total
        </span>
      </div>
      <Sparkline class="self-end" :samples="samples" :height="72" />
    </div>
  </div>
</template>
