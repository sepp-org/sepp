<script setup lang="ts">
import type { QueueFrame, RateSample } from '../api/types'
import Sparkline from './Sparkline.vue'
import StatBadge from './StatBadge.vue'

defineProps<{
  name: string
  frame: QueueFrame
  samples: RateSample[]
  declared: boolean
}>()

function rate(n: number): string {
  return n >= 100 ? n.toFixed(0) : n.toFixed(1)
}
</script>

<template>
  <div
    class="flex cursor-pointer items-center gap-6 rounded-lg border border-ink-800 bg-ink-900 px-4 py-3 transition-colors hover:border-ink-600"
  >
    <div class="min-w-0 flex-1">
      <div class="flex items-center gap-2">
        <span class="truncate font-medium">{{ name }}</span>
        <span
          v-if="declared"
          class="rounded-full bg-ink-800 px-2 py-0.5 text-[10px] tracking-wider text-ink-300 uppercase"
        >
          declared
        </span>
      </div>
      <div class="mt-1 text-xs text-ink-400 tabular-nums">
        {{ rate(frame.rates.enqueued) }}/s enqueued · {{ rate(frame.rates.acked) }}/s acked
      </div>
    </div>
    <div class="flex gap-5">
      <StatBadge label="ready" :value="frame.ready" />
      <StatBadge label="scheduled" :value="frame.scheduled" />
      <StatBadge label="in-flight" :value="frame.inflight" />
      <StatBadge label="dead letters" :value="frame.dead_lettered" alert />
    </div>
    <Sparkline :samples="samples" />
  </div>
</template>
