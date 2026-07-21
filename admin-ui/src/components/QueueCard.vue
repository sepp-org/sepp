<script setup lang="ts">
import { computed } from 'vue'
import type { QueueFrame, RateSample } from '../api/types'
import { rangeMs, useSparkRange } from '../composables/useSparkRange'
import { DEPTH_METRICS, RATE_METRICS, formatRate } from '../lib/metrics'
import RangePicker from './RangePicker.vue'
import Sparkline from './Sparkline.vue'
import StatBadge from './StatBadge.vue'

const props = defineProps<{
  name: string
  frame: QueueFrame
  samples: RateSample[]
  declared: boolean
}>()

const { overrides, effectiveKey } = useSparkRange()

const override = computed<string | null>({
  get: () => overrides[props.name] ?? null,
  set: (v) => {
    if (v === null) delete overrides[props.name]
    else overrides[props.name] = v
  },
})

const windowMs = computed(() => rangeMs(effectiveKey(props.name)))
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
      <!-- min-w floor makes a crowded flex-wrap row push the chart onto its
           own full-width line instead of crushing it to zero. -->
      <div class="flex min-w-48 flex-1 flex-col gap-1.5 self-end">
        <RangePicker v-model="override" auto class="self-end" />
        <Sparkline :samples="samples" :range-ms="windowMs" :height="64" />
      </div>
    </div>
  </div>
</template>
