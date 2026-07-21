<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api/client'
import NewQueueDialog from '../components/NewQueueDialog.vue'
import QueueCard from '../components/QueueCard.vue'
import RangePicker from '../components/RangePicker.vue'
import StrictModeToggle from '../components/StrictModeToggle.vue'
import { useSession } from '../composables/useSession'
import { useSparkRange } from '../composables/useSparkRange'
import { useStatsStream } from '../composables/useStatsStream'
import { DEPTH_METRICS, RATE_METRICS, formatRate } from '../lib/metrics'
import QueueDrawer from './queue/QueueDrawer.vue'

const route = useRoute()
const router = useRouter()
const { frame, history } = useStatsStream()
const { globalRange } = useSparkRange()
// RangePicker's model is nullable for the per-card auto option; the global
// picker never emits null.
const globalPick = computed<string | null>({
  get: () => globalRange.value,
  set: (v) => {
    if (v !== null) globalRange.value = v
  },
})
// Queue creation writes the config file, like the strict-mode toggle: admin.
const { canAdmin } = useSession()

const { data: queueInfos } = useQuery({ queryKey: ['queues'], queryFn: () => api.queues() })

const declared = computed(() => {
  const set = new Set<string>()
  for (const q of queueInfos.value ?? []) if (q.declared) set.add(q.name)
  return set
})

const queues = computed(() => {
  const f = frame.value
  if (!f) return []
  return Object.keys(f.queues)
    .sort()
    .map((name) => ({ name, queue: f.queues[name] }))
})

// Server-wide rates and depths summed across queues, shown next to the title.
const aggregate = computed(() => {
  const f = frame.value
  if (!f || Object.keys(f.queues).length === 0) return null
  const rates = { enqueued: 0, acked: 0, nacked: 0, dead_lettered: 0 }
  const depths = { ready: 0, scheduled: 0, inflight: 0, dead_lettered: 0 }
  for (const q of Object.values(f.queues)) {
    rates.enqueued += q.rates.enqueued
    rates.acked += q.rates.acked
    rates.nacked += q.rates.nacked
    rates.dead_lettered += q.rates.dead_lettered
    depths.ready += q.ready
    depths.scheduled += q.scheduled
    depths.inflight += q.inflight
    depths.dead_lettered += q.dead_lettered
  }
  return { rates, depths }
})

const drawerOpen = computed(() => typeof route.params.name === 'string' && route.params.name !== '')
const showNewQueue = ref(false)

function open(name: string) {
  void router.push({ name: 'queue', params: { name } })
}
</script>

<template>
  <div class="p-6">
    <div class="mb-4 flex flex-wrap items-center gap-x-5 gap-y-2">
      <h1 class="text-lg font-semibold">Queues</h1>
      <div
        v-if="aggregate"
        class="flex flex-wrap items-baseline gap-x-4 gap-y-1 text-sm text-ink-400 tabular-nums"
      >
        <span v-for="d in DEPTH_METRICS" :key="d.key">
          <span
            class="font-medium"
            :class="d.alert && aggregate.depths[d.key] > 0 ? 'text-red-400' : 'text-ink-100'"
          >
            {{ aggregate.depths[d.key].toLocaleString() }}
          </span>
          {{ d.label }}
        </span>
        <span class="text-ink-700">·</span>
        <span v-for="m in RATE_METRICS" :key="m.key">
          <span class="font-medium" :style="{ color: m.stroke }">
            {{ formatRate(aggregate.rates[m.key]) }}/s
          </span>
          {{ m.label }}
        </span>
      </div>
      <div class="ml-auto flex items-center gap-5">
        <RangePicker v-model="globalPick" title="Sparkline time range" />
        <StrictModeToggle v-if="canAdmin" />
        <button
          v-if="canAdmin"
          class="rounded bg-accent px-3 py-1.5 text-sm font-medium text-ink-950 hover:bg-accent-bright"
          @click="showNewQueue = true"
        >
          + New queue
        </button>
      </div>
    </div>
    <div v-if="!frame" class="text-sm text-ink-400">Connecting to stats stream…</div>
    <div
      v-else-if="queues.length === 0"
      class="flex flex-col items-center gap-4 rounded-lg border border-dashed border-ink-700 px-6 py-12 text-center text-sm text-ink-400"
    >
      <p>No queues yet. Create one, or enqueue a job to auto-create its queue.</p>
      <button
        v-if="canAdmin"
        class="rounded bg-accent px-3 py-1.5 text-sm font-medium text-ink-950 hover:bg-accent-bright"
        @click="showNewQueue = true"
      >
        + New queue
      </button>
    </div>
    <div v-else class="flex flex-col gap-3">
      <QueueCard
        v-for="q in queues"
        :key="q.name"
        :name="q.name"
        :frame="q.queue"
        :samples="history[q.name] ?? []"
        :declared="declared.has(q.name)"
        @click="open(q.name)"
      />
    </div>
    <NewQueueDialog v-if="showNewQueue" @close="showNewQueue = false" />
    <QueueDrawer v-if="drawerOpen" />
  </div>
</template>
