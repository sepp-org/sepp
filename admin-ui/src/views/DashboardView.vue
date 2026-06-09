<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api/client'
import QueueCard from '../components/QueueCard.vue'
import { useStatsStream } from '../composables/useStatsStream'
import QueueDrawer from './queue/QueueDrawer.vue'

const route = useRoute()
const router = useRouter()
const { frame, history } = useStatsStream()

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

const drawerOpen = computed(() => typeof route.params.name === 'string' && route.params.name !== '')

function open(name: string) {
  void router.push({ name: 'queue', params: { name } })
}
</script>

<template>
  <div class="p-6">
    <h1 class="mb-4 text-lg font-semibold">Queues</h1>
    <div v-if="!frame" class="text-sm text-ink-400">Connecting to stats stream…</div>
    <div
      v-else-if="queues.length === 0"
      class="rounded-lg border border-dashed border-ink-700 px-6 py-12 text-center text-sm text-ink-400"
    >
      No queues yet. Declare one in sepp.toml or enqueue a job to get started.
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
    <QueueDrawer v-if="drawerOpen" />
  </div>
</template>
