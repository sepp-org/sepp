<script setup lang="ts">
import { computed, type Component } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import Drawer from '../../components/Drawer.vue'
import { useStatsStream } from '../../composables/useStatsStream'
import DangerTab from './DangerTab.vue'
import EnqueueTab from './EnqueueTab.vue'
import JobsTab from './JobsTab.vue'
import SettingsTab from './SettingsTab.vue'

type TabId = 'jobs' | 'settings' | 'enqueue' | 'danger'

const tabs: { id: TabId; label: string }[] = [
  { id: 'jobs', label: 'Jobs' },
  { id: 'settings', label: 'Settings' },
  { id: 'enqueue', label: 'Enqueue' },
  { id: 'danger', label: 'Danger' },
]

const tabComponents: Record<TabId, Component> = {
  jobs: JobsTab,
  settings: SettingsTab,
  enqueue: EnqueueTab,
  danger: DangerTab,
}

const route = useRoute()
const router = useRouter()
const { frame } = useStatsStream()

const name = computed(() => {
  const n = route.params.name
  return (Array.isArray(n) ? n[0] : n) ?? ''
})

const tab = computed<TabId>(() => {
  const t = route.params.tab
  const v = Array.isArray(t) ? t[0] : t
  return v === 'settings' || v === 'enqueue' || v === 'danger' ? v : 'jobs'
})

const badges = computed(() => {
  const q = frame.value?.queues[name.value]
  if (!q) return []
  return [
    { label: 'ready', value: q.ready },
    { label: 'scheduled', value: q.scheduled },
    { label: 'in-flight', value: q.inflight },
    { label: 'dead', value: q.dead_lettered },
  ]
})

function close() {
  void router.push('/')
}
</script>

<template>
  <Drawer @close="close">
    <div class="flex items-center gap-3 border-b border-ink-800 px-5 py-4">
      <h2 class="font-mono text-lg font-semibold">{{ name }}</h2>
      <div class="flex flex-wrap gap-1.5">
        <span
          v-for="b in badges"
          :key="b.label"
          class="rounded-full bg-ink-800 px-2 py-0.5 text-xs text-ink-300"
        >
          {{ b.label }} <span class="font-medium text-ink-100">{{ b.value }}</span>
        </span>
      </div>
      <button
        class="ml-auto px-1 text-lg text-ink-400 hover:text-ink-100"
        aria-label="Close"
        @click="close"
      >
        &times;
      </button>
    </div>
    <nav class="flex gap-1 border-b border-ink-800 px-5">
      <RouterLink
        v-for="t in tabs"
        :key="t.id"
        :to="`/queues/${encodeURIComponent(name)}/${t.id}`"
        class="-mb-px border-b-2 px-3 py-2 text-sm"
        :class="
          t.id === tab
            ? 'border-accent text-ink-100'
            : 'border-transparent text-ink-400 hover:text-ink-100'
        "
      >
        {{ t.label }}
      </RouterLink>
    </nav>
    <div class="min-h-0 flex-1 overflow-y-auto p-5">
      <component :is="tabComponents[tab]" :key="`${name}:${tab}`" :queue="name" />
    </div>
  </Drawer>
</template>
