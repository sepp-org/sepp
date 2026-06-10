<script setup lang="ts">
import { onMounted } from 'vue'
import SeppMark from './components/SeppMark.vue'
import { useStatsStream, type StreamStatus } from './composables/useStatsStream'

const { status, server, start } = useStatsStream()

onMounted(start)

const pill: Record<StreamStatus, { label: string; classes: string }> = {
  live: { label: 'online', classes: 'bg-emerald-500/15 text-emerald-400' },
  polling: { label: 'offline', classes: 'bg-red-500/15 text-red-400' },
  connecting: { label: 'connecting', classes: 'bg-ink-700 text-ink-300' },
}
</script>

<template>
  <div class="flex min-h-screen">
    <aside class="flex w-48 shrink-0 flex-col border-r border-ink-800 bg-ink-900">
      <RouterLink to="/" class="flex items-center gap-2.5 px-4 py-4 text-ink-100">
        <SeppMark :size="22" />
        <span class="font-mono text-[17px] leading-none font-semibold tracking-[-0.5px]">
          sepp
        </span>
      </RouterLink>
      <nav class="flex flex-col gap-1 px-2">
        <RouterLink
          to="/"
          class="rounded px-2 py-1.5 text-sm text-ink-300 hover:bg-ink-800 hover:text-ink-100"
          exact-active-class="bg-ink-800 text-ink-100"
        >
          Queues
        </RouterLink>
        <RouterLink
          to="/config"
          class="rounded px-2 py-1.5 text-sm text-ink-300 hover:bg-ink-800 hover:text-ink-100"
          active-class="bg-ink-800 text-ink-100"
        >
          Config
        </RouterLink>
      </nav>
    </aside>
    <div class="flex min-w-0 flex-1 flex-col">
      <header class="flex items-center gap-3 border-b border-ink-800 px-4 py-2">
        <span v-if="server" class="text-xs text-ink-400">
          server version: <span class="text-ink-200">v{{ server.version }}</span>
        </span>
        <span class="rounded-full px-2 py-0.5 text-xs" :class="pill[status].classes">
          {{ pill[status].label }}
        </span>
      </header>
      <main class="min-w-0 flex-1 overflow-y-auto">
        <RouterView />
      </main>
    </div>
  </div>
</template>
