<script setup lang="ts">
import { watchEffect } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useSession } from './composables/useSession'
import { useStatsStream, type StreamStatus } from './composables/useStatsStream'

const route = useRoute()
const router = useRouter()
const { name, authEnabled, loaded, refresh, logout } = useSession()
const { status, server, start, stop } = useStatsStream()

refresh().catch(() => {})

watchEffect(() => {
  if (loaded.value && (!authEnabled.value || name.value)) start()
})

async function onLogout() {
  await logout()
  stop()
  void router.push('/login')
}

const pillClasses: Record<StreamStatus, string> = {
  live: 'bg-emerald-500/15 text-emerald-400',
  polling: 'bg-amber-500/15 text-amber-400',
  connecting: 'bg-ink-700 text-ink-300',
}
</script>

<template>
  <div v-if="route.name === 'login'" class="min-h-screen">
    <RouterView />
  </div>
  <div v-else class="flex min-h-screen">
    <aside class="flex w-48 shrink-0 flex-col border-r border-ink-800 bg-ink-900">
      <RouterLink to="/" class="px-4 py-4 text-lg font-semibold text-accent">sepp</RouterLink>
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
        <span v-if="server" class="text-xs text-ink-400">v{{ server.version }}</span>
        <span class="rounded-full px-2 py-0.5 text-xs" :class="pillClasses[status]">
          {{ status }}
        </span>
        <div class="ml-auto flex items-center gap-3">
          <template v-if="authEnabled && name">
            <span class="text-sm text-ink-300">{{ name }}</span>
            <button class="text-sm text-ink-400 hover:text-ink-100" @click="onLogout">
              Log out
            </button>
          </template>
        </div>
      </header>
      <main class="min-w-0 flex-1 overflow-y-auto">
        <RouterView />
      </main>
    </div>
  </div>
</template>
