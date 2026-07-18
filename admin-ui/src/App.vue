<script setup lang="ts">
import { useQueryClient } from '@tanstack/vue-query'
import { ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { AdminApiError, api, setUnauthorizedHandler } from './api/client'
import SeppMark from './components/SeppMark.vue'
import { useSession } from './composables/useSession'
import { useStatsStream, type StreamStatus } from './composables/useStatsStream'

const route = useRoute()
const router = useRouter()
const queryClient = useQueryClient()
const { status, frame, server, start, stop } = useStatsStream()
const session = useSession()

// The stream follows the session: no SSE attempts while logged out, a fresh
// hello (full history) right after login. An unresolved probe (server down,
// loaded=false) runs the stream optimistically: it either recovers and the
// frame watcher below completes the probe, or its first 401 redirects to
// login via the handler.
watch(
  () => (session.loaded.value ? session.authed.value : true),
  (ready) => {
    if (ready) start()
    else stop()
  },
  { immediate: true },
)

// Completes a session probe that failed while the server was down; no-op
// once loaded.
watch(frame, (f) => {
  if (f && !session.loaded.value) void session.refresh().catch(() => {})
})

setUnauthorizedHandler(() => {
  session.reset()
  // Cached queries (job payloads, config) belong to the dead session.
  queryClient.clear()
  if (route.name !== 'login') {
    void router.push({
      name: 'login',
      query: route.fullPath !== '/' ? { next: route.fullPath } : {},
    })
  }
})

async function signOut() {
  try {
    await session.logout()
  } catch {
    // Best-effort server-side invalidation; local sign-out already happened.
  } finally {
    queryClient.clear()
    void router.push({ name: 'login' })
  }
}

const jobSearch = ref('')
const searching = ref(false)
const searchError = ref('')

// The lookup resolves the owning queue and state first, then deep-links into
// the queue drawer; JobsTab consumes the `job` query param and opens the
// detail panel from the seeded ['job', id] cache entry.
async function findJob() {
  const id = jobSearch.value.trim()
  if (id === '' || searching.value) return
  searching.value = true
  searchError.value = ''
  try {
    const job = await api.job(id)
    queryClient.setQueryData(['job', id], job)
    jobSearch.value = ''
    await router.push({
      name: 'queue',
      params: { name: job.queue, tab: 'jobs' },
      query: { job: id },
    })
  } catch (e) {
    searchError.value =
      e instanceof AdminApiError && e.status === 404
        ? 'No live job with this ID: it may have completed (acked) or been dead-lettered.'
        : e instanceof Error
          ? e.message
          : 'lookup failed'
  } finally {
    searching.value = false
  }
}

// Typing or navigating away dismisses a stale lookup error.
watch(jobSearch, () => {
  searchError.value = ''
})
watch(
  () => route.fullPath,
  () => {
    searchError.value = ''
  },
)

const pill: Record<StreamStatus, { label: string; classes: string }> = {
  live: { label: 'online', classes: 'bg-emerald-500/15 text-emerald-400' },
  polling: { label: 'offline', classes: 'bg-red-500/15 text-red-400' },
  connecting: { label: 'connecting', classes: 'bg-ink-700 text-ink-300' },
}
</script>

<template>
  <RouterView v-if="route.name === 'login'" />
  <div v-else class="flex min-h-screen">
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
        <form class="relative ml-auto" @submit.prevent="findJob">
          <input
            v-model="jobSearch"
            placeholder="Find job by ID"
            spellcheck="false"
            :class="searching ? 'opacity-60' : ''"
            class="w-56 rounded border border-ink-800 bg-ink-950 px-2 py-1 font-mono text-xs text-ink-100 placeholder:font-sans placeholder:text-ink-500 focus:border-ink-600 focus:outline-none"
          />
          <p
            v-if="searchError"
            class="absolute top-full right-0 z-20 mt-1 w-72 rounded border border-ink-700 bg-ink-900 px-3 py-2 text-xs text-ink-300 shadow-lg"
          >
            {{ searchError }}
          </p>
        </form>
        <div v-if="session.authEnabled.value && session.name.value" class="flex items-center gap-2">
          <span class="text-xs text-ink-400">
            <span class="text-ink-200">{{ session.name.value }}</span>
          </span>
          <span class="rounded-full bg-ink-800 px-2 py-0.5 text-xs text-ink-300">
            {{ session.role.value }}
          </span>
          <button
            class="rounded px-2 py-1 text-xs text-ink-400 hover:bg-ink-800 hover:text-ink-100"
            @click="signOut"
          >
            Sign out
          </button>
        </div>
      </header>
      <main class="min-w-0 flex-1 overflow-y-auto">
        <RouterView />
      </main>
    </div>
  </div>
</template>
