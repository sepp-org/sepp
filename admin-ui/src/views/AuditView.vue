<script setup lang="ts">
import { onMounted, onUnmounted, ref, shallowRef, watch } from 'vue'
import { api } from '../api/client'
import type { AuditEntry } from '../api/types'
import JsonView from '../components/JsonView.vue'
import { useStatsStream } from '../composables/useStatsStream'

const PAGE_SIZE = 50

const { status, auditTail, auditEpoch } = useStatsStream()

const actorFilter = ref('')
const prefixFilter = ref('')

// Sorted newest-first, deduped by seq. Live tail entries and fetched pages
// merge into the same set; both carry the identical entry shape. shallowRef
// for the same JsonValue-recursion reason as auditTail.
const rows = shallowRef<AuditEntry[]>([])
const cursor = ref<number | null>(null)
const reachedEnd = ref(false)
const loading = ref(false)
const loadingMore = ref(false)
const error = ref('')

// Filters as last applied by a reload. Fetches and live-tail matching both
// read this snapshot, not the inputs, so a half-typed filter never mixes
// into rows built under the previous one.
const applied = { actor: '', prefix: '' }
const filtersActive = ref(false)

function matchesFilters(e: AuditEntry): boolean {
  if (applied.actor !== '' && e.actor !== applied.actor) return false
  if (applied.prefix !== '' && !e.action.startsWith(applied.prefix)) return false
  return true
}

function merge(incoming: AuditEntry[]) {
  if (incoming.length === 0) return
  const bySeq = new Map(rows.value.map((e) => [e.seq, e] as const))
  for (const e of incoming) bySeq.set(e.seq, e)
  rows.value = [...bySeq.values()].sort((a, b) => b.seq - a.seq)
}

function fetchPage(before?: number) {
  return api.audit({
    before,
    limit: PAGE_SIZE,
    actor: applied.actor || undefined,
    actionPrefix: applied.prefix || undefined,
  })
}

// Bumped by every reset; a fetch that resolves under a newer generation is
// stale (filters changed or continuity broke mid-flight) and must not merge.
let generation = 0

// Full reset: the held rows are no longer known to be a contiguous suffix of
// the trail (filters changed, SSE reconnected or dropped events).
async function reload() {
  const gen = ++generation
  applied.actor = actorFilter.value.trim()
  applied.prefix = prefixFilter.value.trim()
  filtersActive.value = applied.actor !== '' || applied.prefix !== ''
  loading.value = true
  // An in-flight loadOlder is stale now and skips its own cleanup.
  loadingMore.value = false
  error.value = ''
  rows.value = []
  cursor.value = null
  reachedEnd.value = false
  try {
    const page = await fetchPage()
    if (gen !== generation) return
    merge(page.entries)
    cursor.value = page.next_before
    reachedEnd.value = page.next_before === null
  } catch (e) {
    if (gen !== generation) return
    error.value = e instanceof Error ? e.message : 'load failed'
  } finally {
    if (gen === generation) loading.value = false
  }
}

async function loadOlder() {
  if (loadingMore.value || reachedEnd.value || cursor.value === null) return
  const gen = generation
  loadingMore.value = true
  error.value = ''
  try {
    const page = await fetchPage(cursor.value)
    if (gen !== generation) return
    merge(page.entries)
    cursor.value = page.next_before
    reachedEnd.value = page.next_before === null
  } catch (e) {
    if (gen !== generation) return
    error.value = e instanceof Error ? e.message : 'load failed'
  } finally {
    if (gen === generation) loadingMore.value = false
  }
}

onMounted(() => void reload())
watch(auditEpoch, () => void reload())

// Replaying the whole buffer on every event is idempotent via the seq dedupe.
watch(auditTail, (tail) => merge(tail.filter(matchesFilters)))

let debounce: number | undefined
watch([actorFilter, prefixFilter], () => {
  if (debounce !== undefined) clearTimeout(debounce)
  debounce = window.setTimeout(() => void reload(), 400)
})
onUnmounted(() => {
  if (debounce !== undefined) clearTimeout(debounce)
})

function when(ms: number): string {
  return new Date(ms).toLocaleString()
}

function detailsText(e: AuditEntry): string {
  const s = JSON.stringify(e.details)
  return s === '{}' ? '' : s
}

const expanded = ref<number | null>(null)

function toggle(e: AuditEntry) {
  if (detailsText(e) === '') return
  expanded.value = expanded.value === e.seq ? null : e.seq
}
</script>

<template>
  <div class="p-6">
    <div class="mb-4 flex items-center gap-2">
      <h1 class="text-lg font-semibold text-ink-100">Audit log</h1>
      <span
        v-if="status !== 'live'"
        class="rounded-full bg-amber-500/15 px-2 py-0.5 text-xs text-amber-400"
      >
        live tail paused
      </span>
      <div class="ml-auto flex items-center gap-2">
        <input
          v-model="actorFilter"
          placeholder="actor"
          spellcheck="false"
          class="w-36 rounded border border-ink-800 bg-ink-950 px-2 py-1 font-mono text-xs text-ink-100 placeholder:font-sans placeholder:text-ink-500 focus:border-ink-600 focus:outline-none"
        />
        <input
          v-model="prefixFilter"
          placeholder="action prefix"
          spellcheck="false"
          class="w-40 rounded border border-ink-800 bg-ink-950 px-2 py-1 font-mono text-xs text-ink-100 placeholder:font-sans placeholder:text-ink-500 focus:border-ink-600 focus:outline-none"
        />
      </div>
    </div>

    <p v-if="error" class="mb-2 text-xs text-red-400">
      {{ error }}
      <button class="ml-2 underline hover:text-red-300" @click="reload()">Retry</button>
    </p>
    <p v-if="loading" class="text-sm text-ink-400">Loading…</p>
    <template v-else>
      <table class="w-full border-collapse text-left text-sm">
        <thead>
          <tr class="border-b border-ink-800 text-xs text-ink-400">
            <th class="py-2 pr-8 font-medium">Time</th>
            <th class="py-2 pr-8 font-medium">Actor</th>
            <th class="py-2 pr-8 font-medium">Action</th>
            <th class="w-full py-2 font-medium">Details</th>
          </tr>
        </thead>
        <tbody>
          <template v-for="e in rows" :key="e.seq">
            <tr
              class="border-b border-ink-800/60"
              :class="detailsText(e) ? 'cursor-pointer hover:bg-ink-900/60' : ''"
              @click="toggle(e)"
            >
              <td class="py-2 pr-8 whitespace-nowrap text-ink-300">
                {{ when(e.ts_ms) }}
              </td>
              <td class="py-2 pr-8 whitespace-nowrap">
                <span class="text-ink-200">{{ e.actor }}</span>
                <span class="ml-1.5 rounded-full bg-ink-800 px-1.5 py-0.5 text-xs text-ink-400">
                  {{ e.role }}
                </span>
              </td>
              <td class="py-2 pr-8 font-mono text-sm whitespace-nowrap text-ink-100">
                {{ e.action }}
              </td>
              <td class="w-full max-w-0 py-2">
                <code class="block truncate font-mono text-sm text-ink-400">
                  {{ detailsText(e) }}
                </code>
              </td>
            </tr>
            <tr v-if="expanded === e.seq" class="border-b border-ink-800/60">
              <td colspan="4" class="py-2">
                <JsonView :value="e.details" />
              </td>
            </tr>
          </template>
        </tbody>
      </table>
      <p v-if="rows.length === 0 && !error" class="py-6 text-sm text-ink-500">
        {{
          !reachedEnd
            ? 'No matches in the newest entries yet.'
            : filtersActive
              ? 'No audit entries match.'
              : 'No audit entries yet.'
        }}
      </p>
      <div v-if="!reachedEnd && cursor !== null" class="mt-3">
        <button
          class="rounded border border-ink-700 px-3 py-1.5 text-sm text-ink-300 hover:text-ink-100 disabled:opacity-50"
          :disabled="loadingMore"
          @click="loadOlder()"
        >
          {{ loadingMore ? 'Loading…' : 'Load more' }}
        </button>
      </div>
    </template>
  </div>
</template>
