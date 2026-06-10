<script setup lang="ts">
import { useQuery, useQueryClient } from '@tanstack/vue-query'
import { computed, reactive, ref } from 'vue'
import { AdminApiError, api } from '../api/client'
import type { ConfigChange, EffectiveConfig, JsonValue } from '../api/types'
import ConfigField from '../components/ConfigField.vue'
import { matchesPath } from '../lib/paths'

type FieldKind = 'string' | 'number' | 'boolean' | 'string[]'

interface FieldSpec {
  key: string
  kind: FieldKind
  options?: string[]
  // Offers a "Generate" button that appends a random secret to the list.
  generate?: boolean
}

type SectionTable = Exclude<keyof EffectiveConfig, 'queues'>

const sections: { table: SectionTable; fields: FieldSpec[] }[] = [
  {
    table: 'server',
    fields: [
      { key: 'listen_addr', kind: 'string' },
      { key: 'db_path', kind: 'string' },
      { key: 'tls_cert_path', kind: 'string' },
      { key: 'tls_key_path', kind: 'string' },
      { key: 'strict_queues', kind: 'boolean' },
    ],
  },
  { table: 'auth', fields: [{ key: 'api_keys', kind: 'string[]', generate: true }] },
  {
    table: 'limits',
    fields: [
      { key: 'max_lease_duration_ms', kind: 'number' },
      { key: 'default_max_attempts', kind: 'number' },
      { key: 'max_attempts_ceiling', kind: 'number' },
      { key: 'default_priority', kind: 'number' },
      { key: 'max_reserve_batch', kind: 'number' },
      { key: 'max_reserve_queues', kind: 'number' },
      { key: 'max_wait_timeout_ms', kind: 'number' },
      { key: 'max_enqueue_batch', kind: 'number' },
      { key: 'max_queue_depth', kind: 'number' },
      { key: 'max_payload_bytes', kind: 'number' },
      { key: 'max_message_bytes', kind: 'number' },
      { key: 'max_custom_entries', kind: 'number' },
      { key: 'max_custom_total_bytes', kind: 'number' },
      { key: 'max_custom_key_bytes', kind: 'number' },
      { key: 'max_queue_name_bytes', kind: 'number' },
      { key: 'max_job_type_bytes', kind: 'number' },
      { key: 'max_idempotency_key_bytes', kind: 'number' },
      { key: 'max_schedule_horizon_ms', kind: 'number' },
      { key: 'allowed_encodings', kind: 'string[]' },
    ],
  },
  {
    table: 'storage',
    fields: [
      { key: 'persist_mode', kind: 'string', options: ['sync_all', 'sync_data', 'buffer'] },
      { key: 'sweep_interval_ms', kind: 'number' },
      { key: 'sweep_limit', kind: 'number' },
      { key: 'dedup_window_ms', kind: 'number' },
      { key: 'dead_letter_retention_ms', kind: 'number' },
      { key: 'command_queue_capacity', kind: 'number' },
      { key: 'cache_size_bytes', kind: 'number' },
      { key: 'max_journaling_size_bytes', kind: 'number' },
      { key: 'max_cached_files', kind: 'number' },
      { key: 'worker_threads', kind: 'number' },
    ],
  },
  {
    table: 'logging',
    fields: [
      { key: 'level', kind: 'string' },
      { key: 'format', kind: 'string', options: ['text', 'json'] },
    ],
  },
  {
    table: 'tracing',
    fields: [
      { key: 'enabled', kind: 'boolean' },
      { key: 'otlp_endpoint', kind: 'string' },
      { key: 'service_name', kind: 'string' },
      { key: 'sample_ratio', kind: 'number' },
    ],
  },
  {
    table: 'metrics',
    fields: [
      { key: 'enabled', kind: 'boolean' },
      { key: 'otlp_endpoint', kind: 'string' },
      { key: 'export_interval_ms', kind: 'number' },
      { key: 'prometheus_enabled', kind: 'boolean' },
      { key: 'prometheus_listen_addr', kind: 'string' },
    ],
  },
  {
    table: 'admin',
    fields: [
      { key: 'enabled', kind: 'boolean' },
      { key: 'listen_addr', kind: 'string' },
    ],
  },
]

const queryClient = useQueryClient()
const { data, error, isLoading, refetch } = useQuery({
  queryKey: ['config'],
  queryFn: () => api.config(),
  staleTime: Infinity,
})

const pending = reactive<Record<string, JsonValue | null>>({})
const dirtyCount = computed(() => Object.keys(pending).length)

const saving = ref(false)
const saveError = ref('')
const saveNotice = ref('')
const restartFields = ref<string[]>([])
const showReloadPrompt = ref(false)

function fieldValue(table: SectionTable, key: string): JsonValue {
  const sect = data.value?.effective[table]
  if (!sect) return null
  return (sect as unknown as Record<string, JsonValue>)[key] ?? null
}

function shownValue(table: SectionTable, key: string): JsonValue {
  const path = `${table}.${key}`
  return path in pending ? (pending[path] ?? null) : fieldValue(table, key)
}

function same(a: JsonValue | null, b: JsonValue | null): boolean {
  return JSON.stringify(a ?? null) === JSON.stringify(b ?? null)
}

function onChange(table: SectionTable, key: string, value: JsonValue | null) {
  const path = `${table}.${key}`
  const base = fieldValue(table, key)
  // Zero pills normally means "unset the key", but when the saved value is an
  // explicit [] (reject-all), removing and re-adding a pill must not turn
  // into a destructive delete-the-key change.
  if (value === null && Array.isArray(base) && base.length === 0) value = base
  if (same(value, base)) delete pending[path]
  else pending[path] = value
}

function revert(path: string) {
  delete pending[path]
}

function discardAll() {
  for (const k of Object.keys(pending)) delete pending[k]
  saveError.value = ''
}

function pinned(path: string): boolean {
  return matchesPath(data.value?.env_pinned ?? [], path)
}

function restartOnly(path: string): boolean {
  return matchesPath(data.value?.restart_only ?? [], path)
}

function pendingRestart(path: string): boolean {
  return matchesPath(data.value?.pending_restart ?? [], path)
}

async function save() {
  if (!data.value || saving.value || dirtyCount.value === 0) return
  saving.value = true
  saveError.value = ''
  saveNotice.value = ''
  try {
    const changes: ConfigChange[] = Object.entries(pending).map(([path, value]) => ({
      path,
      value,
    }))
    const res = await api.updateConfig({ etag: data.value.etag, changes })
    discardAll()
    if (res.requires_restart.length > 0) restartFields.value = res.requires_restart
    else if (!res.applied) saveNotice.value = 'Written to sepp.toml; reload not confirmed yet.'
    await queryClient.invalidateQueries({ queryKey: ['config'] })
  } catch (e) {
    if (e instanceof AdminApiError && e.status === 412) showReloadPrompt.value = true
    else saveError.value = e instanceof AdminApiError ? e.message : 'save failed'
  } finally {
    saving.value = false
  }
}

async function reloadAfterConflict() {
  showReloadPrompt.value = false
  discardAll()
  await refetch()
}
</script>

<template>
  <div class="mx-auto max-w-3xl p-6">
    <h1 class="mb-4 text-lg font-semibold">Config</h1>

    <div
      v-if="data && data.pending_restart.length > 0"
      class="mb-4 rounded border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-300"
    >
      sepp.toml changed since the server started; the running values still apply for:
      <span class="font-mono">{{ data.pending_restart.join(', ') }}</span>.
      Restart the server to pick {{ data.pending_restart.length === 1 ? 'it' : 'them' }} up.
    </div>
    <div
      v-else-if="restartFields.length > 0"
      class="mb-4 rounded border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-300"
    >
      Saved. These fields require a server restart to take effect:
      <span class="font-mono">{{ restartFields.join(', ') }}</span>
    </div>
    <p v-if="saveNotice" class="mb-4 text-sm text-ink-400">{{ saveNotice }}</p>

    <div v-if="isLoading" class="text-sm text-ink-400">Loading config…</div>
    <div v-else-if="error" class="text-sm text-red-400">{{ error.message }}</div>

    <template v-else-if="data">
      <section
        v-for="section in sections"
        :key="section.table"
        class="mb-6 rounded-lg border border-ink-800 bg-ink-900"
      >
        <h2 class="border-b border-ink-800 px-4 py-2 font-mono text-sm text-accent">
          [{{ section.table }}]
        </h2>
        <div class="divide-y divide-ink-800/60">
          <ConfigField
            v-for="field in section.fields"
            :key="field.key"
            :path="`${section.table}.${field.key}`"
            :label="field.key"
            :kind="field.kind"
            :options="field.options"
            :generate="field.generate"
            :value="shownValue(section.table, field.key)"
            :dirty="`${section.table}.${field.key}` in pending"
            :env-pinned="pinned(`${section.table}.${field.key}`)"
            :restart-only="restartOnly(`${section.table}.${field.key}`)"
            :pending-restart="pendingRestart(`${section.table}.${field.key}`)"
            @change="(v) => onChange(section.table, field.key, v)"
            @revert="revert(`${section.table}.${field.key}`)"
          />
        </div>
      </section>

      <section class="mb-6 rounded-lg border border-ink-800 bg-ink-900">
        <h2 class="border-b border-ink-800 px-4 py-2 font-mono text-sm text-accent">[[queues]]</h2>
        <div class="px-4 py-3">
          <p class="mb-2 text-xs text-ink-500">
            Queue overrides are edited from each queue's settings drawer.
          </p>
          <p v-if="data.effective.queues.length === 0" class="text-sm text-ink-400">
            No queues declared.
          </p>
          <div v-else class="flex flex-wrap gap-2">
            <RouterLink
              v-for="q in data.effective.queues"
              :key="q.name"
              :to="`/queues/${encodeURIComponent(q.name)}/settings`"
              class="rounded border border-ink-700 bg-ink-950 px-2 py-1 font-mono text-sm text-ink-200 hover:border-accent hover:text-ink-100"
            >
              {{ q.name }}
            </RouterLink>
          </div>
        </div>
      </section>

      <div
        v-if="dirtyCount > 0"
        class="sticky bottom-4 z-10 flex items-center gap-3 rounded-lg border border-ink-700 bg-ink-900 px-4 py-3 shadow-lg"
      >
        <span class="text-sm text-ink-200">
          {{ dirtyCount }} unsaved change{{ dirtyCount === 1 ? '' : 's' }}
        </span>
        <span v-if="saveError" class="text-sm text-red-400">{{ saveError }}</span>
        <div class="ml-auto flex gap-2">
          <button
            class="rounded border border-ink-700 px-3 py-1.5 text-sm text-ink-300 hover:text-ink-100"
            @click="discardAll"
          >
            Discard
          </button>
          <button
            class="rounded bg-accent px-3 py-1.5 text-sm font-medium text-ink-950 hover:bg-accent-bright disabled:opacity-50"
            :disabled="saving"
            @click="save"
          >
            {{ saving ? 'Saving…' : 'Save changes' }}
          </button>
        </div>
      </div>
    </template>

    <div
      v-if="showReloadPrompt"
      class="fixed inset-0 z-50 flex items-center justify-center bg-black/60"
    >
      <div class="w-96 rounded-lg border border-ink-700 bg-ink-900 p-5">
        <h3 class="mb-2 text-sm font-semibold">Config changed on disk</h3>
        <p class="mb-4 text-sm text-ink-300">
          sepp.toml was modified since this page loaded. Reload to pick up the latest values; your
          unsaved edits will be discarded.
        </p>
        <div class="flex justify-end gap-2">
          <button
            class="rounded border border-ink-700 px-3 py-1.5 text-sm text-ink-300 hover:text-ink-100"
            @click="showReloadPrompt = false"
          >
            Keep editing
          </button>
          <button
            class="rounded bg-accent px-3 py-1.5 text-sm font-medium text-ink-950 hover:bg-accent-bright"
            @click="reloadAfterConflict"
          >
            Discard and reload
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
