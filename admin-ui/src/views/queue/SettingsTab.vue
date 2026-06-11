<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { computed, reactive, ref, watch } from 'vue'
import { AdminApiError, api } from '../../api/client'
import type { QueueOverridesPatch, QueueUpdateRequest } from '../../api/types'
import TagInput from '../../components/TagInput.vue'
import { useSession } from '../../composables/useSession'

const props = defineProps<{ queue: string }>()

const { canAdmin } = useSession()

type ListKey = 'allowed_encodings' | 'allowed_job_types'
type NumberKey = Exclude<keyof QueueOverridesPatch, ListKey>
type FieldDef =
  | { key: NumberKey; label: string; kind: 'number'; restartOnly?: boolean }
  | { key: ListKey; label: string; kind: 'list'; restartOnly?: boolean }

const fields: FieldDef[] = [
  { key: 'max_lease_duration_ms', label: 'Max lease duration (ms)', kind: 'number' },
  { key: 'default_max_attempts', label: 'Default max attempts', kind: 'number' },
  { key: 'max_attempts_ceiling', label: 'Max attempts ceiling', kind: 'number' },
  { key: 'default_priority', label: 'Default priority', kind: 'number' },
  { key: 'max_payload_bytes', label: 'Max payload bytes', kind: 'number' },
  { key: 'allowed_encodings', label: 'Allowed encodings', kind: 'list' },
  { key: 'allowed_job_types', label: 'Allowed job types', kind: 'list' },
  { key: 'max_schedule_horizon_ms', label: 'Max schedule horizon (ms)', kind: 'number' },
  { key: 'max_custom_entries', label: 'Max custom entries', kind: 'number' },
  { key: 'max_custom_total_bytes', label: 'Max custom total bytes', kind: 'number' },
  { key: 'max_custom_key_bytes', label: 'Max custom key bytes', kind: 'number' },
  { key: 'dedup_window_ms', label: 'Dedup window (ms)', kind: 'number', restartOnly: true },
  { key: 'max_queue_depth', label: 'Max queue depth', kind: 'number' },
]

const queryClient = useQueryClient()
const { data: queueInfo } = useQuery({
  queryKey: computed(() => ['queues', props.queue]),
  queryFn: () => api.queue(props.queue),
})
const { data: config } = useQuery({ queryKey: ['config'], queryFn: () => api.config() })

const form = reactive<Record<string, string>>({})
const baseline = reactive<Record<string, string>>({})
const forceSync = ref(false)
const validationError = ref('')
const saveError = ref('')
const restartPaths = ref<string[]>([])

function toInput(v: number | string[] | null | undefined): string {
  if (v == null) return ''
  return Array.isArray(v) ? v.join(', ') : String(v)
}

const dirty = computed(() => fields.some((f) => (form[f.key] ?? '') !== (baseline[f.key] ?? '')))

watch(
  queueInfo,
  (q) => {
    if (!q) return
    const sync = !dirty.value || forceSync.value
    for (const f of fields) {
      baseline[f.key] = toInput(q.overrides?.[f.key])
      if (sync) form[f.key] = baseline[f.key]
    }
    forceSync.value = false
  },
  { immediate: true },
)

function effectiveText(f: FieldDef): string {
  const eff = queueInfo.value?.effective
  if (!eff) return ''
  const v = eff[f.key]
  if (v === null) return f.kind === 'list' ? 'any' : 'unlimited'
  return Array.isArray(v) ? v.join(', ') : String(v)
}

// List fields stay comma-joined strings in `form` so the dirty/baseline
// machinery is untouched; the pills are just a view over that string.
function listValue(key: ListKey): string[] {
  return (form[key] ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s !== '')
}

function setList(key: ListKey, tags: string[]) {
  form[key] = tags.join(', ')
}

function buildOverrides(): QueueOverridesPatch | null {
  const out: QueueOverridesPatch = {}
  for (const f of fields) {
    const raw = (form[f.key] ?? '').trim()
    if (raw === '') {
      out[f.key] = null
    } else if (f.kind === 'number') {
      const n = Number(raw)
      if (!Number.isInteger(n) || n < 0) {
        validationError.value = `${f.label} must be a non-negative integer`
        return null
      }
      out[f.key] = n
    } else {
      out[f.key] = raw
        .split(',')
        .map((s) => s.trim())
        .filter((s) => s !== '')
    }
  }
  return out
}

const { mutate: save, isPending: saving } = useMutation({
  mutationFn: (body: QueueUpdateRequest) => api.updateQueue(props.queue, body),
  onSuccess: (res) => {
    restartPaths.value = res.requires_restart
    saveError.value = ''
    forceSync.value = true
    void queryClient.invalidateQueries({ queryKey: ['queues'] })
    void queryClient.invalidateQueries({ queryKey: ['config'] })
  },
  onError: (e) => {
    saveError.value =
      e instanceof AdminApiError && e.status === 412
        ? 'Config changed elsewhere; reset and retry.'
        : e.message
  },
})

function submit() {
  validationError.value = ''
  const etag = config.value?.etag
  if (!etag) return
  const overrides = buildOverrides()
  if (!overrides) return
  save({ etag, overrides })
}

function reset() {
  for (const f of fields) form[f.key] = baseline[f.key] ?? ''
  validationError.value = ''
  saveError.value = ''
}
</script>

<template>
  <div class="flex flex-col gap-4">
    <p class="text-sm text-ink-400">
      Per-queue overrides for <span class="font-mono text-ink-200">{{ queue }}</span
      >. Empty fields fall back to the global limits.
      <span v-if="!canAdmin" class="text-ink-500">Editing requires the admin role.</span>
    </p>

    <div
      v-if="restartPaths.length > 0"
      class="rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-sm text-amber-300"
    >
      Saved. These fields require a server restart to take effect:
      <span class="font-mono">{{ restartPaths.join(', ') }}</span>
    </div>

    <p v-if="!queueInfo" class="text-sm text-ink-400">Loading…</p>
    <template v-else>
      <div class="grid grid-cols-2 gap-4">
        <div v-for="f in fields" :key="f.key" class="flex flex-col gap-1">
          <label class="text-xs text-ink-400" :for="`qset-${f.key}`">
            {{ f.label }}
            <span
              v-if="f.restartOnly"
              class="ml-1 rounded bg-amber-500/15 px-1 text-[10px] uppercase tracking-wide text-amber-400"
              title="Changing this takes effect only after a server restart"
              >restart only</span
            >
          </label>
          <TagInput
            v-if="f.kind === 'list'"
            :id="`qset-${f.key}`"
            :model-value="listValue(f.key)"
            :disabled="!canAdmin"
            :placeholder="effectiveText(f)"
            @update:model-value="(v) => setList(f.key as ListKey, v)"
          />
          <input
            v-else
            :id="`qset-${f.key}`"
            v-model="form[f.key]"
            :disabled="!canAdmin"
            :placeholder="effectiveText(f)"
            class="rounded border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm outline-none focus:border-accent disabled:opacity-50"
          />
          <p class="text-xs text-ink-500">
            {{
              (form[f.key] ?? '').trim() === ''
                ? `global default (${effectiveText(f)})`
                : 'override'
            }}
          </p>
        </div>
      </div>

      <p v-if="validationError" class="text-sm text-red-400">{{ validationError }}</p>
      <p v-if="saveError" class="text-sm text-red-400">{{ saveError }}</p>

      <div v-if="dirty" class="flex items-center gap-2">
        <button
          class="rounded bg-accent px-3 py-1.5 text-sm font-medium text-ink-950 hover:bg-accent-bright disabled:opacity-50"
          :disabled="saving || !config"
          @click="submit"
        >
          {{ saving ? 'Saving…' : 'Save overrides' }}
        </button>
        <button
          class="rounded border border-ink-700 px-3 py-1.5 text-sm text-ink-300 hover:text-ink-100"
          @click="reset"
        >
          Reset
        </button>
      </div>
    </template>
  </div>
</template>
