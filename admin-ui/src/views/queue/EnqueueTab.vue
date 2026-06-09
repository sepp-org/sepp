<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { computed, ref, watch } from 'vue'
import { AdminApiError, api } from '../../api/client'
import type { EnqueueJobRequest, EnqueuePayload, EnqueueRejection } from '../../api/types'
import CopyButton from '../../components/CopyButton.vue'

const props = defineProps<{ queue: string }>()

const queryClient = useQueryClient()
const { data: queueInfo } = useQuery({
  queryKey: computed(() => ['queues', props.queue]),
  queryFn: () => api.queue(props.queue),
})

const payloadTabs = [
  { id: 'json', label: 'JSON' },
  { id: 'text', label: 'Text' },
  { id: 'base64', label: 'Base64' },
] as const
type PayloadTab = (typeof payloadTabs)[number]['id']

const jobType = ref('')
const payloadTab = ref<PayloadTab>('json')
const payloadText = ref('')
const encoding = ref('json')
const priority = ref(0)
const priorityTouched = ref(false)
const maxAttempts = ref('')
const scheduleAt = ref('')
const idempotencyKey = ref('')
const customRows = ref<{ key: string; value: string }[]>([])

const formError = ref('')
const rejection = ref<EnqueueRejection | null>(null)
const lastJobId = ref('')

const allowedEncodings = computed(() => queueInfo.value?.effective.allowed_encodings ?? null)
const encodingSuggestions = computed(() => allowedEncodings.value ?? ['json', 'text'])

watch(
  () => queueInfo.value?.effective.default_priority,
  (p) => {
    if (p !== undefined && !priorityTouched.value) priority.value = p
  },
  { immediate: true },
)

const tabDefaults: Record<PayloadTab, string> = { json: 'json', text: 'text', base64: '' }
watch(payloadTab, (next, prev) => {
  if (encoding.value === tabDefaults[prev] || encoding.value === '') {
    encoding.value = tabDefaults[next]
  }
})

function addCustomRow() {
  customRows.value.push({ key: '', value: '' })
}

function removeCustomRow(i: number) {
  customRows.value.splice(i, 1)
}

function buildPayload(): EnqueuePayload | undefined {
  const text = payloadText.value
  if (text.trim() === '') return undefined
  if (payloadTab.value === 'json') {
    try {
      JSON.parse(text)
    } catch (e) {
      throw new Error(`invalid JSON: ${(e as Error).message}`)
    }
    return { encoding: encoding.value, data_text: text }
  }
  if (payloadTab.value === 'text') return { encoding: encoding.value, data_text: text }
  const b64 = text.replace(/\s+/g, '')
  try {
    atob(b64)
  } catch {
    throw new Error('invalid base64')
  }
  return { encoding: encoding.value, data_b64: b64 }
}

function buildRequest(): EnqueueJobRequest {
  if (!jobType.value.trim()) throw new Error('job type is required')
  const req: EnqueueJobRequest = { job_type: jobType.value.trim(), priority: priority.value }
  const payload = buildPayload()
  if (payload) req.payload = payload
  if (maxAttempts.value.trim() !== '') {
    const n = Number(maxAttempts.value)
    if (!Number.isInteger(n) || n < 1) throw new Error('max attempts must be a positive integer')
    req.max_attempts = n
  }
  if (scheduleAt.value) {
    const ms = new Date(scheduleAt.value).getTime()
    if (Number.isNaN(ms)) throw new Error('invalid schedule time')
    req.scheduled_at_ms = ms
  }
  if (idempotencyKey.value.trim()) req.idempotency_key = idempotencyKey.value.trim()
  const custom: Record<string, string> = {}
  for (const row of customRows.value) {
    if (row.key.trim()) custom[row.key.trim()] = row.value
  }
  if (Object.keys(custom).length > 0) req.custom = custom
  return req
}

const { mutate: enqueue, isPending: enqueueing } = useMutation({
  mutationFn: (body: EnqueueJobRequest) => api.enqueue(props.queue, body),
  onSuccess: (res) => {
    lastJobId.value = res.job_id
    rejection.value = null
    formError.value = ''
    void queryClient.invalidateQueries({ queryKey: ['jobs', props.queue] })
  },
  onError: (e) => {
    lastJobId.value = ''
    if (e instanceof AdminApiError && e.rejection) {
      rejection.value = e.rejection
      formError.value = ''
    } else {
      rejection.value = null
      formError.value = e.message
    }
  },
})

function submit() {
  rejection.value = null
  lastJobId.value = ''
  try {
    const req = buildRequest()
    formError.value = ''
    enqueue(req)
  } catch (e) {
    formError.value = (e as Error).message
  }
}
</script>

<template>
  <form class="flex max-w-xl flex-col gap-4" @submit.prevent="submit">
    <div class="flex flex-col gap-1">
      <label class="text-xs text-ink-400" for="enq-type">Job type</label>
      <input
        id="enq-type"
        v-model="jobType"
        placeholder="send_email"
        class="rounded border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm outline-none focus:border-accent"
      />
    </div>

    <div class="flex flex-col gap-1">
      <span class="text-xs text-ink-400">Payload</span>
      <div class="flex gap-1">
        <button
          v-for="t in payloadTabs"
          :key="t.id"
          type="button"
          class="rounded px-2.5 py-1 text-sm"
          :class="
            t.id === payloadTab ? 'bg-ink-800 text-ink-100' : 'text-ink-400 hover:text-ink-100'
          "
          @click="payloadTab = t.id"
        >
          {{ t.label }}
        </button>
      </div>
      <textarea
        v-model="payloadText"
        rows="6"
        :placeholder="payloadTab === 'json' ? '{ }' : payloadTab === 'base64' ? 'base64 bytes' : 'plain text'"
        class="rounded border border-ink-700 bg-ink-950 px-3 py-2 font-mono text-sm outline-none focus:border-accent"
      />
    </div>

    <div class="flex flex-col gap-1">
      <label class="text-xs text-ink-400" for="enq-encoding">Encoding</label>
      <input
        id="enq-encoding"
        v-model="encoding"
        list="enq-encodings"
        class="rounded border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm outline-none focus:border-accent"
      />
      <datalist id="enq-encodings">
        <option v-for="e in encodingSuggestions" :key="e" :value="e" />
      </datalist>
      <p v-if="allowedEncodings" class="text-xs text-ink-500">
        Allowed: {{ allowedEncodings.join(', ') }}
      </p>
    </div>

    <div class="flex flex-col gap-1">
      <label class="text-xs text-ink-400" for="enq-priority">Priority: {{ priority }}</label>
      <input
        id="enq-priority"
        v-model.number="priority"
        type="range"
        min="0"
        max="9"
        step="1"
        class="accent-accent"
        @input="priorityTouched = true"
      />
    </div>

    <div class="grid grid-cols-2 gap-4">
      <div class="flex flex-col gap-1">
        <label class="text-xs text-ink-400" for="enq-attempts">Max attempts</label>
        <input
          id="enq-attempts"
          v-model="maxAttempts"
          :placeholder="String(queueInfo?.effective.default_max_attempts ?? '')"
          class="rounded border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm outline-none focus:border-accent"
        />
      </div>
      <div class="flex flex-col gap-1">
        <label class="text-xs text-ink-400" for="enq-schedule">Schedule at</label>
        <input
          id="enq-schedule"
          v-model="scheduleAt"
          type="datetime-local"
          class="rounded border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm outline-none focus:border-accent"
        />
      </div>
    </div>

    <div class="flex flex-col gap-1">
      <label class="text-xs text-ink-400" for="enq-idem">Idempotency key</label>
      <input
        id="enq-idem"
        v-model="idempotencyKey"
        placeholder="optional"
        class="rounded border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm outline-none focus:border-accent"
      />
    </div>

    <div class="flex flex-col gap-2">
      <span class="text-xs text-ink-400">Custom</span>
      <div v-for="(row, i) in customRows" :key="i" class="flex gap-2">
        <input
          v-model="row.key"
          placeholder="key"
          class="w-40 rounded border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm outline-none focus:border-accent"
        />
        <input
          v-model="row.value"
          placeholder="value"
          class="flex-1 rounded border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm outline-none focus:border-accent"
        />
        <button
          type="button"
          class="text-sm text-ink-400 hover:text-red-400"
          @click="removeCustomRow(i)"
        >
          Remove
        </button>
      </div>
      <button
        type="button"
        class="self-start text-sm text-ink-400 hover:text-ink-100"
        @click="addCustomRow"
      >
        + Add entry
      </button>
    </div>

    <div
      v-if="rejection"
      class="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-400"
    >
      <p class="font-medium">Rejected: {{ rejection.reason }}</p>
      <p v-if="rejection.detail">{{ rejection.detail }}</p>
    </div>
    <p v-else-if="formError" class="text-sm text-red-400">{{ formError }}</p>
    <div
      v-if="lastJobId"
      class="flex items-center gap-2 rounded border border-emerald-500/40 bg-emerald-500/10 px-3 py-2 text-sm text-emerald-400"
    >
      Enqueued <span class="font-mono">{{ lastJobId }}</span>
      <CopyButton :text="lastJobId" />
    </div>

    <button
      type="submit"
      class="self-start rounded bg-accent px-4 py-2 text-sm font-medium text-ink-950 hover:bg-accent-bright disabled:opacity-50"
      :disabled="enqueueing || !jobType.trim()"
    >
      {{ enqueueing ? 'Enqueuing…' : 'Enqueue' }}
    </button>
  </form>
</template>
