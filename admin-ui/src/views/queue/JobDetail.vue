<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed } from 'vue'
import { api } from '../../api/client'
import type { JobState, JobSummary } from '../../api/types'
import CopyButton from '../../components/CopyButton.vue'
import PayloadView from '../../components/PayloadView.vue'

const props = defineProps<{ queue: string; state: JobState; job: JobSummary }>()

const { data: detail, error } = useQuery({
  queryKey: computed(() =>
    props.state === 'dead_letter'
      ? ['dead-letter', props.queue, props.job.key_b64]
      : ['job', props.job.id],
  ),
  queryFn: () =>
    props.state === 'dead_letter'
      ? api.deadLetter(props.queue, props.job.key_b64)
      : api.job(props.job.id),
  retry: false,
})

const full = computed<JobSummary>(() => detail.value ?? props.job)
const payloadB64 = computed(() => detail.value?.payload.data_b64 ?? props.job.payload.data_b64)
const customEntries = computed(() => Object.entries(full.value.custom ?? {}))

const stateLabels: Record<JobState, string> = {
  ready: 'ready',
  scheduled: 'scheduled',
  inflight: 'in-flight',
  dead_letter: 'dead letter',
}

const times = computed(() => {
  const f = full.value
  const rows: { label: string; ms: number }[] = [{ label: 'Enqueued', ms: f.enqueued_at_ms }]
  if (f.scheduled_at_ms !== undefined) rows.push({ label: 'Scheduled', ms: f.scheduled_at_ms })
  if (f.lease_expires_at_ms !== undefined)
    rows.push({ label: 'Lease expires', ms: f.lease_expires_at_ms })
  if (f.failed_at_ms !== undefined) rows.push({ label: 'Failed', ms: f.failed_at_ms })
  return rows
})

const downloadName = computed(() => {
  const enc = full.value.payload.encoding.toLowerCase()
  const ext = enc.includes('json')
    ? 'json'
    : enc.includes('text') || enc.includes('utf') || enc.includes('plain')
      ? 'txt'
      : 'bin'
  return `${full.value.id}.${ext}`
})

function absTime(ms: number): string {
  return new Date(ms).toLocaleString()
}

function relTime(ms: number): string {
  const diff = ms - Date.now()
  const abs = Math.abs(diff)
  const units: [number, string][] = [
    [86_400_000, 'd'],
    [3_600_000, 'h'],
    [60_000, 'm'],
    [1_000, 's'],
  ]
  for (const [size, label] of units) {
    if (abs >= size) {
      const n = Math.round(abs / size)
      return diff < 0 ? `${n}${label} ago` : `in ${n}${label}`
    }
  }
  return 'now'
}
</script>

<template>
  <div class="flex flex-col gap-4">
    <div class="flex items-center gap-2">
      <span class="font-mono text-sm">{{ full.id }}</span>
      <CopyButton :text="full.id" />
      <span class="rounded-full bg-ink-800 px-2 py-0.5 text-xs text-ink-300">
        {{ stateLabels[state] }}
      </span>
    </div>

    <p v-if="error" class="text-sm text-amber-400">{{ error.message }}</p>

    <dl class="grid grid-cols-2 gap-x-6 gap-y-3 text-sm sm:grid-cols-3">
      <div>
        <dt class="text-xs text-ink-400">Type</dt>
        <dd>{{ full.job_type }}</dd>
      </div>
      <div>
        <dt class="text-xs text-ink-400">Priority</dt>
        <dd>{{ full.priority }}</dd>
      </div>
      <div>
        <dt class="text-xs text-ink-400">Attempt</dt>
        <dd>{{ full.attempt }}/{{ full.max_attempts }}</dd>
      </div>
      <div v-for="t in times" :key="t.label">
        <dt class="text-xs text-ink-400">{{ t.label }}</dt>
        <dd>{{ absTime(t.ms) }} <span class="text-ink-400">({{ relTime(t.ms) }})</span></dd>
      </div>
      <div v-if="full.cause">
        <dt class="text-xs text-ink-400">Cause</dt>
        <dd>{{ full.cause }}</dd>
      </div>
    </dl>

    <div v-if="full.last_reason">
      <h4 class="mb-1 text-xs font-medium text-ink-400">Last failure reason</h4>
      <p class="rounded border border-ink-800 bg-ink-950 px-3 py-2 text-sm">
        {{ full.last_reason }}
      </p>
    </div>

    <div v-if="customEntries.length > 0">
      <h4 class="mb-1 text-xs font-medium text-ink-400">Custom</h4>
      <table class="w-full text-left text-sm">
        <tbody>
          <tr v-for="[k, v] in customEntries" :key="k" class="border-b border-ink-800/60">
            <td class="w-48 py-1.5 pr-3 font-mono text-xs text-ink-300">{{ k }}</td>
            <td class="py-1.5 font-mono text-xs">{{ v }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div>
      <h4 class="mb-1 text-xs font-medium text-ink-400">Payload</h4>
      <PayloadView
        :encoding="full.payload.encoding"
        :size-bytes="full.payload.size_bytes"
        :data-b64="payloadB64"
        :download-name="downloadName"
      />
    </div>
  </div>
</template>
