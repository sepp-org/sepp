<script setup lang="ts">
import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/vue-query'
import { computed, ref, watch } from 'vue'
import { api } from '../../api/client'
import type { JobState, JobSummary } from '../../api/types'
import ConfirmDialog from '../../components/ConfirmDialog.vue'
import CopyButton from '../../components/CopyButton.vue'
import { useStatsStream } from '../../composables/useStatsStream'
import JobDetail from './JobDetail.vue'

const props = defineProps<{ queue: string }>()

const MAX_KEYS_PER_ACTION = 100

const states: { id: JobState; label: string }[] = [
  { id: 'ready', label: 'Ready' },
  { id: 'scheduled', label: 'Scheduled' },
  { id: 'inflight', label: 'In-flight' },
  { id: 'dead_letter', label: 'Dead letters' },
]

const state = ref<JobState>('ready')
const selected = ref<JobSummary | null>(null)
const checked = ref(new Set<string>())
const confirmAction = ref<'requeue' | 'delete' | 'dead_letter' | null>(null)
const actionNotice = ref('')

const { server } = useStatsStream()
const isDl = computed(() => state.value === 'dead_letter')
// In-flight jobs are excluded: a worker holds their lease.
const canDeadLetter = computed(() => state.value === 'ready' || state.value === 'scheduled')
const selectable = computed(() => isDl.value || canDeadLetter.value)
const retentionOff = computed(() => server.value?.dead_letter_retention_ms === 0)
const retentionDisabled = computed(() => isDl.value && retentionOff.value)

const queryClient = useQueryClient()
const { data, error, isPending, hasNextPage, isFetchingNextPage, fetchNextPage } =
  useInfiniteQuery({
    queryKey: computed(() => ['jobs', props.queue, state.value]),
    queryFn: ({ pageParam }) => api.jobs(props.queue, state.value, pageParam ?? undefined, 50),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.next_cursor,
    enabled: computed(() => !retentionDisabled.value),
  })

const jobs = computed(() => data.value?.pages.flatMap((p) => p.jobs) ?? [])

watch(state, () => {
  selected.value = null
  checked.value = new Set()
  actionNotice.value = ''
})

const allChecked = computed(
  () => jobs.value.length > 0 && jobs.value.every((j) => checked.value.has(j.key_b64)),
)

function toggle(key: string) {
  if (checked.value.has(key)) checked.value.delete(key)
  else checked.value.add(key)
}

function toggleAll() {
  checked.value = allChecked.value ? new Set() : new Set(jobs.value.map((j) => j.key_b64))
}

function afterDlAction() {
  checked.value = new Set()
  confirmAction.value = null
  void queryClient.invalidateQueries({ queryKey: ['jobs', props.queue] })
}

const {
  mutate: requeue,
  isPending: requeueing,
  error: requeueError,
} = useMutation({
  mutationFn: (keys: string[]) => api.requeueDeadLetters(props.queue, keys),
  onSuccess: (res) => {
    actionNotice.value =
      `Requeued ${res.requeued} job${res.requeued === 1 ? '' : 's'}` +
      (res.missing > 0 ? `; ${res.missing} already gone` : '')
    afterDlAction()
  },
  onError: () => {
    confirmAction.value = null
  },
})

const {
  mutate: remove,
  isPending: removing,
  error: removeError,
} = useMutation({
  mutationFn: (keys: string[]) => api.deleteDeadLetters(props.queue, keys),
  onSuccess: (res) => {
    actionNotice.value =
      `Deleted ${res.deleted} job${res.deleted === 1 ? '' : 's'}` +
      (res.missing > 0 ? `; ${res.missing} already gone` : '')
    afterDlAction()
  },
  onError: () => {
    confirmAction.value = null
  },
})

const {
  mutate: deadLetter,
  isPending: deadLettering,
  error: deadLetterError,
} = useMutation({
  mutationFn: (keys: string[]) =>
    api.deadLetterJobs(props.queue, state.value as 'ready' | 'scheduled', keys),
  onSuccess: (res) => {
    const n = res.dead_lettered
    actionNotice.value =
      (retentionOff.value ? `Dropped ${n} job${n === 1 ? '' : 's'}` : `Dead-lettered ${n} job${n === 1 ? '' : 's'}`) +
      (res.missing > 0 ? `; ${res.missing} already gone` : '')
    afterDlAction()
  },
  onError: () => {
    confirmAction.value = null
  },
})

const actionBusy = computed(() => requeueing.value || removing.value || deadLettering.value)
const actionError = computed(
  () => requeueError.value ?? removeError.value ?? deadLetterError.value,
)
const actionCount = computed(() => Math.min(checked.value.size, MAX_KEYS_PER_ACTION))

function runAction() {
  const keys = [...checked.value].slice(0, MAX_KEYS_PER_ACTION)
  if (keys.length === 0) return
  actionNotice.value = ''
  if (confirmAction.value === 'requeue') requeue(keys)
  else if (confirmAction.value === 'delete') remove(keys)
  else if (confirmAction.value === 'dead_letter') deadLetter(keys)
}

function onDetailGone(notice: string) {
  selected.value = null
  checked.value = new Set()
  actionNotice.value = notice
}

const timeHeader = computed(() =>
  state.value === 'dead_letter' ? 'Failed' : state.value === 'scheduled' ? 'Scheduled' : 'Enqueued',
)

function jobTime(job: JobSummary): string {
  const ms =
    state.value === 'dead_letter'
      ? job.failed_at_ms
      : state.value === 'scheduled'
        ? job.scheduled_at_ms
        : job.enqueued_at_ms
  return ms === undefined ? '' : relTime(ms)
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
  <div v-if="selected" class="flex flex-col gap-3">
    <div>
      <button class="text-sm text-ink-400 hover:text-ink-100" @click="selected = null">
        &larr; Back to list
      </button>
    </div>
    <JobDetail :queue="queue" :state="state" :job="selected" @gone="onDetailGone" />
  </div>
  <div v-else class="flex flex-col gap-3">
    <div class="flex gap-1">
      <button
        v-for="s in states"
        :key="s.id"
        class="rounded px-2.5 py-1 text-sm"
        :class="s.id === state ? 'bg-ink-800 text-ink-100' : 'text-ink-400 hover:text-ink-100'"
        @click="state = s.id"
      >
        {{ s.label }}
      </button>
    </div>

    <div
      v-if="retentionDisabled"
      class="rounded border border-ink-800 px-4 py-6 text-center text-sm text-ink-400"
    >
      Dead-letter retention is disabled
      (<span class="font-mono">dead_letter_retention_ms = 0</span>); failed jobs are dropped
      instead of kept here.
    </div>
    <template v-else>
      <div v-if="selectable && jobs.length > 0" class="flex items-center gap-2">
        <span class="text-xs text-ink-400">{{ checked.size }} selected</span>
        <template v-if="isDl">
          <button
            class="rounded border border-ink-700 px-2.5 py-1 text-sm text-ink-300 hover:text-ink-100 disabled:opacity-50"
            :disabled="checked.size === 0 || actionBusy"
            @click="confirmAction = 'requeue'"
          >
            Requeue
          </button>
          <button
            class="rounded border border-red-500/40 px-2.5 py-1 text-sm text-red-400 hover:bg-red-500/10 disabled:opacity-50"
            :disabled="checked.size === 0 || actionBusy"
            @click="confirmAction = 'delete'"
          >
            Delete
          </button>
        </template>
        <button
          v-else
          class="rounded border border-red-500/40 px-2.5 py-1 text-sm text-red-400 hover:bg-red-500/10 disabled:opacity-50"
          :disabled="checked.size === 0 || actionBusy"
          @click="confirmAction = 'dead_letter'"
        >
          Dead-letter
        </button>
      </div>

      <p v-if="actionNotice" class="text-sm text-emerald-400">{{ actionNotice }}</p>
      <p v-if="actionError" class="text-sm text-red-400">{{ actionError.message }}</p>
      <p v-if="error" class="text-sm text-red-400">{{ error.message }}</p>

      <p v-if="isPending" class="text-sm text-ink-400">Loading jobs…</p>
      <!-- Fixed layout: the sized columns stay compact and the unsized ones
           (ID, type, last reason) absorb whatever width the drawer has. -->
      <table v-else-if="jobs.length > 0" class="w-full table-fixed text-left text-sm">
        <thead>
          <tr class="border-b border-ink-800 text-xs text-ink-400">
            <th v-if="selectable" class="w-8 py-2">
              <input type="checkbox" class="accent-accent" :checked="allChecked" @change="toggleAll" />
            </th>
            <th class="py-2 pr-3 font-medium">ID</th>
            <th class="py-2 pr-3 font-medium">Type</th>
            <th class="w-14 py-2 pr-3 font-medium">Priority</th>
            <th class="w-16 py-2 pr-3 font-medium">Attempt</th>
            <th class="w-28 py-2 pr-3 font-medium">{{ timeHeader }}</th>
            <th v-if="isDl" class="w-28 py-2 pr-3 font-medium">Cause</th>
            <th v-if="isDl" class="py-2 font-medium">Last reason</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="job in jobs"
            :key="job.key_b64"
            class="cursor-pointer border-b border-ink-800/60 hover:bg-ink-800/40"
            @click="selected = job"
          >
            <td v-if="selectable" class="py-2" @click.stop>
              <input
                type="checkbox"
                class="accent-accent"
                :checked="checked.has(job.key_b64)"
                @change="toggle(job.key_b64)"
              />
            </td>
            <td class="py-2 pr-3">
              <div class="flex items-center gap-1">
                <span class="min-w-0 truncate font-mono text-xs" :title="job.id">
                  {{ job.id }}
                </span>
                <CopyButton :text="job.id" />
              </div>
            </td>
            <td class="truncate py-2 pr-3" :title="job.job_type">{{ job.job_type }}</td>
            <td class="py-2 pr-3">{{ job.priority }}</td>
            <td class="py-2 pr-3">{{ job.attempt }}/{{ job.max_attempts }}</td>
            <td class="truncate py-2 pr-3 text-ink-300">{{ jobTime(job) }}</td>
            <td v-if="isDl" class="truncate py-2 pr-3" :title="job.cause">{{ job.cause ?? '' }}</td>
            <td v-if="isDl" class="truncate py-2 text-ink-300" :title="job.last_reason">
              {{ job.last_reason ?? '' }}
            </td>
          </tr>
        </tbody>
      </table>
      <p
        v-else-if="!error"
        class="rounded border border-ink-800 px-4 py-6 text-center text-sm text-ink-400"
      >
        No {{ isDl ? 'dead-lettered' : state }} jobs.
      </p>

      <div v-if="hasNextPage">
        <button
          class="rounded border border-ink-700 px-3 py-1.5 text-sm text-ink-300 hover:text-ink-100 disabled:opacity-50"
          :disabled="isFetchingNextPage"
          @click="fetchNextPage()"
        >
          {{ isFetchingNextPage ? 'Loading…' : 'Load more' }}
        </button>
      </div>
    </template>
  </div>

  <ConfirmDialog
    v-if="confirmAction"
    :title="
      confirmAction === 'requeue'
        ? 'Requeue dead letters'
        : confirmAction === 'delete'
          ? 'Delete dead letters'
          : 'Dead-letter jobs'
    "
    :confirm-label="
      confirmAction === 'requeue' ? 'Requeue' : confirmAction === 'delete' ? 'Delete' : 'Dead-letter'
    "
    :danger="confirmAction !== 'requeue'"
    :busy="actionBusy"
    @confirm="runAction"
    @cancel="confirmAction = null"
  >
    <p v-if="confirmAction === 'requeue'">
      Requeue {{ actionCount }} dead-lettered job{{ actionCount === 1 ? '' : 's' }} back to ready
      with the attempt counter reset?
    </p>
    <p v-else-if="confirmAction === 'delete'">
      Permanently delete {{ actionCount }} dead-lettered job{{ actionCount === 1 ? '' : 's' }}?
      This cannot be undone.
    </p>
    <template v-else>
      <p>
        Move {{ actionCount }} {{ state }} job{{ actionCount === 1 ? '' : 's' }} to the
        dead-letter queue? Workers will never see {{ actionCount === 1 ? 'it' : 'them' }}.
      </p>
      <p v-if="retentionOff" class="mt-1 text-amber-400">
        Dead-letter retention is disabled
        (<span class="font-mono">dead_letter_retention_ms = 0</span>), so
        {{ actionCount === 1 ? 'this job is' : 'these jobs are' }} dropped permanently instead of
        kept for replay.
      </p>
    </template>
    <p v-if="checked.size > MAX_KEYS_PER_ACTION" class="mt-1 text-xs text-ink-500">
      Capped at {{ MAX_KEYS_PER_ACTION }} per request; repeat for the rest.
    </p>
  </ConfirmDialog>
</template>
