<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'
import { AdminApiError, api } from '../../api/client'
import { useStatsStream } from '../../composables/useStatsStream'

const props = defineProps<{ queue: string }>()

const router = useRouter()
const queryClient = useQueryClient()
const { frame } = useStatsStream()

const inflight = computed(() => frame.value?.queues[props.queue]?.inflight ?? 0)
const blocked = computed(() => inflight.value > 0)

const { data: config } = useQuery({ queryKey: ['config'], queryFn: () => api.config() })

const confirmText = ref('')
const error = ref('')

const { mutate: del, isPending: deleting } = useMutation({
  mutationFn: (purge: boolean) => {
    const etag = config.value?.etag
    if (!etag) return Promise.reject(new Error('config not loaded yet'))
    return api.deleteQueue(props.queue, etag, purge)
  },
  onSuccess: () => {
    void queryClient.invalidateQueries({ queryKey: ['queues'] })
    void queryClient.invalidateQueries({ queryKey: ['config'] })
    void router.push('/')
  },
  onError: (e) => {
    if (e instanceof AdminApiError && e.code === 'inflight') {
      error.value = 'Queue has in-flight jobs; wait for them to complete or their leases to expire.'
    } else if (e instanceof AdminApiError && e.code === 'not_empty') {
      error.value = 'Queue still has jobs; use "Purge and delete" to remove them first.'
    } else if (e instanceof AdminApiError && e.status === 412) {
      error.value = 'Config changed elsewhere; reload and retry.'
    } else {
      error.value = e.message
    }
  },
})

const canAct = computed(
  () => confirmText.value === props.queue && !blocked.value && !!config.value && !deleting.value,
)

function run(purge: boolean) {
  if (!canAct.value) return
  error.value = ''
  del(purge)
}
</script>

<template>
  <div class="flex max-w-xl flex-col gap-4">
    <div class="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-400">
      Deleting a queue removes its declaration from sepp.toml. Purging permanently deletes all
      ready, scheduled and dead-lettered jobs.
    </div>

    <div
      v-if="blocked"
      class="rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-sm text-amber-300"
    >
      {{ inflight }} job{{ inflight === 1 ? '' : 's' }} in flight. Purge and delete are disabled
      until they complete or their leases expire.
    </div>

    <div class="flex flex-col gap-1">
      <label class="text-xs text-ink-400" for="danger-confirm">
        Type <span class="font-mono text-ink-200">{{ queue }}</span> to confirm
      </label>
      <input
        id="danger-confirm"
        v-model="confirmText"
        :placeholder="queue"
        autocomplete="off"
        class="rounded border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm outline-none focus:border-accent"
      />
    </div>

    <div class="flex gap-2">
      <button
        class="rounded border border-red-500/40 px-3 py-1.5 text-sm text-red-400 hover:bg-red-500/10 disabled:opacity-50"
        :disabled="!canAct"
        @click="run(false)"
      >
        Delete queue
      </button>
      <button
        class="rounded bg-red-500/80 px-3 py-1.5 text-sm font-medium text-white hover:bg-red-500 disabled:opacity-50"
        :disabled="!canAct"
        @click="run(true)"
      >
        Purge and delete
      </button>
    </div>
    <p class="text-xs text-ink-500">
      "Delete queue" fails if jobs remain; "Purge and delete" removes them first.
    </p>

    <p v-if="error" class="text-sm text-red-400">{{ error }}</p>
  </div>
</template>
