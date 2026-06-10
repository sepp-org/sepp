<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { computed, ref } from 'vue'
import { AdminApiError, api } from '../api/client'
import { matchesPath } from '../lib/paths'

const queryClient = useQueryClient()
const { data: config } = useQuery({ queryKey: ['config'], queryFn: () => api.config() })

const strict = computed(() => config.value?.effective.server.strict_queues ?? false)
const pinned = computed(() =>
  matchesPath(config.value?.env_pinned ?? [], 'server.strict_queues'),
)
const error = ref('')

const { mutate: toggle, isPending } = useMutation({
  mutationFn: () =>
    api.updateConfig({
      etag: config.value!.etag,
      changes: [{ path: 'server.strict_queues', value: !strict.value }],
    }),
  onSuccess: () => {
    error.value = ''
    void queryClient.invalidateQueries({ queryKey: ['config'] })
  },
  onError: (e) => {
    error.value =
      e instanceof AdminApiError && e.status === 412
        ? 'Config changed elsewhere; retry.'
        : e instanceof Error
          ? e.message
          : 'toggle failed'
    void queryClient.invalidateQueries({ queryKey: ['config'] })
  },
})

const title = computed(() =>
  pinned.value
    ? 'server.strict_queues is pinned by an environment variable'
    : 'When on, enqueues and reserves naming undeclared queues are rejected',
)

function onClick() {
  if (!config.value || isPending.value || pinned.value) return
  toggle()
}
</script>

<template>
  <div class="flex items-center gap-2">
    <span v-if="error" class="text-xs text-red-400">{{ error }}</span>
    <button
      type="button"
      role="switch"
      :aria-checked="strict"
      :disabled="!config || isPending || pinned"
      :title="title"
      class="flex cursor-pointer items-center gap-2 disabled:cursor-default disabled:opacity-50"
      @click="onClick"
    >
      <span class="text-sm text-ink-300">Strict queues</span>
      <span
        class="relative h-5 w-9 rounded-full transition-colors"
        :class="strict ? 'bg-accent' : 'bg-ink-700'"
      >
        <span
          class="absolute top-0.5 left-0.5 size-4 rounded-full bg-ink-100 transition-transform"
          :class="strict ? 'translate-x-4' : ''"
        />
      </span>
    </button>
  </div>
</template>
