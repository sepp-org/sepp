<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { AdminApiError, api } from '../api/client'

const emit = defineEmits<{ close: [] }>()

const router = useRouter()
const queryClient = useQueryClient()
const { data: config } = useQuery({ queryKey: ['config'], queryFn: () => api.config() })
const { data: queueInfos } = useQuery({ queryKey: ['queues'], queryFn: () => api.queues() })

const name = ref('')
const error = ref('')
const notice = ref('')
const nameInput = ref<HTMLInputElement | null>(null)

const trimmed = computed(() => name.value.trim())
// Browsers collapse dot segments out of URL paths before the request is sent.
const reserved = computed(() => trimmed.value === '.' || trimmed.value === '..')
const exists = computed(() =>
  (queueInfos.value ?? []).some((q) => q.name === trimmed.value && q.declared),
)

const { mutate: create, isPending } = useMutation({
  mutationFn: (queueName: string) =>
    api.updateQueue(queueName, { etag: config.value!.etag, overrides: {} }),
  onSuccess: (res, queueName) => {
    void queryClient.invalidateQueries({ queryKey: ['queues'] })
    void queryClient.invalidateQueries({ queryKey: ['config'] })
    if (!res.applied) {
      notice.value =
        'Written to sepp.toml, but the server has not confirmed the reload; ' +
        'the queue may only appear after a restart.'
      return
    }
    emit('close')
    void router.push({ name: 'queue', params: { name: queueName, tab: 'settings' } })
  },
  onError: (e) => {
    error.value =
      e instanceof AdminApiError && e.status === 412
        ? 'Config changed elsewhere; close the dialog and retry.'
        : e instanceof Error
          ? e.message
          : 'creating the queue failed'
  },
})

function submit() {
  if (!trimmed.value || !config.value || isPending.value || exists.value || reserved.value) return
  error.value = ''
  notice.value = ''
  create(trimmed.value)
}

// Capture phase so Esc closes the dialog without also closing a drawer underneath.
function onKey(e: KeyboardEvent) {
  if (e.key === 'Escape') {
    e.stopPropagation()
    emit('close')
  }
}

onMounted(() => {
  window.addEventListener('keydown', onKey, true)
  nameInput.value?.focus()
})
onUnmounted(() => window.removeEventListener('keydown', onKey, true))
</script>

<template>
  <Teleport to="body">
    <div class="fixed inset-0 z-50 flex items-center justify-center">
      <div class="absolute inset-0 bg-black/60" @click="emit('close')" />
      <form
        class="relative w-96 rounded-lg border border-ink-700 bg-ink-900 p-5"
        @submit.prevent="submit"
      >
        <h3 class="text-sm font-semibold">New queue</h3>
        <p class="mt-1 text-sm text-ink-300">
          Declares the queue in sepp.toml. Limits can be overridden afterwards in the queue's
          Settings tab.
        </p>
        <input
          ref="nameInput"
          v-model="name"
          :disabled="isPending"
          placeholder="queue-name"
          autocomplete="off"
          spellcheck="false"
          class="mt-3 w-full rounded border border-ink-700 bg-ink-950 px-3 py-2 font-mono text-sm outline-none focus:border-accent disabled:opacity-50"
        />
        <p v-if="exists" class="mt-2 text-sm text-amber-400">This queue is already declared.</p>
        <p v-if="reserved" class="mt-2 text-sm text-amber-400">
          "." and ".." are not valid queue names.
        </p>
        <p v-if="notice" class="mt-2 text-sm text-amber-400">{{ notice }}</p>
        <p v-if="error" class="mt-2 text-sm text-red-400">{{ error }}</p>
        <div class="mt-4 flex justify-end gap-2">
          <button
            type="button"
            class="rounded border border-ink-700 px-3 py-1.5 text-sm text-ink-300 hover:text-ink-100"
            @click="emit('close')"
          >
            Cancel
          </button>
          <button
            type="submit"
            :disabled="!trimmed || exists || reserved || isPending || !config"
            class="rounded bg-accent px-3 py-1.5 text-sm font-medium text-ink-950 hover:bg-accent-bright disabled:opacity-50"
          >
            {{ isPending ? 'Creating…' : 'Create queue' }}
          </button>
        </div>
      </form>
    </div>
  </Teleport>
</template>
