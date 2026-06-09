<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'

defineProps<{
  title: string
  confirmLabel?: string
  danger?: boolean
  busy?: boolean
}>()

const emit = defineEmits<{ confirm: []; cancel: [] }>()

// Capture phase so Esc cancels the dialog without also closing the drawer underneath.
function onKey(e: KeyboardEvent) {
  if (e.key === 'Escape') {
    e.stopPropagation()
    emit('cancel')
  }
}

onMounted(() => window.addEventListener('keydown', onKey, true))
onUnmounted(() => window.removeEventListener('keydown', onKey, true))
</script>

<template>
  <Teleport to="body">
    <div class="fixed inset-0 z-50 flex items-center justify-center">
      <div class="absolute inset-0 bg-black/60" @click="emit('cancel')" />
      <div class="relative w-96 rounded-lg border border-ink-700 bg-ink-900 p-5">
        <h3 class="text-sm font-semibold">{{ title }}</h3>
        <div class="mt-2 text-sm text-ink-300"><slot /></div>
        <div class="mt-4 flex justify-end gap-2">
          <button
            class="rounded border border-ink-700 px-3 py-1.5 text-sm text-ink-300 hover:text-ink-100"
            @click="emit('cancel')"
          >
            Cancel
          </button>
          <button
            class="rounded px-3 py-1.5 text-sm font-medium disabled:opacity-50"
            :class="
              danger
                ? 'bg-red-500/80 text-white hover:bg-red-500'
                : 'bg-accent text-ink-950 hover:bg-accent-bright'
            "
            :disabled="busy"
            @click="emit('confirm')"
          >
            {{ confirmLabel ?? 'Confirm' }}
          </button>
        </div>
      </div>
    </div>
  </Teleport>
</template>
