<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'

const emit = defineEmits<{ close: [] }>()

function onKey(e: KeyboardEvent) {
  if (e.key === 'Escape') emit('close')
}

onMounted(() => window.addEventListener('keydown', onKey))
onUnmounted(() => window.removeEventListener('keydown', onKey))
</script>

<template>
  <Teleport to="body">
    <div class="fixed inset-0 z-40">
      <div class="absolute inset-0 bg-black/50" @click="emit('close')" />
      <div
        class="absolute inset-y-0 right-0 flex w-full max-w-3xl flex-col border-l border-ink-800 bg-ink-900 shadow-xl"
      >
        <slot />
      </div>
    </div>
  </Teleport>
</template>
