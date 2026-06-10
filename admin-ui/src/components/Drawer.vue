<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'

const emit = defineEmits<{ close: [] }>()

const DEFAULT_WIDTH = 768 // the old fixed max-w-3xl
const MIN_WIDTH = 420
const STORAGE_KEY = 'sepp-admin-drawer-width'

function clamp(w: number): number {
  return Math.min(Math.max(w, MIN_WIDTH), Math.max(MIN_WIDTH, window.innerWidth - 80))
}

const stored = Number(localStorage.getItem(STORAGE_KEY))
const width = ref(Number.isFinite(stored) && stored > 0 ? clamp(stored) : DEFAULT_WIDTH)
const dragging = ref(false)

function onKey(e: KeyboardEvent) {
  if (e.key === 'Escape') emit('close')
}

// Pointer capture keeps move events flowing to the handle for the whole drag,
// even when the cursor leaves it (or the window, on most platforms).
function startResize(e: PointerEvent) {
  dragging.value = true
  ;(e.target as HTMLElement).setPointerCapture(e.pointerId)
  e.preventDefault()
}

function onResize(e: PointerEvent) {
  if (!dragging.value) return
  width.value = clamp(window.innerWidth - e.clientX)
}

function endResize() {
  if (!dragging.value) return
  dragging.value = false
  localStorage.setItem(STORAGE_KEY, String(Math.round(width.value)))
}

function resetWidth() {
  width.value = DEFAULT_WIDTH
  localStorage.removeItem(STORAGE_KEY)
}

onMounted(() => window.addEventListener('keydown', onKey))
onUnmounted(() => window.removeEventListener('keydown', onKey))
</script>

<template>
  <Teleport to="body">
    <div class="fixed inset-0 z-40">
      <div class="absolute inset-0 bg-black/50" @click="emit('close')" />
      <div
        class="absolute inset-y-0 right-0 flex max-w-[calc(100vw-1rem)] flex-col border-l border-ink-800 bg-ink-900 shadow-xl"
        :class="dragging ? 'select-none' : ''"
        :style="{ width: `${width}px` }"
      >
        <div
          class="absolute inset-y-0 -left-1 z-10 w-2.5 cursor-col-resize touch-none transition-colors"
          :class="dragging ? 'bg-accent/40' : 'hover:bg-accent/25'"
          title="Drag to resize; double-click to reset"
          @pointerdown="startResize"
          @pointermove="onResize"
          @pointerup="endResize"
          @pointercancel="endResize"
          @dblclick="resetWidth"
        />
        <slot />
      </div>
    </div>
  </Teleport>
</template>
