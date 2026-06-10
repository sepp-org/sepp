<script setup lang="ts">
import { ref } from 'vue'

const props = withDefaults(
  defineProps<{
    modelValue: string[]
    disabled?: boolean
    placeholder?: string
    id?: string
  }>(),
  { disabled: false, placeholder: '', id: undefined },
)

const emit = defineEmits<{ 'update:modelValue': [value: string[]] }>()

const draft = ref('')
const input = ref<HTMLInputElement | null>(null)

// Pasted comma-separated text becomes several pills at once; duplicates fold.
function commitDraft() {
  const parts = draft.value
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s !== '')
  draft.value = ''
  if (parts.length === 0) return
  const next = [...props.modelValue]
  for (const p of parts) if (!next.includes(p)) next.push(p)
  if (next.length !== props.modelValue.length) emit('update:modelValue', next)
}

function remove(i: number) {
  if (props.disabled) return
  const next = [...props.modelValue]
  next.splice(i, 1)
  emit('update:modelValue', next)
}

function onKeydown(e: KeyboardEvent) {
  // IME composition: v-model holds the pre-composition draft, so acting on
  // Enter/Backspace here would commit stale text or eat a pill mid-edit.
  // keyCode 229 covers Safari's post-compositionend candidate-confirm Enter.
  if (e.isComposing || e.keyCode === 229) return
  if (e.key === 'Enter' || e.key === ',') {
    e.preventDefault()
    commitDraft()
  } else if (e.key === 'Backspace' && draft.value === '' && props.modelValue.length > 0) {
    remove(props.modelValue.length - 1)
  }
}
</script>

<template>
  <div
    class="flex min-h-[30px] w-full max-w-md min-w-0 flex-wrap items-center gap-1 rounded border border-ink-700 bg-ink-950 px-1.5 py-1 focus-within:border-accent"
    :class="disabled ? 'opacity-50' : 'cursor-text'"
    @click="input?.focus()"
  >
    <span
      v-for="(tag, i) in modelValue"
      :key="`${tag}-${i}`"
      class="flex max-w-full min-w-0 items-center gap-1 rounded-full bg-ink-800 px-2 py-0.5 font-mono text-xs text-ink-200"
    >
      <span class="truncate" :title="tag">{{ tag }}</span>
      <button
        v-if="!disabled"
        type="button"
        class="shrink-0 text-ink-400 hover:text-red-400"
        :aria-label="`Remove ${tag}`"
        @click.stop="remove(i)"
      >
        &times;
      </button>
    </span>
    <input
      :id="id"
      ref="input"
      v-model="draft"
      :disabled="disabled"
      :placeholder="modelValue.length === 0 ? placeholder : ''"
      class="min-w-20 flex-1 bg-transparent py-0.5 font-mono text-sm outline-none placeholder:text-ink-600"
      @keydown="onKeydown"
      @blur="commitDraft"
    />
  </div>
</template>
