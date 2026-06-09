<script setup lang="ts">
import { ref, watch } from 'vue'
import type { JsonValue } from '../api/types'

const props = defineProps<{
  path: string
  label: string
  kind: 'string' | 'number' | 'boolean' | 'string[]'
  value: JsonValue
  dirty: boolean
  envPinned: boolean
  restartOnly: boolean
  options?: string[]
}>()

const emit = defineEmits<{
  change: [value: JsonValue | null]
  revert: []
}>()

function format(v: JsonValue): string {
  if (v == null) return ''
  if (Array.isArray(v)) return v.map(String).join(', ')
  return String(v)
}

const text = ref(format(props.value))
watch(
  () => props.value,
  (v) => {
    text.value = format(v)
  },
)

const envVar =
  'SEPP_' +
  props.path
    .split('.')
    .map((s) => s.toUpperCase())
    .join('__')

function commit() {
  const raw = text.value.trim()
  if (raw === '') {
    emit('change', null)
    return
  }
  if (props.kind === 'number') {
    const n = Number(raw)
    if (!Number.isFinite(n)) {
      text.value = format(props.value)
      return
    }
    text.value = String(n)
    emit('change', n)
  } else if (props.kind === 'string[]') {
    const parts = raw
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s !== '')
    text.value = parts.join(', ')
    emit('change', parts.length > 0 ? parts : null)
  } else {
    emit('change', raw)
  }
}

function onToggle(ev: Event) {
  emit('change', (ev.target as HTMLInputElement).checked)
}

function onSelect(ev: Event) {
  emit('change', (ev.target as HTMLSelectElement).value)
}
</script>

<template>
  <div class="flex items-center gap-3 px-4 py-2">
    <div class="flex w-64 shrink-0 items-center gap-2">
      <span class="truncate font-mono text-sm text-ink-200">{{ label }}</span>
      <span
        v-if="restartOnly"
        class="rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] font-medium text-amber-400"
        title="Changing this field requires a server restart"
      >
        restart
      </span>
      <span v-if="dirty" class="size-1.5 shrink-0 rounded-full bg-accent" title="Unsaved change"></span>
    </div>
    <div class="flex min-w-0 flex-1 items-center gap-2">
      <input
        v-if="kind === 'boolean'"
        type="checkbox"
        :checked="value === true"
        :disabled="envPinned"
        class="size-4 accent-accent disabled:opacity-50"
        @change="onToggle"
      />
      <select
        v-else-if="options"
        :value="format(value)"
        :disabled="envPinned"
        class="rounded border border-ink-700 bg-ink-950 px-2 py-1 text-sm outline-none focus:border-accent disabled:opacity-50"
        @change="onSelect"
      >
        <option v-for="o in options" :key="o" :value="o">{{ o }}</option>
      </select>
      <input
        v-else
        v-model="text"
        :type="kind === 'number' ? 'number' : 'text'"
        step="any"
        :disabled="envPinned"
        placeholder="default"
        class="w-full max-w-md rounded border border-ink-700 bg-ink-950 px-2 py-1 font-mono text-sm outline-none focus:border-accent disabled:opacity-50"
        @change="commit"
        @keydown.enter="($event.target as HTMLInputElement).blur()"
      />
      <span
        v-if="envPinned"
        class="cursor-help text-ink-500"
        :title="`Pinned by ${envVar}; unset the environment variable to edit`"
      >
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" fill="currentColor" class="size-3.5">
          <path
            fill-rule="evenodd"
            d="M8 1a3.5 3.5 0 0 0-3.5 3.5V7A1.5 1.5 0 0 0 3 8.5v5A1.5 1.5 0 0 0 4.5 15h7a1.5 1.5 0 0 0 1.5-1.5v-5A1.5 1.5 0 0 0 11.5 7V4.5A3.5 3.5 0 0 0 8 1Zm2 6V4.5a2 2 0 1 0-4 0V7h4Z"
            clip-rule="evenodd"
          />
        </svg>
      </span>
      <button v-if="dirty" class="text-xs text-ink-400 hover:text-ink-100" @click="emit('revert')">
        revert
      </button>
    </div>
  </div>
</template>
