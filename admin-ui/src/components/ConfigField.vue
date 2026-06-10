<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { JsonValue } from '../api/types'
import TagInput from './TagInput.vue'

const props = defineProps<{
  path: string
  label: string
  kind: 'string' | 'number' | 'boolean' | 'string[]'
  value: JsonValue
  dirty: boolean
  envPinned: boolean
  restartOnly: boolean
  pendingRestart?: boolean
  options?: string[]
  generate?: boolean
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

const tags = computed(() => (Array.isArray(props.value) ? props.value.map(String) : []))

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
  } else {
    emit('change', raw)
  }
}

function onTags(next: string[]) {
  // Zero pills means "unset, fall back to the default", mirroring the empty
  // text input on scalar fields.
  emit('change', next.length > 0 ? next : null)
}

const copied = ref(false)

function generateSecret() {
  const bytes = new Uint8Array(32)
  crypto.getRandomValues(bytes)
  const b64 = btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '')
  const key = `sepp_${b64}`
  emit('change', [...tags.value, key])
  navigator.clipboard
    ?.writeText(key)
    .then(() => {
      copied.value = true
      setTimeout(() => (copied.value = false), 1500)
    })
    .catch(() => {})
}
</script>

<template>
  <div class="flex items-center gap-3 px-4 py-2">
    <div class="flex w-64 shrink-0 items-center gap-2">
      <span class="truncate font-mono text-sm text-ink-200">{{ label }}</span>
      <span
        v-if="pendingRestart"
        class="rounded bg-amber-500/30 px-1.5 py-0.5 text-[10px] font-medium text-amber-300"
        title="Changed on disk since the server started; the running value applies until a restart"
      >
        restart pending
      </span>
      <span
        v-else-if="restartOnly"
        class="rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] font-medium text-amber-400"
        title="Changing this field requires a server restart"
      >
        restart
      </span>
      <span v-if="dirty" class="size-1.5 shrink-0 rounded-full bg-accent" title="Unsaved change"></span>
    </div>
    <div class="flex min-w-0 flex-1 items-center gap-2">
      <button
        v-if="kind === 'boolean'"
        type="button"
        role="switch"
        :aria-checked="value === true"
        :disabled="envPinned"
        class="relative h-5 w-9 shrink-0 rounded-full transition-colors disabled:opacity-50"
        :class="value === true ? 'bg-accent' : 'bg-ink-700'"
        @click="emit('change', value !== true)"
      >
        <span
          class="absolute top-0.5 left-0.5 size-4 rounded-full bg-ink-100 transition-transform"
          :class="value === true ? 'translate-x-4' : ''"
        />
      </button>
      <div
        v-else-if="options"
        class="flex overflow-hidden rounded border border-ink-700"
        :class="envPinned ? 'opacity-50' : ''"
      >
        <button
          v-for="o in options"
          :key="o"
          type="button"
          :disabled="envPinned"
          class="border-l border-ink-700 px-2.5 py-1 font-mono text-sm transition-colors first:border-l-0"
          :class="
            o === format(value)
              ? 'bg-ink-700 text-ink-100'
              : 'bg-ink-950 text-ink-400 hover:text-ink-100'
          "
          @click="emit('change', o)"
        >
          {{ o }}
        </button>
      </div>
      <template v-else-if="kind === 'string[]'">
        <TagInput
          :model-value="tags"
          :disabled="envPinned"
          :placeholder="Array.isArray(value) ? 'reject all (empty list in sepp.toml)' : 'default'"
          @update:model-value="onTags"
        />
        <button
          v-if="generate"
          type="button"
          class="shrink-0 rounded border border-ink-700 px-2 py-1 text-xs text-ink-300 hover:text-ink-100 disabled:opacity-50"
          :disabled="envPinned"
          title="Generate a random key, add it to the list, and copy it to the clipboard"
          @click="generateSecret"
        >
          {{ copied ? 'Copied!' : 'Generate' }}
        </button>
      </template>
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
        class="shrink-0 cursor-help text-ink-500"
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
      <button
        v-if="dirty"
        class="shrink-0 text-xs text-ink-400 hover:text-ink-100"
        @click="emit('revert')"
      >
        revert
      </button>
    </div>
  </div>
</template>
