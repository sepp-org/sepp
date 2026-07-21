<script setup lang="ts">
import { SPARK_RANGES, useSparkRange } from '../composables/useSparkRange'

// modelValue null = follow the global range (only offered with `auto`).
const props = withDefaults(defineProps<{ modelValue: string | null; auto?: boolean }>(), {
  auto: false,
})
const emit = defineEmits<{ 'update:modelValue': [string | null] }>()

const { globalRange } = useSparkRange()

function cls(key: string): string {
  if (props.modelValue === key) return 'bg-ink-700 text-ink-100'
  // Under auto, hint which range the global picker resolves to.
  if (props.auto && props.modelValue === null && globalRange.value === key)
    return 'text-ink-300 hover:text-ink-100'
  return 'text-ink-500 hover:text-ink-200'
}
</script>

<template>
  <!-- .stop also on the container: padding clicks must not reach the card. -->
  <div
    class="flex items-center rounded border border-ink-800 bg-ink-950/60 p-0.5 text-[10px] font-medium tracking-wider uppercase"
    @click.stop
  >
    <button
      v-if="auto"
      class="rounded-sm px-1.5 py-0.5"
      :class="modelValue === null ? 'bg-ink-700 text-ink-100' : 'text-ink-500 hover:text-ink-200'"
      @click.stop="emit('update:modelValue', null)"
    >
      auto
    </button>
    <button
      v-for="r in SPARK_RANGES"
      :key="r.key"
      class="rounded-sm px-1.5 py-0.5"
      :class="cls(r.key)"
      @click.stop="emit('update:modelValue', r.key)"
    >
      {{ r.key }}
    </button>
  </div>
</template>
