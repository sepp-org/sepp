<script setup lang="ts">
import { computed } from 'vue'
import type { JsonValue } from '../api/types'

const props = defineProps<{ value: JsonValue }>()

const text = computed(() => JSON.stringify(props.value, null, 2))
</script>

<template>
  <pre
    class="overflow-x-auto rounded border border-ink-800 bg-ink-950 p-3 font-mono text-xs leading-relaxed text-ink-200"
  >{{ text }}</pre>
</template>
