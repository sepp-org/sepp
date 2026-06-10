<script setup lang="ts">
import { computed } from 'vue'
import type { JsonValue } from '../api/types'
import { jsonToHtml } from '../lib/jsonHighlight'

const props = defineProps<{ value: JsonValue }>()

const html = computed(() => jsonToHtml(JSON.stringify(props.value, null, 2)))
</script>

<template>
  <pre
    class="overflow-x-auto rounded border border-ink-800 bg-ink-950 p-3 font-mono text-xs leading-relaxed text-ink-200"
    v-html="html"
  ></pre>
</template>
