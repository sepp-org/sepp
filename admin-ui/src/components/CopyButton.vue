<script setup lang="ts">
import { ref } from 'vue'

const props = defineProps<{ text: string }>()

const copied = ref(false)
let timer: number | undefined

async function copy() {
  await navigator.clipboard.writeText(props.text)
  copied.value = true
  window.clearTimeout(timer)
  timer = window.setTimeout(() => (copied.value = false), 1500)
}
</script>

<template>
  <button
    type="button"
    class="text-xs"
    :class="copied ? 'text-emerald-400' : 'text-ink-400 hover:text-ink-100'"
    @click.stop="copy"
  >
    {{ copied ? 'copied' : 'copy' }}
  </button>
</template>
