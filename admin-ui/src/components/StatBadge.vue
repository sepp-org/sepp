<script setup lang="ts">
defineProps<{ label: string; value: number; alert?: boolean }>()
</script>

<template>
  <div class="flex min-w-14 flex-col">
    <span class="text-[10px] tracking-wider text-ink-500 uppercase">{{ label }}</span>
    <span class="text-sm tabular-nums" :class="alert && value > 0 ? 'text-red-400' : 'text-ink-100'">
      {{ value.toLocaleString() }}
    </span>
  </div>
</template>
