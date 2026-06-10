<script setup lang="ts">
defineProps<{ label: string; value: number; alert?: boolean }>()
</script>

<template>
  <div class="flex min-w-16 flex-col">
    <span
      class="text-2xl leading-7 font-semibold tabular-nums"
      :class="alert && value > 0 ? 'text-red-400' : 'text-ink-100'"
    >
      {{ value.toLocaleString() }}
    </span>
    <span class="mt-1 text-[10px] tracking-wider text-ink-500 uppercase">{{ label }}</span>
  </div>
</template>
