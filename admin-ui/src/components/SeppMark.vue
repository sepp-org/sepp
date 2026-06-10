<script setup lang="ts">
// Mirrors docs/components/sepp-mark.tsx: the three queued bars follow the
// text color via currentColor; the bottom bar is always the brand accent.
withDefaults(defineProps<{ size?: number }>(), { size: 22 })
</script>

<template>
  <svg :width="size" :height="size" viewBox="0 0 100 100" class="block" aria-hidden="true">
    <rect x="22" y="22" width="28" height="10" rx="2" fill="currentColor" />
    <rect x="22" y="38" width="36" height="10" rx="2" fill="currentColor" />
    <rect x="22" y="54" width="44" height="10" rx="2" fill="currentColor" />
    <rect x="22" y="70" width="60" height="10" rx="2" fill="#ec6a2e" />
  </svg>
</template>
