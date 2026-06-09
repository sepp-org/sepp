<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { AdminApiError } from '../api/client'
import { useSession } from '../composables/useSession'

const router = useRouter()
const { name, authEnabled, refresh, login } = useSession()

const key = ref('')
const error = ref('')
const busy = ref(false)

onMounted(async () => {
  try {
    await refresh()
  } catch {
    return
  }
  if (!authEnabled.value || name.value) void router.replace('/')
})

async function submit() {
  if (!key.value || busy.value) return
  busy.value = true
  error.value = ''
  try {
    await login(key.value)
    void router.replace('/')
  } catch (e) {
    error.value = e instanceof AdminApiError ? e.message : 'login failed'
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <div class="flex min-h-screen items-center justify-center">
    <form
      class="flex w-80 flex-col gap-4 rounded-lg border border-ink-800 bg-ink-900 p-6"
      @submit.prevent="submit"
    >
      <h1 class="text-lg font-semibold"><span class="text-accent">sepp</span> admin</h1>
      <input
        v-model="key"
        type="password"
        placeholder="Admin key"
        autocomplete="off"
        class="rounded border border-ink-700 bg-ink-950 px-3 py-2 text-sm outline-none focus:border-accent"
      />
      <p v-if="error" class="text-sm text-red-400">{{ error }}</p>
      <button
        type="submit"
        :disabled="busy || !key"
        class="rounded bg-accent px-3 py-2 text-sm font-medium text-ink-950 hover:bg-accent-bright disabled:opacity-50"
      >
        Sign in
      </button>
    </form>
  </div>
</template>
