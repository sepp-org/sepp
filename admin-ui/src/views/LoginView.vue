<script setup lang="ts">
import { onMounted, ref, useTemplateRef } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { AdminApiError } from '../api/client'
import SeppMark from '../components/SeppMark.vue'
import { useSession } from '../composables/useSession'

const route = useRoute()
const router = useRouter()
const { login } = useSession()

const name = ref('')
const key = ref('')
const reveal = ref(false)
const error = ref('')
const busy = ref(false)

// The autofocus attribute only applies at document load, not when the SPA
// inserts this view after a redirect.
const nameInput = useTemplateRef('nameInput')
const keyInput = useTemplateRef('keyInput')
onMounted(() => nameInput.value?.focus())

// Internal paths only; anything else falls back to the dashboard.
function nextPath(): string {
  const next = route.query.next
  if (typeof next === 'string' && next.startsWith('/') && !next.startsWith('//')) return next
  return '/'
}

async function submit() {
  if (!name.value || !key.value || busy.value) return
  busy.value = true
  error.value = ''
  try {
    await login(name.value, key.value)
    void router.replace(nextPath())
  } catch (e) {
    error.value =
      e instanceof AdminApiError
        ? e.status === 401
          ? 'invalid name or key'
          : e.message
        : 'server unreachable — is sepp running?'
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <main class="flex min-h-screen items-center justify-center px-6">
    <div class="w-full max-w-sm">
      <div class="mb-6 flex items-center gap-2.5">
        <SeppMark :size="28" class="text-ink-100" />
        <span class="font-mono text-xl leading-none font-semibold tracking-[-0.5px] text-ink-100">
          sepp
        </span>
        <span class="mt-0.5 text-sm text-ink-400">admin</span>
      </div>

      <form
        class="rounded-lg border border-ink-800 bg-ink-900 p-5"
        @submit.prevent="submit"
      >
        <label for="admin-name" class="block text-xs text-ink-400">Name</label>
        <input
          id="admin-name"
          ref="nameInput"
          v-model="name"
          type="text"
          autocomplete="username"
          spellcheck="false"
          class="mt-1.5 w-full rounded border border-ink-700 bg-ink-950 py-2 px-3 font-mono text-sm text-ink-100 outline-none focus:border-accent"
          @keydown.enter.prevent="keyInput?.focus()"
        />

        <label for="admin-key" class="mt-4 block text-xs text-ink-400">Key</label>
        <div class="relative mt-1.5">
          <input
            id="admin-key"
            ref="keyInput"
            v-model="key"
            :type="reveal ? 'text' : 'password'"
            autocomplete="current-password"
            spellcheck="false"
            :aria-invalid="!!error"
            class="w-full rounded border border-ink-700 bg-ink-950 py-2 pr-14 pl-3 font-mono text-sm text-ink-100 outline-none focus:border-accent"
          />
          <button
            type="button"
            tabindex="-1"
            class="absolute top-1/2 right-2 -translate-y-1/2 rounded px-1.5 py-0.5 text-xs text-ink-500 hover:text-ink-300"
            @click="reveal = !reveal"
          >
            {{ reveal ? 'hide' : 'show' }}
          </button>
        </div>

        <p v-if="error" class="mt-3 text-sm text-red-400" role="alert" aria-live="polite">
          {{ error }}
        </p>

        <button
          type="submit"
          :disabled="busy || !name || !key"
          class="mt-4 flex w-full items-center justify-center gap-2.5 rounded bg-accent py-2 text-sm font-medium text-ink-950 hover:bg-accent-bright disabled:opacity-50"
        >
          <template v-if="busy">
            <span class="march flex items-end gap-[3px]" aria-hidden="true">
              <span v-for="i in 4" :key="i" :style="{ '--bar': i - 1 }" />
            </span>
            Verifying
          </template>
          <template v-else>Sign in</template>
        </button>
      </form>

      <p class="mt-4 text-xs leading-relaxed text-ink-500">
        Keys are configured in
        <code class="rounded bg-ink-900 px-1 py-0.5 font-mono text-[11px] text-ink-300"
          >sepp.toml</code
        >
        under
        <code class="rounded bg-ink-900 px-1 py-0.5 font-mono text-[11px] text-ink-300"
          >[admin]</code
        >.
      </p>
    </div>
  </main>
</template>

<style scoped>
/* Queue bars marching while a login is in flight. */
.march span {
  width: 3px;
  background: currentColor;
  border-radius: 1px;
  animation: march 0.9s ease-in-out infinite;
  animation-delay: calc(var(--bar) * 0.12s);
  height: 5px;
}

@keyframes march {
  0%,
  100% {
    height: 5px;
    opacity: 0.5;
  }
  40% {
    height: 13px;
    opacity: 1;
  }
}
</style>
