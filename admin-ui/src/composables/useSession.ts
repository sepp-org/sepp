import { computed, ref } from 'vue'
import { AdminApiError, api } from '../api/client'
import type { Role, SessionLoginResponse } from '../api/types'

const name = ref<string | null>(null)
const role = ref<Role | null>(null)
const authEnabled = ref(false)
const loaded = ref(false)

export function useSession() {
  async function refresh(): Promise<void> {
    try {
      const s = await api.session()
      name.value = s.name
      role.value = s.role
      authEnabled.value = s.auth_enabled
    } catch (e) {
      if (!(e instanceof AdminApiError && e.status === 401)) throw e
      // 401 means auth is on and we hold no session.
      name.value = null
      role.value = null
      authEnabled.value = true
    }
    // Deliberately not set when the probe itself failed (server down): the
    // router guard and App.vue re-probe until one completes, so an auth-off
    // admin is not latched into the viewer UI by a badly timed restart.
    loaded.value = true
  }

  async function login(loginName: string, key: string): Promise<SessionLoginResponse> {
    const res = await api.login(loginName, key)
    name.value = res.name
    role.value = res.role
    authEnabled.value = true
    loaded.value = true
    return res
  }

  async function logout(): Promise<void> {
    try {
      // Best-effort: the server-side session may already be expired or the
      // server unreachable; locally signing out must work regardless.
      await api.logout()
    } finally {
      name.value = null
      role.value = null
    }
  }

  function reset() {
    name.value = null
    role.value = null
    authEnabled.value = true
  }

  // Signed in, or auth is off entirely (implicit local admin).
  const authed = computed(() => !authEnabled.value || name.value !== null)
  const canOperate = computed(() => role.value === 'operator' || role.value === 'admin')
  const canAdmin = computed(() => role.value === 'admin')

  return {
    name,
    role,
    authEnabled,
    loaded,
    authed,
    canOperate,
    canAdmin,
    refresh,
    login,
    logout,
    reset,
  }
}
