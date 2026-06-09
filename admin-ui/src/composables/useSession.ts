import { ref } from 'vue'
import { AdminApiError, api } from '../api/client'
import type { SessionLoginResponse } from '../api/types'

const name = ref<string | null>(null)
const authEnabled = ref(false)
const loaded = ref(false)

export function useSession() {
  async function refresh(): Promise<void> {
    try {
      const s = await api.session()
      name.value = s.name
      authEnabled.value = s.auth_enabled
    } catch (e) {
      if (!(e instanceof AdminApiError && e.status === 401)) throw e
      name.value = null
      authEnabled.value = true
    } finally {
      loaded.value = true
    }
  }

  async function login(key: string): Promise<SessionLoginResponse> {
    const res = await api.login(key)
    name.value = res.name
    authEnabled.value = true
    loaded.value = true
    return res
  }

  async function logout(): Promise<void> {
    await api.logout()
    name.value = null
  }

  return { name, authEnabled, loaded, refresh, login, logout }
}
