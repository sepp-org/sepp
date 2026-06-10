import { createRouter, createWebHistory } from 'vue-router'
import { useSession } from './composables/useSession'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', name: 'dashboard', component: () => import('./views/DashboardView.vue') },
    {
      path: '/queues/:name/:tab?',
      name: 'queue',
      component: () => import('./views/DashboardView.vue'),
      meta: { drawer: true },
    },
    { path: '/config', name: 'config', component: () => import('./views/ConfigView.vue') },
    { path: '/login', name: 'login', component: () => import('./views/LoginView.vue') },
  ],
})

router.beforeEach(async (to) => {
  const { loaded, authed, refresh } = useSession()
  if (!loaded.value) {
    // A failed probe (server down) leaves `loaded` false and falls through
    // as auth-disabled: the dashboard renders offline, and the session is
    // re-probed on the next navigation or stream recovery (App.vue) instead
    // of wedging navigation here.
    await refresh().catch(() => {})
  }
  if (to.name === 'login') {
    return authed.value ? '/' : true
  }
  if (!authed.value) {
    return { name: 'login', query: to.fullPath !== '/' ? { next: to.fullPath } : {} }
  }
  return true
})

export default router
