import { createRouter, createWebHistory } from 'vue-router'

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
  ],
})

export default router
