import tailwindcss from '@tailwindcss/vite'
import vue from '@vitejs/plugin-vue'
import { defineConfig } from 'vite'

export default defineConfig({
  base: '/admin/',
  plugins: [vue(), tailwindcss()],
  server: {
    proxy: {
      '/admin/api': 'http://127.0.0.1:9465',
    },
  },
})
