# sepp admin UI

The built-in admin web UI, built with [Vue 3](https://vuejs.org), [Vite](https://vite.dev) and [Tailwind CSS](https://tailwindcss.com). The production build is embedded into the sepp binary at compile time.

## Getting started

```bash
npm install
npm run dev
```

The dev server proxies `/admin/api` to `http://127.0.0.1:9465`. Make sure to start a local sepp server first so the API is available.
