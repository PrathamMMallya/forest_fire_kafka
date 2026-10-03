import { defineConfig } from 'vite'

// Vite config with dev proxy so `/api/*` routes are forwarded to local inference server
export default defineConfig({
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '')
      }
    }
  }
})