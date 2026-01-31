import { defineConfig } from 'vite';

export default defineConfig(({ command }) => ({
  define: {
    'process.env': {},
  },
  esbuild: {
    jsx: 'automatic',
    jsxImportSource: '@askrjs/askr',
  },
  server: {
    port: 5173,
    open: true,
    proxy: {
      // Proxy API calls to Fitz backend
      '/api': {
        target: 'http://localhost:4090',
        changeOrigin: true,
      },
      '/metrics': {
        target: 'http://localhost:4090',
        changeOrigin: true,
      },
      '/healthz': {
        target: 'http://localhost:4090',
        changeOrigin: true,
      },
      '/readyz': {
        target: 'http://localhost:4090',
        changeOrigin: true,
      },
      '/startupz': {
        target: 'http://localhost:4090',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://localhost:4090',
        ws: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
}));
