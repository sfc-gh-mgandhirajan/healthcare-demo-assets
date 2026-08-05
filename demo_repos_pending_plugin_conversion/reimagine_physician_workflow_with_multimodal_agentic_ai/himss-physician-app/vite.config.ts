import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  return {
    plugins: [react(), tailwindcss()],
    server: {
      proxy: {
        '/api': {
          target: `https://${env.VITE_SNOWFLAKE_ACCOUNT || 'myorg-myaccount'}.snowflakecomputing.com`,
          changeOrigin: true,
          secure: true,
        },
      },
    },
  }
})
