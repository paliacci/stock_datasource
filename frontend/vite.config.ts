import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src')
    }
  },
  server: {
    host: '0.0.0.0', // 允许外部IP访问
    port: 3008,
    strictPort: false, // 允许使用其他端口
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8008',
        changeOrigin: true,
        rewrite: (path) => {
          console.log('Proxy rewrite:', path)
          return path
        }
      }
    }
  }
})
