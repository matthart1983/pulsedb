/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        'pulse-base': '#0a0e17',
        'pulse-surface': '#111827',
        'pulse-elevated': '#1a2332',
        'pulse-overlay': '#1f2937',
        'pulse-border': '#1e293b',
        'pulse-focus': '#3b82f6',
        'pulse-text': '#e2e8f0',
        'pulse-text-secondary': '#94a3b8',
        'pulse-text-muted': '#64748b',
        'chart-blue': '#3b82f6',
        'chart-cyan': '#06b6d4',
        'chart-green': '#10b981',
        'chart-red': '#ef4444',
        'chart-amber': '#f59e0b',
        'chart-purple': '#8b5cf6',
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'Cascadia Code', 'monospace'],
        sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
