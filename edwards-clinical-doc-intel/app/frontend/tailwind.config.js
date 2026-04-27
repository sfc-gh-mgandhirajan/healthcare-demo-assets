/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        edwards: {
          red: '#C8102E',
          dark: '#1A1F36',
          slate: '#2D3748',
          gray: '#64748B',
          light: '#F1F5F9',
          accent: '#0891B2',
        }
      }
    },
  },
  plugins: [],
}
