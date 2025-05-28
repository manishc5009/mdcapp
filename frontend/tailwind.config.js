export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: '#7367F0',
        secondary: '#82868B',
        dark: {
          DEFAULT: '#283046',
          light: '#2F3349',
          lighter: '#343D55'
        }
      }
    },
  },
  plugins: [],
  darkMode: 'class',
}