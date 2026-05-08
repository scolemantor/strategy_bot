/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bull: "#22c55e",
        bear: "#ef4444",
        conflict: "#eab308",
        ink: "#0b0f17",
        panel: "#111827",
        panel2: "#1f2937",
        accent: "#60a5fa",
      },
      fontFamily: {
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
    },
  },
  plugins: [],
};
