import type { Config } from "tailwindcss";

const strezlessConfig: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        strezless: {
          primary: "#8B5CF6",
          secondary: "#EC4899",
          accent: "#F59E0B",
          dark: "#1F2937",
          light: "#F3F4F6",
        },
      },
      fontFamily: {
        display: ["var(--font-display)", "system-ui", "sans-serif"],
        body: ["var(--font-body)", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};

export default strezlessConfig;
