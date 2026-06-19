import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vitejs.dev/config/
export default defineConfig(async ({ mode }) => ({
  plugins: [react()],
  define: {
    "import.meta.env.VITE_APP_TARGET": JSON.stringify(mode === "web" ? "web" : "desktop"),
  },

  // Vite options tailored for Tauri development and only applied in `tauri dev` or `tauri build`
  //
  // 1. prevent vite from obscuring rust errors
  clearScreen: false,
  // 2. tauri expects a fixed port, fail if that port is not available
  server: {
    port: 1420,
    strictPort: true,
  },
  // 3. to make use of `TAURI_PLATFORM` and other env variables
  envPrefix: ["VITE_", "TAURI_"],
}));
