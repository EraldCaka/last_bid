// tailwind.config.js
const path = require("path");

module.exports = {
  content: [
    "./js/**/*.js",
    "../lib/last_bid_web.ex",
    "../lib/last_bid_web/**/*.*ex",
    "../lib/last_bid_web/**/*.heex"
  ],
  theme: {
    extend: {
      colors: {
        terminal: {
          bg:         "#030712",
          surface:    "#0d1117",
          surface2:   "#161b22",
          border:     "#21262d",
          borderbright: "#30363d",
        }
      },
      fontFamily: {
        mono: ["JetBrains Mono", "ui-monospace", "monospace"],
        sans: ["Inter", "system-ui", "sans-serif"],
      },
      animation: {
        "spin-slow": "spin 8s linear infinite",
        "float": "float 3s ease-in-out infinite",
        "gradient-x": "gradient-x 4s ease infinite",
      },
      keyframes: {
        float: {
          "0%, 100%": { transform: "translateY(0)" },
          "50%": { transform: "translateY(-6px)" },
        },
        "gradient-x": {
          "0%, 100%": { backgroundPosition: "0% 50%" },
          "50%": { backgroundPosition: "100% 50%" },
        },
      },
      backgroundSize: {
        "200": "200% 200%",
      },
    },
  },
  plugins: [],
};
