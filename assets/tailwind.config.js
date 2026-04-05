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
    extend: {}
  },
  plugins: []
};
