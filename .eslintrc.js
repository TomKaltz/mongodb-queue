module.exports = {
  env: {
    es6: true,
    node: true
  },
  extends: ["standard", "eslint:recommended"],
  rules: {
    indent: ["error", 2],
    "linebreak-style": ["error", "unix"],
    quotes: ["error", "single"],
    semi: ["error", "never"],
    "comma-dangle": ["error", "always-multiline"]
  }
};
