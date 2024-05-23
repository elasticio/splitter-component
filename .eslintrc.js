module.exports = {
  extends: 'airbnb-base',
  parser: 'babel-eslint',
  env: {
    mocha: true,
    node: true,
  },
  rules: {
    'max-len': ['error', { code: 240 }],
  },
};
