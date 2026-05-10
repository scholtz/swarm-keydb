import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/integration',
  testMatch: /.*\.playwright\.ts/,
  timeout: 60_000,
  retries: 1,
  use: {
    headless: true,
    baseURL: 'http://127.0.0.1:4173'
  }
});
