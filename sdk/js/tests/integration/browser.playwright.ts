import { test, expect } from '@playwright/test';
import { createServer, type Server } from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const distDir = join(__dirname, '..', '..', 'dist');
const moduleUrl = '/index.js';

let server: Server;

const mimeType = (path: string): string => {
  const ext = extname(path);
  if (ext === '.js') return 'application/javascript; charset=utf-8';
  if (ext === '.html') return 'text/html; charset=utf-8';
  return 'text/plain; charset=utf-8';
};

test.beforeAll(async () => {
  server = createServer(async (req, res) => {
    const url = req.url ?? '/';
    if (url === '/' || url === '/test-runner.html') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end('<!doctype html><html><body>swarm-keydb-sdk-tests</body></html>');
      return;
    }

    const safeFiles: Record<string, string> = {
      '/index.js': 'index.js',
      '/index.js.map': 'index.js.map'
    };
    const selectedFile = safeFiles[url];
    if (!selectedFile) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('not found');
      return;
    }
    const filePath = join(distDir, selectedFile);
    try {
      const content = await readFile(filePath);
      res.writeHead(200, { 'Content-Type': mimeType(filePath) });
      res.end(content);
    } catch {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('not found');
    }
  });

  await new Promise<void>((resolve) => {
    server.listen(4173, '127.0.0.1', () => resolve());
  });
});

test.afterAll(async () => {
  if (server) {
    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
  }
});

test.beforeEach(async ({ page }) => {
  await page.goto('/test-runner.html');
});

test('browser client supports HELLO 3 and basic SET/GET', async ({ page }) => {
  const result = await page.evaluate(async (url) => {
    const sdk = await import(url);
    const client = sdk.createClient({
      wsUrl: 'ws://127.0.0.1:8765/',
      httpUrl: 'http://127.0.0.1:8080'
    });

    await client.connect();
    const key = `pw:kv:${Date.now()}`;
    await client.set(key, 'world');
    const value = await client.get(key);
    const hello = await client.raw('HELLO', '3');
    await client.disconnect();

    return { value, hello };
  }, moduleUrl);

  expect(result.value).toBe('world');
  expect(JSON.stringify(result.hello)).toContain('3');
});

test('browser client supports Pub/Sub and XREAD BLOCK', async ({ page }) => {
  const result = await page.evaluate(async (url) => {
    const sdk = await import(url);
    const publisher = sdk.createClient({ wsUrl: 'ws://127.0.0.1:8765/' });
    const subscriber = sdk.createClient({ wsUrl: 'ws://127.0.0.1:8765/' });
    const writer = sdk.createClient({ wsUrl: 'ws://127.0.0.1:8765/' });
    const reader = sdk.createClient({ wsUrl: 'ws://127.0.0.1:8765/' });

    await publisher.connect();
    await subscriber.connect();
    await writer.connect();
    await reader.connect();

    const channel = `pw:ch:${Date.now()}`;
    const stream = `pw:stream:${Date.now()}`;

    const pubSubPromise = new Promise<string>((resolve) => {
      subscriber.subscribe(channel, (_c: string, message: string) => resolve(message));
    });

    await new Promise((resolve) => setTimeout(resolve, 75));
    await publisher.publish(channel, 'hello-browser');
    const pubSubMessage = await pubSubPromise;

    const xreadPromise = reader.xread({ [stream]: '$' }, { blockMs: 1500, count: 1 });
    await new Promise((resolve) => setTimeout(resolve, 75));
    await writer.xadd(stream, '*', { field: 'value' });
    const xreadResult = await xreadPromise;

    await publisher.disconnect();
    await subscriber.disconnect();
    await writer.disconnect();
    await reader.disconnect();

    return {
      pubSubMessage,
      xreadFound: Array.isArray(xreadResult) && xreadResult.length > 0
    };
  }, moduleUrl);

  expect(result.pubSubMessage).toBe('hello-browser');
  expect(result.xreadFound).toBe(true);
});

test('browser client uses HTTP fallback when websocket is unavailable', async ({ page }) => {
  const result = await page.evaluate(async (url) => {
    const sdk = await import(url);
    const client = sdk.createClient({
      wsUrl: 'ws://127.0.0.1:1/',
      httpUrl: 'http://127.0.0.1:8080',
      requestTimeoutMs: 250,
      reconnect: false,
      httpFallback: true
    });

    const key = `pw:http:${Date.now()}`;
    await client.set(key, 'fallback-ok');
    return client.get(key);
  }, moduleUrl);

  expect(result).toBe('fallback-ok');
});
