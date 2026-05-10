import { describe, expect, it } from 'vitest';
import { SwarmKeyDbClient } from '../src/index';

describe('SwarmKeyDbClient', () => {
  it('serializes core command calls and set options', async () => {
    const calls: Array<[string, ...string[]]> = [];
    const client = new SwarmKeyDbClient();

    (client as any).call = async (command: string, ...args: string[]) => {
      calls.push([command, ...args]);
      if (command === 'GET') {
        return 'world';
      }
      return 'OK';
    };

    await expect(client.set('hello', 'world', { ex: 10, nx: true })).resolves.toBe('OK');
    await expect(client.get('hello')).resolves.toBe('world');

    expect(calls).toEqual([
      ['SET', 'hello', 'world', 'EX', '10', 'NX'],
      ['GET', 'hello']
    ]);
  });

  it('normalizes HGETALL map and RESP2 array payloads', async () => {
    const client = new SwarmKeyDbClient();
    const responses: unknown[] = [
      { a: '1', b: 2 },
      ['x', '9', 'y', '10']
    ];

    (client as any).call = async () => responses.shift();

    await expect(client.hgetall('hash')).resolves.toEqual({ a: '1', b: '2' });
    await expect(client.hgetall('hash')).resolves.toEqual({ x: '9', y: '10' });
  });

  it('routes push frames to registered pubsub handlers', async () => {
    const client = new SwarmKeyDbClient();
    (client as any).call = async () => ['subscribe', 'news', 1];

    let seen = '';
    await client.subscribe('news', (_channel, message) => {
      seen = message;
    });

    (client as any).handlePush(['message', 'news', 'hello']);
    expect(seen).toBe('hello');
  });

  it('falls back to HTTP for GET when websocket command fails', async () => {
    const fetchImpl = async () => ({ json: async () => ({ result: 'http-value' }) }) as Response;
    const client = new SwarmKeyDbClient({ fetchImpl });

    (client as any).call = async () => {
      throw new Error('socket unavailable');
    };

    await expect(client.get('fallback-key')).resolves.toBe('http-value');
  });
});
