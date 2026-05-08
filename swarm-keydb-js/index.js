import { createClient } from 'redis';
import { KeyNotFoundError, wrapRedisError } from './errors.js';

export { ConnectionError, KeyNotFoundError, SwarmKeyDbError } from './errors.js';

export class SwarmKeyDb {
  constructor(options, clientFactory) {
    this.options = options;
    this.clientFactory = clientFactory ?? ((url) => createClient({ url }));
    this.client = null;
  }

  async connect() {
    if (this.client) {
      return;
    }

    const protocol = this.options.tls ? 'rediss' : 'redis';
    const auth = this.options.password ? `:${encodeURIComponent(this.options.password)}@` : '';
    const url = `${protocol}://${auth}${this.options.host}:${this.options.port}`;

    this.client = this.clientFactory(url);
    try {
      await this.client.connect();
    } catch (error) {
      throw wrapRedisError('connect', error);
    }
  }

  async disconnect() {
    if (!this.client) {
      return;
    }

    try {
      await this.client.quit();
    } catch {
      await this.client.disconnect();
    }

    this.client = null;
  }

  async get(key) {
    this.#validateKey(key);

    try {
      return await this.client.get(key);
    } catch (error) {
      throw wrapRedisError(`get(${key})`, error);
    }
  }

  async getOrThrow(key) {
    const value = await this.get(key);
    if (value === null) {
      throw new KeyNotFoundError(`Key not found: ${key}`);
    }

    return value;
  }

  async put(key, value) {
    this.#validateKey(key);
    try {
      await this.client.set(key, value);
    } catch (error) {
      throw wrapRedisError(`put(${key})`, error);
    }
  }

  async delete(key) {
    this.#validateKey(key);
    try {
      return (await this.client.del(key)) > 0;
    } catch (error) {
      throw wrapRedisError(`delete(${key})`, error);
    }
  }

  async list(pattern = '*') {
    try {
      return await this.client.keys(pattern);
    } catch (error) {
      throw wrapRedisError(`list(${pattern})`, error);
    }
  }

  async batchGet(keys) {
    if (!Array.isArray(keys) || keys.length === 0) {
      return [];
    }

    keys.forEach((key) => this.#validateKey(key));

    try {
      return await this.client.mGet(keys);
    } catch (error) {
      throw wrapRedisError('batchGet', error);
    }
  }

  async batchPut(entries) {
    const pairs = Array.isArray(entries)
      ? Object.fromEntries(entries.map((entry) => [entry.key, entry.value]))
      : entries;

    const keys = Object.keys(pairs ?? {});
    if (keys.length === 0) {
      return;
    }

    keys.forEach((key) => this.#validateKey(key));

    try {
      await this.client.mSet(pairs);
    } catch (error) {
      throw wrapRedisError('batchPut', error);
    }
  }

  async setWithTTL(key, value, ttlSeconds) {
    this.#validateKey(key);
    if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
      throw new Error('ttlSeconds must be a positive integer.');
    }

    try {
      await this.client.setEx(key, ttlSeconds, value);
    } catch (error) {
      throw wrapRedisError(`setWithTTL(${key})`, error);
    }
  }

  #validateKey(key) {
    if (typeof key !== 'string' || key.length === 0) {
      throw new Error('key must be a non-empty string.');
    }
  }
}
