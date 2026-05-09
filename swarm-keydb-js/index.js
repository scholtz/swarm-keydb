import { createClient } from 'redis';
import { createHmac } from 'node:crypto';
import { KeyNotFoundError, wrapRedisError } from './errors.js';

export { ConnectionError, KeyNotFoundError, SwarmKeyDbError } from './errors.js';
export const PrivacyMode = Object.freeze({
  None: 'none',
  ObliviousHashing: 'oblivious_hashing',
  FullPSI: 'full_psi'
});

/**
 * DID authentication modes supported by SwarmKeyDB.
 * @enum {string}
 */
export const DidAuthMode = Object.freeze({
  /** No DID authentication (default). */
  None: 'none',
  /** Authenticate callers using did:ethr backed by an Ethereum address. */
  EthrDid: 'ethr_did'
});

export const OfflineMode = Object.freeze({
  Never: 'never',
  Auto: 'auto',
  Always: 'always'
});

export class SwarmKeyDb {
  /**
   * @param {object} options - Connection and feature options.
   * @param {string}  options.host        - Redis host.
   * @param {number}  options.port        - Redis port.
   * @param {string}  [options.privacyMode]  - Privacy mode (see PrivacyMode).
   * @param {string}  [options.privacyKey]   - Privacy HMAC key (hex).
   * @param {string}  [options.didMode]      - DID authentication mode (see DidAuthMode).
   * @param {string}  [options.didRpcUrl]    - Ethereum RPC URL used for on-chain DID resolution.
   * @param {string}  [options.didMethod]    - DID method string, e.g. "ethr" (default).
   * @param {string}  [options.offlineMode]  - Offline mode (see OfflineMode).
   * @param {Function} [clientFactory]     - Optional factory for the underlying Redis client (for testing).
   */
  constructor(options, clientFactory) {
    this.options = options;
    this.clientFactory = clientFactory ?? ((url) => createClient({ url }));
    this.client = null;
    this.privacyMode = (options.privacyMode ?? PrivacyMode.None).toLowerCase();
    this.privacyKey = options.privacyKey;
    this.tokenToPlain = new Map();
    this.didMode = options.didMode ?? DidAuthMode.None;
    this.offlineMode = options.offlineMode ?? OfflineMode.Never;
    this._currentDid = null;
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

  /**
   * Registers a decentralized identity (DID) for the current connection.
   * When both `proofMessage` and `proofSignature` are provided, the server verifies the
   * Ethereum personal-sign proof immediately.  All subsequent operations on this connection
   * are performed under the given DID.
   *
   * @param {string} did              - DID string, e.g. `did:ethr:0x…`.
   * @param {string} [proofMessage]   - Plain-text challenge that was signed.
   * @param {string} [proofSignature] - 65-byte hex-encoded Ethereum personal-sign signature.
   */
  async setDid(did, proofMessage, proofSignature) {
    this._currentDid = did;

    const args = ['AUTHDID', did];
    if (proofMessage && proofSignature) {
      args.push(proofMessage, proofSignature);
    }
    await this.client.sendCommand(args);
  }

  /**
   * Clears the current DID context from this connection.
   */
  clearDid() {
    this._currentDid = null;
  }

  async get(key) {
    this.#validateKey(key);
    const token = this.#tokenizeKey(key);

    try {
      return await this.client.get(token);
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
    const token = this.#tokenizeKey(key);
    try {
      await this.client.set(token, value);
      this.#rememberKey(key, token);
    } catch (error) {
      throw wrapRedisError(`put(${key})`, error);
    }
  }

  async delete(key) {
    this.#validateKey(key);
    const token = this.#tokenizeKey(key);
    try {
      const deleted = (await this.client.del(token)) > 0;
      if (deleted) {
        this.tokenToPlain.delete(token);
      }
      return deleted;
    } catch (error) {
      throw wrapRedisError(`delete(${key})`, error);
    }
  }

  async list(pattern = '*') {
    try {
      if (this.privacyMode === PrivacyMode.None) {
        return await this.client.keys(pattern);
      }
      return [...this.tokenToPlain.values()].filter((key) => this.#matchesPattern(key, pattern));
    } catch (error) {
      throw wrapRedisError(`list(${pattern})`, error);
    }
  }

  async batchGet(keys) {
    if (!Array.isArray(keys) || keys.length === 0) {
      return [];
    }

    keys.forEach((key) => this.#validateKey(key));
    const tokens = keys.map((key) => this.#tokenizeKey(key));

    try {
      return await this.client.mGet(tokens);
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
    const tokenizedPairs = Object.fromEntries(keys.map((key) => [this.#tokenizeKey(key), pairs[key]]));

    try {
      await this.client.mSet(tokenizedPairs);
      keys.forEach((key) => this.#rememberKey(key, this.#tokenizeKey(key)));
    } catch (error) {
      throw wrapRedisError('batchPut', error);
    }
  }

  async setWithTTL(key, value, ttlSeconds) {
    this.#validateKey(key);
    if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
      throw new Error('ttlSeconds must be a positive integer');
    }
    const token = this.#tokenizeKey(key);

    try {
      await this.client.setEx(token, ttlSeconds, value);
      this.#rememberKey(key, token);
    } catch (error) {
      throw wrapRedisError(`setWithTTL(${key})`, error);
    }
  }

  async backup() {
    try {
      return await this.client.sendCommand(['BACKUP']);
    } catch (error) {
      throw wrapRedisError('backup', error);
    }
  }

  async restore(ref, key) {
    if (typeof ref !== 'string' || ref.length === 0) {
      throw new Error('ref must be a non-empty string');
    }

    const args = ['RESTOREDB', ref];
    if (typeof key === 'string' && key.length > 0) {
      args.push(key);
    }

    try {
      return Number(await this.client.sendCommand(args));
    } catch (error) {
      throw wrapRedisError(`restore(${ref})`, error);
    }
  }

  async rotateKey(oldKey, newKey) {
    if (typeof oldKey !== 'string' || oldKey.length === 0) {
      throw new Error('oldKey must be a non-empty string');
    }
    if (typeof newKey !== 'string' || newKey.length === 0) {
      throw new Error('newKey must be a non-empty string');
    }

    try {
      return await this.client.sendCommand(['ROTATEKEY', oldKey, newKey]);
    } catch (error) {
      throw wrapRedisError('rotateKey', error);
    }
  }

  #validateKey(key) {
    if (typeof key !== 'string' || key.length === 0) {
      throw new Error('key must be a non-empty string');
    }
  }

  #tokenizeKey(key) {
    if (this.privacyMode === PrivacyMode.None) {
      return key;
    }
    if (typeof this.privacyKey !== 'string' || this.privacyKey.length === 0) {
      throw new Error('privacyKey must be set when privacyMode is enabled');
    }
    return createHmac('sha256', Buffer.from(this.privacyKey, 'hex')).update(key, 'utf8').digest('hex');
  }

  #rememberKey(key, token) {
    if (this.privacyMode !== PrivacyMode.None) {
      this.tokenToPlain.set(token, key);
    }
  }

  #matchesPattern(key, pattern) {
    if (!pattern || pattern === '*') {
      return true;
    }
    if (!pattern.includes('*')) {
      return key === pattern;
    }
    const prefix = pattern.slice(0, pattern.indexOf('*'));
    return key.startsWith(prefix);
  }
}
