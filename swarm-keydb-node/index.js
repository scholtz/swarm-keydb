const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const defaultRetryDecider = () => true;

function defaultSerialize(value) {
  return typeof value === 'string' ? value : JSON.stringify(value);
}

function defaultDeserialize(value) {
  if (value === null) {
    return null;
  }

  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function createDefaultClientFactory() {
  return async (clientOptions) => {
    const { SwarmKeyDb } = await import('swarm-keydb-js');
    return new SwarmKeyDb(clientOptions);
  };
}

export class SwarmKeyDbService {
  constructor(options = {}) {
    this.clientFactory = options.clientFactory ?? createDefaultClientFactory();
    this.clientOptions = options.clientOptions ?? { host: '127.0.0.1', port: 6379 };
    this.poolSize = Math.max(1, options.poolSize ?? 2);
    this.maxRetries = Math.max(0, options.maxRetries ?? 3);
    this.retryDelayMs = Math.max(0, options.retryDelayMs ?? 25);
    this.shouldRetry = options.shouldRetry ?? defaultRetryDecider;
    this.clients = [];
    this.nextClientIndex = 0;
    this.isInitialized = false;
  }

  async initialize() {
    if (this.isInitialized) {
      return;
    }

    for (let index = 0; index < this.poolSize; index += 1) {
      const client = await this.clientFactory(this.clientOptions);
      await client.connect();
      this.clients.push(client);
    }

    this.isInitialized = true;
  }

  async dispose() {
    const disconnects = this.clients.map((client) => client.disconnect());
    await Promise.allSettled(disconnects);
    this.clients = [];
    this.isInitialized = false;
  }

  async get(key, options = {}) {
    const parse = options.parse ?? defaultDeserialize;
    const raw = await this.#withRetry((client) => client.get(key));
    return parse(raw);
  }

  async put(key, value, options = {}) {
    const serialize = options.serialize ?? defaultSerialize;
    await this.#withRetry((client) => client.put(key, serialize(value)));
    return value;
  }

  async delete(key) {
    return this.#withRetry((client) => client.delete(key));
  }

  async list(prefix = '') {
    const pattern = prefix.length > 0 ? `${prefix}*` : '*';
    return this.#withRetry((client) => client.list(pattern));
  }

  async *scan(prefix = '', batchSize = 200) {
    const keys = await this.list(prefix);
    for (let start = 0; start < keys.length; start += batchSize) {
      for (const key of keys.slice(start, start + batchSize)) {
        yield key;
      }
    }
  }

  async #withRetry(operation) {
    if (!this.isInitialized || this.clients.length === 0) {
      await this.initialize();
    }

    let attempt = 0;
    let lastError;

    while (attempt <= this.maxRetries) {
      const client = this.#nextClient();
      try {
        return await operation(client);
      } catch (error) {
        lastError = error;
        if (attempt >= this.maxRetries || !this.shouldRetry(error, attempt + 1)) {
          throw error;
        }

        await sleep(this.retryDelayMs * (2 ** attempt));
        attempt += 1;
      }
    }

    throw lastError;
  }

  #nextClient() {
    const client = this.clients[this.nextClientIndex % this.clients.length];
    this.nextClientIndex = (this.nextClientIndex + 1) % this.clients.length;
    return client;
  }
}

export function createExpressSwarmKeyDbMiddleware(service, options = {}) {
  const sessionKeyResolver = options.sessionKeyResolver ?? ((req) => req.headers['x-swarm-session-key']);

  return async (req, res, next) => {
    try {
      req.swarmKeyDb = service;

      const sessionKey = sessionKeyResolver(req);
      if (typeof sessionKey === 'string' && sessionKey.length > 0) {
        req.swarmSession = await service.get(sessionKey);
        req.saveSwarmSession = async (value) => service.put(sessionKey, value);
      }

      next();
    } catch (error) {
      next(error);
    }
  };
}

export function createExpressCacheMiddleware(service, options = {}) {
  const keyResolver = options.keyResolver ?? ((req) => `cache:${req.method}:${req.originalUrl ?? req.url}`);

  return async (req, res, next) => {
    try {
      const cacheKey = keyResolver(req);
      const cached = await service.get(cacheKey);
      if (cached !== null && cached !== undefined) {
        res.setHeader('x-swarm-cache', 'HIT');
        res.json(cached);
        return;
      }

      const originalJson = res.json.bind(res);
      res.json = (body) => {
        service.put(cacheKey, body).catch(() => {});
        res.setHeader('x-swarm-cache', 'MISS');
        return originalJson(body);
      };

      next();
    } catch (error) {
      next(error);
    }
  };
}

export function createFastifySwarmKeyDbPlugin(service, options = {}) {
  const sessionKeyResolver = options.sessionKeyResolver ?? ((request) => request.headers['x-swarm-session-key']);

  return async function swarmKeyDbPlugin(fastify) {
    fastify.decorateRequest('swarmKeyDb', null);
    fastify.decorateRequest('swarmSession', null);

    fastify.addHook('onRequest', async (request) => {
      request.swarmKeyDb = service;
      const sessionKey = sessionKeyResolver(request);
      if (typeof sessionKey === 'string' && sessionKey.length > 0) {
        request.swarmSession = await service.get(sessionKey);
      }
    });
  };
}
