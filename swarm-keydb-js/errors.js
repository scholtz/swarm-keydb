export class SwarmKeyDbError extends Error {
  constructor(message, cause) {
    super(message);
    this.name = this.constructor.name;
    this.cause = cause;
  }
}

export class ConnectionError extends SwarmKeyDbError {}
export class KeyNotFoundError extends SwarmKeyDbError {}

export function wrapRedisError(action, error) {
  const message = error?.message ?? String(error);
  if (/ECONN|connect|socket|network/i.test(message)) {
    return new ConnectionError(`Connection failure during ${action}: ${message}`, error);
  }

  return new SwarmKeyDbError(`SwarmKeyDb ${action} failed: ${message}`, error);
}
