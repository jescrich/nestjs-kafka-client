import { IdempotencyStore } from './idempotency.store';

/**
 * Minimal, client-agnostic Redis surface the {@link RedisIdempotencyStore} drives. Kept to
 * three primitives so the library never hard-depends on `ioredis` / `node-redis` (neither is
 * a dependency or peerDependency). Wire your client in once via {@link RedisIdempotencyStore.fromIoredis}
 * / {@link RedisIdempotencyStore.fromNodeRedis}, or implement this adapter directly.
 */
export interface IdempotencyRedisAdapter {
  /**
   * Atomic `SET key 1 PX <ttlMs> NX`. MUST resolve `true` only when the key was newly created
   * (it did not previously exist) and `false` when it was already present. This single round-trip
   * is the cross-pod dedup guarantee.
   */
  setIfAbsent(key: string, ttlMs: number): Promise<boolean>;
  /** Unconditional `SET key 1 PX <ttlMs>`. */
  set(key: string, ttlMs: number): Promise<void>;
  /** `EXISTS key` → true/false. */
  exists(key: string): Promise<boolean>;
}

export interface RedisIdempotencyStoreOptions {
  /** Default TTL for an entry, in milliseconds. Default: 3_600_000 (1h). */
  ttlMs?: number;
  /** Key namespace, prepended to every messageId. Default: 'idem:'. */
  keyPrefix?: string;
}

const DEFAULT_TTL_MS = 3_600_000;
const DEFAULT_KEY_PREFIX = 'idem:';

/**
 * Shared, multi-pod {@link IdempotencyStore} backed by Redis. Unlike `InMemoryIdempotencyStore`
 * (per-process), every instance/replica sees the same dedup set, so a message processed on pod A
 * is recognised as a duplicate when a rebalance redelivers it to pod B.
 *
 * Prefer {@link markIfNew} (atomic `SET NX`) over `isProcessed` + `markProcessed`: the latter pair
 * is a check-then-act that two replicas could interleave. The consume loop's default
 * mark-after-success flow is still safe here because a Kafka partition is owned by exactly one
 * group member at a time — but `markIfNew` is the correct primitive when you want a hard claim.
 */
export class RedisIdempotencyStore implements IdempotencyStore {
  private readonly ttlMs: number;
  private readonly keyPrefix: string;

  constructor(
    private readonly redis: IdempotencyRedisAdapter,
    options?: RedisIdempotencyStoreOptions,
  ) {
    this.ttlMs = options?.ttlMs ?? DEFAULT_TTL_MS;
    this.keyPrefix = options?.keyPrefix ?? DEFAULT_KEY_PREFIX;
  }

  async isProcessed(messageId: string): Promise<boolean> {
    return this.redis.exists(this.key(messageId));
  }

  async markProcessed(messageId: string, ttlMs?: number): Promise<void> {
    await this.redis.set(this.key(messageId), ttlMs ?? this.ttlMs);
  }

  async markIfNew(messageId: string, ttlMs?: number): Promise<boolean> {
    return this.redis.setIfAbsent(this.key(messageId), ttlMs ?? this.ttlMs);
  }

  private key(messageId: string): string {
    return `${this.keyPrefix}${messageId}`;
  }

  /**
   * Build a store from an `ioredis` client (signature: `set(key, val, 'PX', ttl, 'NX')`).
   * The client is typed `any` so the library carries no `ioredis` dependency.
   */
  static fromIoredis(redis: any, options?: RedisIdempotencyStoreOptions): RedisIdempotencyStore {
    const adapter: IdempotencyRedisAdapter = {
      setIfAbsent: async (key, ttlMs) => (await redis.set(key, '1', 'PX', ttlMs, 'NX')) === 'OK',
      set: async (key, ttlMs) => {
        await redis.set(key, '1', 'PX', ttlMs);
      },
      exists: async (key) => (await redis.exists(key)) === 1,
    };
    return new RedisIdempotencyStore(adapter, options);
  }

  /**
   * Build a store from a `node-redis` v4 client (signature: `set(key, val, { PX, NX })`).
   * The client is typed `any` so the library carries no `node-redis` dependency.
   */
  static fromNodeRedis(redis: any, options?: RedisIdempotencyStoreOptions): RedisIdempotencyStore {
    const adapter: IdempotencyRedisAdapter = {
      setIfAbsent: async (key, ttlMs) =>
        (await redis.set(key, '1', { PX: ttlMs, NX: true })) === 'OK',
      set: async (key, ttlMs) => {
        await redis.set(key, '1', { PX: ttlMs });
      },
      exists: async (key) => (await redis.exists(key)) === 1,
    };
    return new RedisIdempotencyStore(adapter, options);
  }
}
