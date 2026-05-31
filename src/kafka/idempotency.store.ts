/**
 * Idempotency / deduplication store contract.
 *
 * Implementations track which message ids have already been processed so that
 * consumers can safely skip duplicate deliveries (Kafka guarantees at-least-once).
 */
export interface IdempotencyStore {
  /** Returns true if messageId was already processed and is still within its TTL. */
  isProcessed(messageId: string): Promise<boolean>;
  /** Marks messageId as processed. An optional ttlMs overrides the default TTL. */
  markProcessed(messageId: string, ttlMs?: number): Promise<void>;
}

/**
 * Options for {@link InMemoryIdempotencyStore}.
 */
export interface InMemoryIdempotencyStoreOptions {
  /** Default time-to-live for an entry, in milliseconds. Default: 3_600_000 (1h). */
  ttlMs?: number;
  /** Maximum number of live entries before the oldest is evicted. Default: 100_000. */
  maxEntries?: number;
  /** Interval for the periodic cleanup timer, in milliseconds. Default: 60_000. Values <= 0 disable it. */
  cleanupIntervalMs?: number;
}

const DEFAULT_TTL_MS = 3_600_000;
const DEFAULT_MAX_ENTRIES = 100_000;
const DEFAULT_CLEANUP_INTERVAL_MS = 60_000;

/**
 * In-memory implementation of {@link IdempotencyStore}.
 *
 * Stores `messageId -> expiresAt (epoch ms)` in a `Map`, relying on the Map's
 * insertion-order guarantee for recency-based eviction. Suitable for single-process
 * deployments; for multi-instance setups back the store with a shared cache instead.
 */
export class InMemoryIdempotencyStore implements IdempotencyStore {
  private readonly store = new Map<string, number>();
  private readonly ttlMs: number;
  private readonly maxEntries: number;
  private readonly cleanupIntervalMs: number;
  private cleanupTimer?: ReturnType<typeof setInterval>;

  constructor(options?: InMemoryIdempotencyStoreOptions) {
    this.ttlMs = options?.ttlMs ?? DEFAULT_TTL_MS;
    this.maxEntries = options?.maxEntries ?? DEFAULT_MAX_ENTRIES;
    this.cleanupIntervalMs = options?.cleanupIntervalMs ?? DEFAULT_CLEANUP_INTERVAL_MS;

    if (this.cleanupIntervalMs > 0) {
      this.cleanupTimer = setInterval(() => this.cleanupExpired(), this.cleanupIntervalMs);
      // Do not keep the event loop alive just because of this timer.
      if (typeof this.cleanupTimer.unref === 'function') {
        this.cleanupTimer.unref();
      }
    }
  }

  /**
   * Returns true if messageId was already processed and is still within its TTL.
   * Expired entries are removed lazily on access.
   */
  async isProcessed(messageId: string): Promise<boolean> {
    const expiresAt = this.store.get(messageId);
    if (expiresAt === undefined) {
      return false;
    }
    if (Date.now() < expiresAt) {
      return true;
    }
    // Expired: drop it lazily.
    this.store.delete(messageId);
    return false;
  }

  /**
   * Marks messageId as processed. An optional ttlMs overrides the default TTL.
   * Re-marking an existing id refreshes its recency and expiry. When the store
   * exceeds maxEntries, the oldest inserted entry is evicted.
   */
  async markProcessed(messageId: string, ttlMs?: number): Promise<void> {
    const expiresAt = Date.now() + (ttlMs ?? this.ttlMs);

    // Re-insert to move the key to the most-recent position (recency ordering).
    if (this.store.has(messageId)) {
      this.store.delete(messageId);
    }
    this.store.set(messageId, expiresAt);

    if (this.store.size > this.maxEntries) {
      const oldest = this.store.keys().next().value;
      if (oldest !== undefined) {
        this.store.delete(oldest);
      }
    }
  }

  /** Number of live (non-expired) entries. */
  size(): number {
    const now = Date.now();
    let count = 0;
    for (const [key, expiresAt] of this.store) {
      if (now < expiresAt) {
        count++;
      } else {
        // Opportunistically purge expired entries while counting.
        this.store.delete(key);
      }
    }
    return count;
  }

  /** Removes all entries. Does not stop the cleanup timer. */
  clear(): void {
    this.store.clear();
  }

  /** Stops the periodic cleanup timer. Idempotent. */
  stopCleanup(): void {
    if (this.cleanupTimer !== undefined) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = undefined;
    }
  }

  /** Removes all expired entries. Used by the periodic cleanup timer. */
  private cleanupExpired(): void {
    const now = Date.now();
    for (const [key, expiresAt] of this.store) {
      if (now >= expiresAt) {
        this.store.delete(key);
      }
    }
  }
}
