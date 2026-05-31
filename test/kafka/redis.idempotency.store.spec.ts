import { RedisIdempotencyStore, IdempotencyRedisAdapter } from '../../src/kafka/redis.idempotency.store';

/** In-memory fake of the 3-method adapter, modelling SET NX PX / SET PX / EXISTS. */
function fakeAdapter(): IdempotencyRedisAdapter & { store: Map<string, number> } {
  const store = new Map<string, number>();
  const live = (key: string) => {
    const exp = store.get(key);
    if (exp === undefined) return false;
    if (Date.now() < exp) return true;
    store.delete(key);
    return false;
  };
  return {
    store,
    async setIfAbsent(key, ttlMs) {
      if (live(key)) return false;
      store.set(key, Date.now() + ttlMs);
      return true;
    },
    async set(key, ttlMs) {
      store.set(key, Date.now() + ttlMs);
    },
    async exists(key) {
      return live(key);
    },
  };
}

describe('RedisIdempotencyStore', () => {
  it('marks and detects a processed id', async () => {
    const store = new RedisIdempotencyStore(fakeAdapter());
    await expect(store.isProcessed('a')).resolves.toBe(false);
    await store.markProcessed('a');
    await expect(store.isProcessed('a')).resolves.toBe(true);
  });

  it('markIfNew is true once then false (atomic claim)', async () => {
    const store = new RedisIdempotencyStore(fakeAdapter());
    await expect(store.markIfNew('x')).resolves.toBe(true);
    await expect(store.markIfNew('x')).resolves.toBe(false);
  });

  it('namespaces keys with the configured prefix', async () => {
    const adapter = fakeAdapter();
    const store = new RedisIdempotencyStore(adapter, { keyPrefix: 'evt:' });
    await store.markProcessed('123');
    expect([...adapter.store.keys()]).toEqual(['evt:123']);
  });

  it('fromIoredis wires SET NX PX correctly', async () => {
    const calls: any[][] = [];
    const ioredis = {
      set: jest.fn(async (...args: any[]) => {
        calls.push(args);
        return 'OK';
      }),
      exists: jest.fn(async () => 0),
    };
    const store = RedisIdempotencyStore.fromIoredis(ioredis, { ttlMs: 5000, keyPrefix: 'idem:' });
    await expect(store.markIfNew('m1')).resolves.toBe(true);
    expect(calls[0]).toEqual(['idem:m1', '1', 'PX', 5000, 'NX']);
  });

  it('fromNodeRedis wires SET {PX,NX} correctly', async () => {
    const calls: any[][] = [];
    const nodeRedis = {
      set: jest.fn(async (...args: any[]) => {
        calls.push(args);
        return 'OK';
      }),
      exists: jest.fn(async () => 0),
    };
    const store = RedisIdempotencyStore.fromNodeRedis(nodeRedis, { ttlMs: 5000 });
    await expect(store.markIfNew('m2')).resolves.toBe(true);
    expect(calls[0]).toEqual(['idem:m2', '1', { PX: 5000, NX: true }]);
  });
});
