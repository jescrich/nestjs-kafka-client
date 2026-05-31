import { InMemoryIdempotencyStore } from '../../src/kafka/idempotency.store';

describe('InMemoryIdempotencyStore', () => {
  let store: InMemoryIdempotencyStore;

  afterEach(() => {
    store?.stopCleanup();
  });

  it('returns false for an unknown id in a new store', async () => {
    store = new InMemoryIdempotencyStore();
    await expect(store.isProcessed('a')).resolves.toBe(false);
  });

  it('returns true after markProcessed', async () => {
    store = new InMemoryIdempotencyStore();
    await store.markProcessed('a');
    await expect(store.isProcessed('a')).resolves.toBe(true);
  });

  it('expires entries after the TTL', async () => {
    store = new InMemoryIdempotencyStore();
    await store.markProcessed('a', 50);
    await expect(store.isProcessed('a')).resolves.toBe(true);
    await new Promise((r) => setTimeout(r, 80));
    await expect(store.isProcessed('a')).resolves.toBe(false);
  });

  it('evicts the oldest entry when maxEntries is exceeded', async () => {
    store = new InMemoryIdempotencyStore({ maxEntries: 2 });
    await store.markProcessed('a');
    await store.markProcessed('b');
    await store.markProcessed('c');

    await expect(store.isProcessed('a')).resolves.toBe(false);
    await expect(store.isProcessed('b')).resolves.toBe(true);
    await expect(store.isProcessed('c')).resolves.toBe(true);
    expect(store.size()).toBe(2);
  });

  it('re-marking an id refreshes its recency', async () => {
    store = new InMemoryIdempotencyStore({ maxEntries: 2 });
    await store.markProcessed('a');
    await store.markProcessed('b');
    // 'a' becomes the most recent again.
    await store.markProcessed('a');
    // Inserting 'c' should evict 'b' (now the oldest), not 'a'.
    await store.markProcessed('c');

    await expect(store.isProcessed('a')).resolves.toBe(true);
    await expect(store.isProcessed('b')).resolves.toBe(false);
    await expect(store.isProcessed('c')).resolves.toBe(true);
  });

  it('clear() resets size to 0', async () => {
    store = new InMemoryIdempotencyStore();
    await store.markProcessed('a');
    await store.markProcessed('b');
    expect(store.size()).toBe(2);
    store.clear();
    expect(store.size()).toBe(0);
  });

  it('stopCleanup() does not throw and can be called twice', () => {
    store = new InMemoryIdempotencyStore();
    expect(() => {
      store.stopCleanup();
      store.stopCleanup();
    }).not.toThrow();
  });

  it('works the same with cleanupIntervalMs: 0 (no timer)', async () => {
    store = new InMemoryIdempotencyStore({ cleanupIntervalMs: 0 });
    await store.markProcessed('a');
    await expect(store.isProcessed('a')).resolves.toBe(true);
    await expect(store.isProcessed('b')).resolves.toBe(false);
    expect(store.size()).toBe(1);
  });

  it('markIfNew claims an id once, then reports duplicates', async () => {
    store = new InMemoryIdempotencyStore();
    await expect(store.markIfNew('a')).resolves.toBe(true);
    await expect(store.markIfNew('a')).resolves.toBe(false);
    await expect(store.isProcessed('a')).resolves.toBe(true);
  });

  it('markIfNew re-claims an id after its TTL expires', async () => {
    store = new InMemoryIdempotencyStore();
    await expect(store.markIfNew('a', 40)).resolves.toBe(true);
    await new Promise((r) => setTimeout(r, 70));
    await expect(store.markIfNew('a', 40)).resolves.toBe(true);
  });
});
