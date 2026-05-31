import { Logger } from '@nestjs/common';
import { KafkaClient } from '../../src/kafka/kafka.client';
import { InMemoryIdempotencyStore } from '../../src/kafka/idempotency.store';
import { createEnvelope } from '../../src/kafka/kafka.envelope';
import { KafkaMessage } from '@nestjs/microservices/external/kafka.interface';

/**
 * Unit tests for the new KafkaClient features (idempotent producer, ensureTopics,
 * retry backoff, envelope/dedup resolution). These do NOT require a real broker:
 * kafkajs is mocked at the module level.
 */

jest.mock('kafkajs');

function buildMessage(value: any, headers?: Record<string, Buffer>, offset = '0'): KafkaMessage {
  return {
    key: Buffer.from('k'),
    value: Buffer.from(JSON.stringify(value)),
    timestamp: '0',
    attributes: 0,
    offset,
    headers: headers ?? {},
    size: 0,
  } as unknown as KafkaMessage;
}

describe('KafkaClient new features', () => {
  let kafkaClient: KafkaClient;
  let mockProducer: any;
  let mockConsumer: any;
  let mockAdmin: any;
  let producerFactory: jest.Mock;

  beforeAll(() => {
    const { logLevel } = require('kafkajs');
    logLevel.WARN = 3;
    logLevel.ERROR = 4;
  });

  beforeEach(() => {
    jest.spyOn(Logger.prototype, 'log').mockImplementation();
    jest.spyOn(Logger.prototype, 'debug').mockImplementation();
    jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    jest.spyOn(Logger.prototype, 'error').mockImplementation();

    mockProducer = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue(undefined),
    };
    mockConsumer = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockResolvedValue(undefined),
      pause: jest.fn().mockResolvedValue(undefined),
      resume: jest.fn().mockResolvedValue(undefined),
      commitOffsets: jest.fn().mockResolvedValue(undefined),
      on: jest.fn(),
    };
    mockAdmin = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      listTopics: jest.fn().mockResolvedValue([]),
      createTopics: jest.fn().mockResolvedValue(true),
    };
    producerFactory = jest.fn(() => mockProducer);

    const { Kafka } = require('kafkajs');
    Kafka.mockImplementation(() => ({
      producer: producerFactory,
      consumer: jest.fn(() => mockConsumer),
      admin: jest.fn(() => mockAdmin),
    }));
  });

  afterEach(async () => {
    if (kafkaClient) {
      await kafkaClient.shutdown();
    }
    jest.clearAllMocks();
  });

  describe('Idempotent producer', () => {
    it('configures the producer with idempotent + maxInFlightRequests when enabled', async () => {
      kafkaClient = new KafkaClient('idem-client', 'localhost:9092', {
        producerIdempotent: true,
        producerMaxInFlightRequests: 5,
      });
      await kafkaClient.initialize();

      expect(producerFactory).toHaveBeenCalledWith(
        expect.objectContaining({ idempotent: true, maxInFlightRequests: 5 }),
      );
    });

    it('honors a custom maxInFlightRequests value', async () => {
      kafkaClient = new KafkaClient('idem-client2', 'localhost:9092', {
        producerIdempotent: true,
        producerMaxInFlightRequests: 1,
      });
      await kafkaClient.initialize();

      expect(producerFactory).toHaveBeenCalledWith(
        expect.objectContaining({ idempotent: true, maxInFlightRequests: 1 }),
      );
    });

    it('does not set idempotent when not requested', async () => {
      kafkaClient = new KafkaClient('non-idem-client', 'localhost:9092', {});
      await kafkaClient.initialize();

      const config = producerFactory.mock.calls[0][0];
      expect(config?.idempotent).toBeUndefined();
    });

    it('enables idempotence implicitly for a transactional producer', async () => {
      kafkaClient = new KafkaClient('tx-client', 'localhost:9092', {
        producerTransactionalId: 'tx-1',
      });
      await kafkaClient.initialize();

      expect(producerFactory).toHaveBeenCalledWith(
        expect.objectContaining({ transactionalId: 'tx-1', idempotent: true }),
      );
    });
  });

  describe('produce acks', () => {
    it('sends with the default acks (-1)', async () => {
      kafkaClient = new KafkaClient('acks-client', 'localhost:9092', {});
      await kafkaClient.initialize();

      await kafkaClient.produce('topic-a', 'key-1', { hello: 'world' });

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({ topic: 'topic-a', acks: -1 }),
      );
    });

    it('sends with a configured acks value', async () => {
      kafkaClient = new KafkaClient('acks-client2', 'localhost:9092', {
        producerAcks: 1,
      });
      await kafkaClient.initialize();

      await kafkaClient.produce('topic-b', 'key-2', { hello: 'world' });

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({ topic: 'topic-b', acks: 1 }),
      );
    });
  });

  describe('produce with headers', () => {
    it('forwards headers on the message', async () => {
      kafkaClient = new KafkaClient('hdr-client', 'localhost:9092', {});
      await kafkaClient.initialize();

      await kafkaClient.produce('topic-h', 'key-h', { hello: 'world' }, {
        headers: { tenant: 'acme', traceparent: '00-abc' },
      });

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: 'topic-h',
          messages: [
            expect.objectContaining({
              key: 'key-h',
              value: JSON.stringify({ hello: 'world' }),
              headers: { tenant: 'acme', traceparent: '00-abc' },
            }),
          ],
        }),
      );
    });
  });

  describe('send (low-level, header-aware)', () => {
    it('sends a pre-serialized value with headers verbatim and default acks', async () => {
      kafkaClient = new KafkaClient('send-client', 'localhost:9092', {});
      await kafkaClient.initialize();

      const value = JSON.stringify({ event_type: 'order.created', tenant: 'acme' });
      await kafkaClient.send({
        topic: 'orders',
        messages: [{ key: 'urn:order:1', value, headers: { tenant: 'acme' } }],
      });

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'orders',
        acks: -1,
        messages: [{ key: 'urn:order:1', value, headers: { tenant: 'acme' } }],
      });
    });

    it('honors a per-call acks override and counts produced messages', async () => {
      kafkaClient = new KafkaClient('send-client2', 'localhost:9092', {});
      await kafkaClient.initialize();

      await kafkaClient.send({
        topic: 'orders',
        acks: 1,
        messages: [
          { key: 'a', value: 'v1' },
          { key: 'b', value: 'v2' },
        ],
      });

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({ topic: 'orders', acks: 1 }),
      );
      expect(kafkaClient.getMetrics().producedMessages).toBe(2);
    });

    it('rejects when the client is shutting down', async () => {
      kafkaClient = new KafkaClient('send-client3', 'localhost:9092', {});
      await kafkaClient.initialize();
      await kafkaClient.shutdown();

      await expect(
        kafkaClient.send({ topic: 'orders', messages: [{ key: 'a', value: 'v' }] }),
      ).rejects.toThrow('Client is shutting down');
    });
  });

  describe('ensureTopics', () => {
    it('creates only the missing topics with default partitions/replication', async () => {
      mockAdmin.listTopics.mockResolvedValue(['a']);
      kafkaClient = new KafkaClient('ensure-client', 'localhost:9092', {
        defaultNumPartitions: 3,
        defaultReplicationFactor: 2,
      });
      await kafkaClient.initialize();

      await kafkaClient.ensureTopics([{ topic: 'a' }, { topic: 'b' }]);

      expect(mockAdmin.createTopics).toHaveBeenCalledTimes(1);
      const arg = mockAdmin.createTopics.mock.calls[0][0];
      expect(arg.waitForLeaders).toBe(true);
      expect(arg.topics).toHaveLength(1);
      expect(arg.topics[0]).toEqual(
        expect.objectContaining({ topic: 'b', numPartitions: 3, replicationFactor: 2 }),
      );
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    it('does not call createTopics when all topics already exist', async () => {
      mockAdmin.listTopics.mockResolvedValue(['a', 'b']);
      kafkaClient = new KafkaClient('ensure-client2', 'localhost:9092', {});
      await kafkaClient.initialize();

      await kafkaClient.ensureTopics([{ topic: 'a' }, { topic: 'b' }]);

      expect(mockAdmin.createTopics).not.toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    it('returns immediately on an empty topic list', async () => {
      kafkaClient = new KafkaClient('ensure-client3', 'localhost:9092', {});
      await kafkaClient.initialize();
      mockAdmin.connect.mockClear();

      await kafkaClient.ensureTopics([]);

      expect(mockAdmin.connect).not.toHaveBeenCalled();
    });

    it('respects per-topic partition/replication overrides', async () => {
      mockAdmin.listTopics.mockResolvedValue([]);
      kafkaClient = new KafkaClient('ensure-client4', 'localhost:9092', {});
      await kafkaClient.initialize();

      await kafkaClient.ensureTopics([{ topic: 'c', numPartitions: 10, replicationFactor: 3 }]);

      const arg = mockAdmin.createTopics.mock.calls[0][0];
      expect(arg.topics[0]).toEqual(
        expect.objectContaining({ topic: 'c', numPartitions: 10, replicationFactor: 3 }),
      );
    });
  });

  describe('computeBackoffDelay', () => {
    it('always returns base for the fixed strategy', async () => {
      kafkaClient = new KafkaClient('backoff-fixed', 'localhost:9092', {
        retryBackoffStrategy: 'fixed',
        messageRetryDelayMs: 1000,
      });
      await kafkaClient.initialize();

      const compute = (n: number) => (kafkaClient as any).computeBackoffDelay(n);
      expect(compute(1)).toBe(1000);
      expect(compute(5)).toBe(1000);
    });

    it('grows with attempts and stays within [base, max] for exponential', async () => {
      const base = 1000;
      const max = 30000;
      kafkaClient = new KafkaClient('backoff-exp', 'localhost:9092', {
        retryBackoffStrategy: 'exponential',
        messageRetryDelayMs: base,
        retryBackoffMaxMs: max,
      });
      await kafkaClient.initialize();

      const compute = (n: number) => (kafkaClient as any).computeBackoffDelay(n);

      const a1 = compute(1);
      const a3 = compute(3);

      // attempt 1 ~ base..base+jitter (jitter <= min(base,1000))
      expect(a1).toBeGreaterThanOrEqual(base);
      expect(a1).toBeLessThanOrEqual(base + Math.min(base, 1000));

      // later attempts are larger
      expect(a3).toBeGreaterThan(a1);

      // never exceeds the cap, even for very large attempts
      for (let attempt = 1; attempt <= 20; attempt++) {
        expect(compute(attempt)).toBeLessThanOrEqual(max);
      }
    });
  });

  describe('resolveEvent / dedup', () => {
    it('unwraps an envelope and uses eventId as idempotency id', async () => {
      kafkaClient = new KafkaClient('env-client', 'localhost:9092', {
        useEnvelope: true,
      });
      await kafkaClient.initialize();

      const envelope = createEnvelope({ amount: 42 }, { eventType: 'order.created', eventId: 'evt-123' });
      const message = buildMessage(envelope);

      const { event, idempotencyId } = (kafkaClient as any).resolveEvent(message, 'orders');
      expect(event).toEqual({ amount: 42 });
      expect(idempotencyId).toBe('evt-123');
    });

    it('falls back to x-message-id header when not using envelopes', async () => {
      kafkaClient = new KafkaClient('hdr-client', 'localhost:9092', {});
      await kafkaClient.initialize();

      const message = buildMessage({ foo: 'bar' }, { 'x-message-id': Buffer.from('msg-77') });
      const { event, idempotencyId } = (kafkaClient as any).resolveEvent(message, 'topic');
      expect(event).toEqual({ foo: 'bar' });
      expect(idempotencyId).toBe('msg-77');
    });

    it('returns null idempotency id when no source is available', async () => {
      kafkaClient = new KafkaClient('none-client', 'localhost:9092', {});
      await kafkaClient.initialize();

      const message = buildMessage({ foo: 'bar' });
      const { idempotencyId } = (kafkaClient as any).resolveEvent(message, 'topic');
      expect(idempotencyId).toBeNull();
    });

    it('prefers a configured idempotencyKeyExtractor', async () => {
      kafkaClient = new KafkaClient('ext-client', 'localhost:9092', {
        idempotencyKeyExtractor: (_msg, topic) => `${topic}-fixed`,
      });
      await kafkaClient.initialize();

      const message = buildMessage({ foo: 'bar' }, { 'x-message-id': Buffer.from('ignored') });
      const { idempotencyId } = (kafkaClient as any).resolveEvent(message, 'orders');
      expect(idempotencyId).toBe('orders-fixed');
    });

    it('throws for an invalid envelope when validation is enabled', async () => {
      kafkaClient = new KafkaClient('val-client', 'localhost:9092', {
        useEnvelope: true,
        validateEnvelopeOnConsume: true,
      });
      await kafkaClient.initialize();

      const message = buildMessage({ not: 'an envelope' });
      expect(() => (kafkaClient as any).resolveEvent(message, 'topic')).toThrow(/Invalid envelope/);
    });
  });

  describe('InMemoryIdempotencyStore end-to-end with resolveEvent', () => {
    it('marks the resolved id and reports it as processed', async () => {
      const store = new InMemoryIdempotencyStore({ cleanupIntervalMs: 0 });
      kafkaClient = new KafkaClient('store-client', 'localhost:9092', {
        useEnvelope: true,
        idempotencyStore: store,
      });
      await kafkaClient.initialize();

      const envelope = createEnvelope({ x: 1 }, { eventType: 'e', eventId: 'evt-store-1' });
      const message = buildMessage(envelope);
      const { idempotencyId } = (kafkaClient as any).resolveEvent(message, 'topic');

      expect(await store.isProcessed(idempotencyId!)).toBe(false);
      await store.markProcessed(idempotencyId!);
      expect(await store.isProcessed(idempotencyId!)).toBe(true);

      store.stopCleanup();
    });
  });
});
