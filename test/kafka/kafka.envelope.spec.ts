import {
  createEnvelope,
  isEnvelope,
  unwrapEnvelope,
  validateEnvelope,
  KafkaEnvelope,
} from '../../src/kafka/kafka.envelope';

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

describe('kafka.envelope', () => {
  describe('createEnvelope', () => {
    it('creates an envelope with sensible defaults', () => {
      const env = createEnvelope({ foo: 1 }, { eventType: 'order.created' });

      expect(typeof env.eventId).toBe('string');
      expect(env.eventId.length).toBeGreaterThan(0);
      expect(env.eventId).toMatch(UUID_REGEX);
      expect(Number.isNaN(Date.parse(env.timestamp))).toBe(false);
      expect(env.version).toBe('1');
      expect(env.eventType).toBe('order.created');
      expect(env.payload).toEqual({ foo: 1 });
    });

    it('respects provided values', () => {
      const env = createEnvelope(
        { foo: 1 },
        {
          eventType: 'order.created',
          eventId: '11111111-1111-1111-1111-111111111111',
          timestamp: '2020-01-01T00:00:00.000Z',
          version: '2',
          tenant: 'acme',
          traceId: 'trace-abc',
          source: 'orders-service',
        },
      );

      expect(env.eventId).toBe('11111111-1111-1111-1111-111111111111');
      expect(env.timestamp).toBe('2020-01-01T00:00:00.000Z');
      expect(env.version).toBe('2');
      expect(env.tenant).toBe('acme');
      expect(env.traceId).toBe('trace-abc');
      expect(env.source).toBe('orders-service');
      expect('tenant' in env).toBe(true);
      expect('traceId' in env).toBe(true);
      expect('source' in env).toBe(true);
    });

    it('does not include undefined optional keys', () => {
      const env = createEnvelope({ foo: 1 }, { eventType: 'order.created' });

      expect('tenant' in env).toBe(false);
      expect('traceId' in env).toBe(false);
      expect('source' in env).toBe(false);
    });
  });

  describe('isEnvelope', () => {
    it('returns true for a created envelope', () => {
      const env = createEnvelope({ foo: 1 }, { eventType: 'order.created' });
      expect(isEnvelope(env)).toBe(true);
    });

    it('returns false for non-envelope values', () => {
      expect(isEnvelope({ foo: 1 })).toBe(false);
      expect(isEnvelope(null)).toBe(false);
      expect(isEnvelope(undefined)).toBe(false);
      expect(isEnvelope(42)).toBe(false);
      expect(isEnvelope('str')).toBe(false);
      expect(isEnvelope([1, 2, 3])).toBe(false);

      const missingPayload = {
        eventId: '11111111-1111-1111-1111-111111111111',
        eventType: 'order.created',
        timestamp: '2020-01-01T00:00:00.000Z',
        version: '1',
      };
      expect(isEnvelope(missingPayload)).toBe(false);
    });
  });

  describe('unwrapEnvelope', () => {
    it('unwraps an envelope into envelope and inner payload', () => {
      const env = createEnvelope({ foo: 1 }, { eventType: 'order.created' });
      const result = unwrapEnvelope<{ foo: number }>(env);

      expect(result.envelope).not.toBeNull();
      expect(result.payload).toEqual({ foo: 1 });
    });

    it('returns raw value as payload when not an envelope', () => {
      const result = unwrapEnvelope({ foo: 1 });
      expect(result).toEqual({ envelope: null, payload: { foo: 1 } });
    });
  });

  describe('validateEnvelope', () => {
    it('is valid for a created envelope', () => {
      const env = createEnvelope({ foo: 1 }, { eventType: 'order.created' });
      const result = validateEnvelope(env);

      expect(result.valid).toBe(true);
      expect(result.errors).toEqual([]);
    });

    it('is invalid with non-empty errors for malformed values', () => {
      const empty = validateEnvelope({});
      expect(empty.valid).toBe(false);
      expect(empty.errors.length).toBeGreaterThan(0);
      empty.errors.forEach((e) => expect(typeof e).toBe('string'));

      const noPayload = validateEnvelope({
        eventId: '11111111-1111-1111-1111-111111111111',
        eventType: 'order.created',
        timestamp: '2020-01-01T00:00:00.000Z',
        version: '1',
      });
      expect(noPayload.valid).toBe(false);
      expect(noPayload.errors.length).toBeGreaterThan(0);
      noPayload.errors.forEach((e) => expect(typeof e).toBe('string'));

      const numericEventId = validateEnvelope({
        eventId: 123,
        eventType: 'order.created',
        timestamp: '2020-01-01T00:00:00.000Z',
        version: '1',
        payload: { foo: 1 },
      } as unknown as KafkaEnvelope);
      expect(numericEventId.valid).toBe(false);
      expect(numericEventId.errors.length).toBeGreaterThan(0);
      numericEventId.errors.forEach((e) => expect(typeof e).toBe('string'));
    });
  });
});
