import { randomUUID } from 'crypto';

/**
 * Generic, optional message contract used to wrap Kafka payloads with
 * transport-agnostic metadata (event identity, type, tenant, tracing, etc.).
 */
export interface KafkaEnvelope<T = unknown> {
  eventId: string; // UUID
  eventType: string; // e.g. 'order.created'
  tenant?: string;
  timestamp: string; // ISO 8601
  version: string; // schema version, default '1'
  traceId?: string;
  source?: string;
  payload: T;
}

/**
 * Options accepted by {@link createEnvelope}. Only `eventType` is required;
 * the rest fall back to sensible defaults.
 */
export interface CreateEnvelopeOptions {
  eventType: string;
  tenant?: string;
  version?: string; // default '1'
  traceId?: string;
  source?: string;
  eventId?: string; // default randomUUID()
  timestamp?: string; // default new Date().toISOString()
}

/**
 * Wraps a payload in a {@link KafkaEnvelope} with metadata. Generates an
 * `eventId` and `timestamp` when not provided, and omits optional keys that
 * are left undefined.
 */
export function createEnvelope<T>(payload: T, options: CreateEnvelopeOptions): KafkaEnvelope<T> {
  const envelope: KafkaEnvelope<T> = {
    eventId: options.eventId ?? randomUUID(),
    eventType: options.eventType,
    timestamp: options.timestamp ?? new Date().toISOString(),
    version: options.version ?? '1',
    payload,
  };

  if (options.tenant !== undefined) {
    envelope.tenant = options.tenant;
  }
  if (options.traceId !== undefined) {
    envelope.traceId = options.traceId;
  }
  if (options.source !== undefined) {
    envelope.source = options.source;
  }

  return envelope;
}

/**
 * Type guard returning `true` when `value` has the shape of a
 * {@link KafkaEnvelope}.
 */
export function isEnvelope(value: unknown): value is KafkaEnvelope {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }

  const candidate = value as Record<string, unknown>;

  return (
    typeof candidate.eventId === 'string' &&
    candidate.eventId.length > 0 &&
    typeof candidate.eventType === 'string' &&
    candidate.eventType.length > 0 &&
    typeof candidate.timestamp === 'string' &&
    typeof candidate.version === 'string' &&
    'payload' in candidate
  );
}

/**
 * Tolerantly unwraps a value: when it is a {@link KafkaEnvelope} the envelope
 * and its inner payload are returned; otherwise the raw value is returned as
 * the payload with a `null` envelope.
 */
export function unwrapEnvelope<T = unknown>(value: unknown): { envelope: KafkaEnvelope<T> | null; payload: T } {
  if (isEnvelope(value)) {
    return { envelope: value as KafkaEnvelope<T>, payload: (value as KafkaEnvelope<T>).payload };
  }

  return { envelope: null, payload: value as T };
}

/**
 * Validates the required fields of a {@link KafkaEnvelope}, collecting a
 * human-readable message for every problem found.
 */
export function validateEnvelope(value: unknown): { valid: boolean; errors: string[] } {
  const errors: string[] = [];

  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    errors.push('envelope must be an object');
    return { valid: false, errors };
  }

  const candidate = value as Record<string, unknown>;

  if (typeof candidate.eventId !== 'string') {
    errors.push('eventId is required and must be a string');
  }
  if (typeof candidate.eventType !== 'string') {
    errors.push('eventType is required and must be a string');
  }
  if (typeof candidate.timestamp !== 'string') {
    errors.push('timestamp is required and must be a string');
  }
  if (typeof candidate.version !== 'string') {
    errors.push('version is required and must be a string');
  }
  if (!('payload' in candidate)) {
    errors.push('payload is required');
  }

  return { valid: errors.length === 0, errors };
}
